//
// Copyright (C) 2020 Yahoo Japan Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#pragma once

#include "NGT/Index.h"
#include "NGT/NGTQ/Quantizer.h"
#define NGTQG_RESIZED_NODE
#define NGTQ_QUANTIZED_TREE

#define NGT_GRAPH_CHECK_VECTOR
#undef NGT_GRAPH_CHECK_BOOLEANSET
#undef NGT_GRAPH_CHECK_HASH_BASED_BOOLEAN_SET
#undef NGT_GRAPH_CHECK_TRICKYBOOLEANSET
typedef NGT::BooleanVectorByEpoch<uint8_t> BooleanVectorByEpoch;

#ifdef NGTQG_X86SIMDSORT
#include "x86simdsort.h"
#endif

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)

inline static void prefetchShort(unsigned char *ptr, const size_t byteSizeOfObject) {
#ifndef NGT_NO_AVX
  switch ((byteSizeOfObject - 1) >> 6) {
  default:
  case 16: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 15: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 14: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 13: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 12: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 11: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 10: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 9: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 8: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 7: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 6: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 5: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 4: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 3: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 2: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 1: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
  case 0:
    _mm_prefetch(ptr, _MM_HINT_T0);
    ptr += 64;
    break;
  }
#endif // NGT_NO_AVX
}

namespace NGT {
template <typename T = detail::CandidateObject> using DistanceSorter = HeapCandidateObjects<T>;
}

#ifdef NGTQ_QBG

#define GLOBAL_SIZE 1

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
#undef NGTQG_BLOB_GRAPH
#endif

namespace QBG {
class BuildParameters;
};

namespace NGTQG {
class SearchContainer : public NGT::SearchContainer {
 public:
  SearchContainer(SearchContainer &sc, NGT::Object &f)
      : NGT::SearchContainer(sc, f), resultExpansion(sc.resultExpansion) {}
  SearchContainer() : resultExpansion(0.0) {}

  void setResultExpansion(float re) { resultExpansion = re; }
  float resultExpansion;
};

class SearchQuery : public NGTQG::SearchContainer, public NGT::QueryContainer {
 public:
  template <typename QTYPE> SearchQuery(const std::vector<QTYPE> &q) : NGT::QueryContainer(q) {}
};

class QuantizedNode {
 public:
  ~QuantizedNode() { clear(); }
  void clear() {
    ids = 0;
    NGT::alignedFree64(objects);
    objects = 0;
  }
  uint32_t subspaceID;
  uint32_t nOfObjects;
  uint32_t *ids;
  void *objects;
#ifdef NGTQ_OBGRAPH
  std::vector<std::vector<uint32_t>> blobIDs;
#endif
};

typedef QuantizedNode RearrangedQuantizedObjectSet;

class QuantizedGraphRepository : public std::vector<QuantizedNode> {
  typedef std::vector<QuantizedNode> PARENT;

 public:
  QuantizedGraphRepository(NGTQ::Index &quantizedIndex)
      : quantizer(quantizedIndex.getQuantizer()),
        numOfSubspaces(quantizedIndex.getQuantizer().property.localDivisionNo),
        graphType(quantizedIndex.getQuantizer().property.graphType) {}
  ~QuantizedGraphRepository() {}

  void *get(size_t id) { return PARENT::operator[](id).objects; }

  uint32_t *getIDs(size_t id) { return PARENT::operator[](id).ids; }
  QuantizedNode &getNode(size_t id) { return PARENT::operator[](id); }
  QuantizedNode *getNodes() { return PARENT::data(); }
  void construct(NGT::Index &ngtindex, NGTQ::Index &quantizedIndex, size_t maxNoOfEdges) {
    NGT::GraphAndTreeIndex &index         = static_cast<NGT::GraphAndTreeIndex &>(ngtindex.getIndex());
    NGT::NeighborhoodGraph &graph         = static_cast<NGT::NeighborhoodGraph &>(index);
    NGT::GraphRepository &graphRepository = graph.repository;
    construct(graphRepository, quantizedIndex, maxNoOfEdges);
  }

  void construct(NGT::GraphRepository &graphRepository, NGTQ::Index &quantizedIndex, size_t maxNoOfEdges);

  void serialize(std::ofstream &os, NGT::ObjectSpace *objspace = 0);

  void deserialize(std::ifstream &is, NGT::ObjectSpace *objectspace = 0);

  bool stat(const string &path) {
    struct stat st;
    const std::string p(path + "/grp");
    return ::stat(p.c_str(), &st) == 0;
  }

  void save(const string &path) {
    if (PARENT::size() == 0) {
      return;
    }
    const std::string p(path + "/grp");
    std::ofstream os(p);
    serialize(os);
  }

  void load(const string &path) {
    const std::string p(path + "/grp");
    std::ifstream is(p);
    deserialize(is);
  }

  void setGraphType(NGTQ::GraphType gt) { graphType = gt; }

  NGTQ::Quantizer &quantizer;
  size_t numOfSubspaces;
  NGTQ::GraphType graphType;
};

class Index : public NGT::Index {
 public:
  Index(const std::string &indexPath, size_t maxNoOfEdges = 128, bool rdOnly = false)
      : NGT::Index(indexPath, rdOnly, NGT::Index::OpenTypeNone), readOnly(rdOnly), path(indexPath),
        quantizedIndex(indexPath + "/" + getQGDirectoryName(), rdOnly), quantizedGraph(quantizedIndex) {
    {
#ifdef NGTQ_OBGRAPH
      quantizedGraph.setGraphType(NGTQ::GraphTypeGraph);
#endif
      struct stat st;
      std::string qgpath(path + "/qg/grp");
      if (stat(qgpath.c_str(), &st) == 0) {
        quantizedGraph.load(path + "/" + getQGDirectoryName());
      } else {
        if (readOnly) {
          std::cerr << "No quantized graph. Construct it temporarily." << std::endl;
        }
        quantizedGraph.construct(*this, quantizedIndex, maxNoOfEdges);
      }
    }
  }

  void save() { quantizedGraph.save(path + "/" + getQGDirectoryName()); }

  void searchQuantizedGraph(NGT::NeighborhoodGraph &graph, NGTQG::SearchContainer &sc,
                            NGT::ObjectDistances &seeds) {
    auto specifiedRadius = sc.radius;
    size_t sizeBackup    = sc.size;
    if (sc.resultExpansion > 1.0) {
      sc.size *= sc.resultExpansion;
    }

    NGTQ::Quantizer &quantizer                             = quantizedIndex.getQuantizer();
    NGTQ::QuantizedObjectDistance &quantizedObjectDistance = quantizer.getQuantizedObjectDistance();

#ifdef NGTQ_QBG
    NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 cache[GLOBAL_SIZE];
#else
    NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 cache[GLOBAL_SIZE + 1];
#endif
    auto rotatedQuery = graph.getObjectSpace().getObject(sc.object);
    if (quantizer.property.dimension > rotatedQuery.size()) {
      rotatedQuery.resize(quantizer.property.dimension);
    }
#ifndef NGTQG_NO_ROTATION
    if (quantizedObjectDistance.rotation != nullptr) {
      quantizedObjectDistance.rotation->mul(rotatedQuery.data());
    }
#endif
#ifdef NGTQ_QBG
    for (int i = 0; i < GLOBAL_SIZE; i++) {
#else
    for (int i = 1; i < GLOBAL_SIZE + 1; i++) {
#endif
      quantizedObjectDistance.initialize(cache[i]);
      quantizedObjectDistance.createDistanceLookup(rotatedQuery.data(), i, cache[i]);
    }
    if (sc.explorationCoefficient == 0.0) {
      sc.explorationCoefficient = NGT_EXPLORATION_COEFFICIENT;
    }
    NGT::NeighborhoodGraph::UncheckedSet unchecked;
    NGT::NeighborhoodGraph::DistanceCheckedSet distanceChecked(
        NGT::Index::getObjectSpace().getRepository().size());
    NGT::NeighborhoodGraph::ResultSet results;

    graph.setupDistances(sc, seeds, NGT::PrimitiveComparator::L2Float::compare);
    graph.setupSeeds(sc, seeds, results, unchecked, distanceChecked);
    NGT::Distance explorationRadius = sc.explorationCoefficient * sc.radius;
    NGT::ObjectDistance result;
    NGT::ObjectDistance target;

    while (!unchecked.empty()) {
      target = unchecked.top();
      unchecked.pop();
      if (target.distance > explorationRadius) {
        break;
      }
      auto *neighborIDs   = quantizedGraph.getIDs(target.id);
      size_t neighborSize = quantizedGraph.getNode(target.id).nOfObjects;
      float ds[neighborSize + NGTQ_SIMD_BLOCK_SIZE];

#ifdef NGTQG_PREFETCH
      {
        uint8_t *lid = static_cast<uint8_t *>(quantizedGraph.get(target.id));
        size_t size  = ((neighborSize - 1) / (NGTQ_SIMD_BLOCK_SIZE * NGTQ_BATCH_SIZE) + 1) *
                      (NGTQ_SIMD_BLOCK_SIZE * NGTQ_BATCH_SIZE);
        size /= 2;
        size *= quantizedIndex.getQuantizer().divisionNo;

        NGT::MemoryCache::prefetch(lid, size);
      }
#endif
#ifdef NGTQ_QBG
      quantizedObjectDistance(quantizedGraph.get(target.id), ds, neighborSize, cache[0]);
#else
      quantizedObjectDistance(quantizedGraph.get(target.id), ds, neighborSize, cache[1]);
#endif
      for (size_t idx = 0; idx < neighborSize; idx++) {
        NGT::Distance distance = ds[idx];
        auto objid             = neighborIDs[idx];
        if (distance <= explorationRadius) {
          bool checked = distanceChecked[objid];
          if (checked) {
            continue;
          }
#ifdef NGT_VISIT_COUNT
          sc.visitCount++;
#endif
          distanceChecked.insert(objid);
          result.set(objid, distance);
          unchecked.push(result);
          if (distance <= sc.radius) {
            results.push(result);
            if (results.size() >= sc.size) {
              if (results.size() > sc.size) {
                results.pop();
              }
              sc.radius         = results.top().distance;
              explorationRadius = sc.explorationCoefficient * sc.radius;
            }
          }
        }
      }
    }

    if (sc.resultIsAvailable()) {
      NGT::ObjectDistances &qresults = sc.getResult();
      qresults.moveFrom(results);
      if (sc.resultExpansion >= 1.0) {
        {
          NGT::ObjectRepository &objectRepository  = NGT::Index::getObjectSpace().getRepository();
          NGT::ObjectSpace::Comparator &comparator = NGT::Index::getObjectSpace().getComparator();
          for (auto i = qresults.begin(); i != qresults.end(); ++i) {
#ifdef NGTQG_PREFETCH
            if (static_cast<size_t>(distance(qresults.begin(), i + 10)) < qresults.size()) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
              NGT::PersistentObject &o = *objectRepository.get((*(i + 10)).id);
#else
              NGT::Object &o = *objectRepository[(*(i + 10)).id];
#endif
              _mm_prefetch(&o[0], _MM_HINT_T0);
            }
#endif
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            NGT::PersistentObject &obj = *objectRepository.get((*i).id);
#else
            NGT::Object &obj = *objectRepository[(*i).id];
#endif
            (*i).distance = comparator(sc.object, obj);
          }
          std::sort(qresults.begin(), qresults.end());
          if (specifiedRadius < std::numeric_limits<float>::max()) {
            auto pos =
                std::upper_bound(qresults.begin(), qresults.end(), NGT::ObjectDistance(0, specifiedRadius));
            qresults.resize(distance(qresults.begin(), pos));
          }
        }
        sc.size = sizeBackup;
        if (sc.size < qresults.size()) {
          qresults.resize(sc.size);
        }
      }
    } else {
      if (sc.resultExpansion >= 1.0) {
        {
          NGT::ObjectRepository &objectRepository  = NGT::Index::getObjectSpace().getRepository();
          NGT::ObjectSpace::Comparator &comparator = NGT::Index::getObjectSpace().getComparator();
          while (!sc.workingResult.empty()) {
            sc.workingResult.pop();
          }
          while (!results.empty()) {
            NGT::ObjectDistance obj = results.top();
            obj.distance            = comparator(sc.object, *objectRepository.get(obj.id));
            results.pop();
            sc.workingResult.push(obj);
          }
          sc.size = sizeBackup;
          while (sc.workingResult.size() > sc.size) {
            sc.workingResult.pop();
          }
          if (specifiedRadius < std::numeric_limits<float>::max()) {
            while (sc.workingResult.top().distance > specifiedRadius && !sc.workingResult.empty()) {
              sc.workingResult.pop();
            }
          }
        }
      } else {
        sc.workingResult = std::move(results);
      }
    }
  }

#if !defined(NGTQG_CLASSIC_SEARCH)
  void searchQuantizedGraphUsingDistanceSorter(NGT::NeighborhoodGraph &graph, NGTQG::SearchContainer &sc,
                                               NGT::Node::NodeID seedID) {
    auto specifiedRadius                                   = sc.radius;
    NGTQ::Quantizer &quantizer                             = quantizedIndex.getQuantizer();
    NGTQ::QuantizedObjectDistance &quantizedObjectDistance = quantizer.getQuantizedObjectDistance();
#ifdef NGTQG_PREFETCH
    const size_t prefetchSize = graph.objectSpace->getPrefetchSize();
#endif

    auto rotatedQuery = graph.getObjectSpace().getObject(sc.object);
    if (quantizer.property.dimension > rotatedQuery.size()) {
      rotatedQuery.resize(quantizer.property.dimension);
    }
#ifndef NGTQG_NO_ROTATION
    if (quantizedObjectDistance.rotation != nullptr) {
      quantizedObjectDistance.rotation->mul(rotatedQuery.data());
    }
#endif
#ifdef NGTQ_QBG
    NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 cache[GLOBAL_SIZE];
#else
    NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 cache[GLOBAL_SIZE + 1];
#endif

#ifdef NGTQ_QBG
    for (int i = 0; i < GLOBAL_SIZE; i++) {
#else
    for (int i = 1; i < GLOBAL_SIZE + 1; i++) {
#endif
      quantizedObjectDistance.initialize(cache[i]);
      quantizedObjectDistance.createDistanceLookup(rotatedQuery.data(), i, cache[i]);
    }
    if (sc.explorationCoefficient == 0.0) {
      sc.explorationCoefficient = NGT_EXPLORATION_COEFFICIENT;
    }
    size_t threadID = omp_get_thread_num();
    if (threadID >= candidateNodePools.size()) {
      candidateNodePools.resize(threadID + 1);
    }
    NGT::DistanceSorter<> *__restrict__ uncheckedPtr = &candidateNodePools[threadID];

    uncheckedPtr->setResultExpansion(sc.resultExpansion);
    uncheckedPtr->reset(sc.size, sc.resultExpansion);
    if (threadID >= visitPools.size()) {
      visitPools.resize(threadID + 1);
    }
    auto &distanceChecked = visitPools[threadID];
    distanceChecked.reset();
    QuantizedNode *quantizedNodes = quantizedGraph.getNodes();
    alignas(16) uint16_t ds[128 + NGTQ_SIMD_BLOCK_SIZE];
    constexpr uint16_t explorationShift   = 10;
    constexpr uint16_t explorationScale   = 1 << explorationShift;
    const uint32_t explorationCoefficient = sc.explorationCoefficient * explorationScale;
    auto targetID                         = seedID;
    uint32_t radius                       = 0x8FFFFFFF;
    uint32_t explorationRadius            = 0x8FFFFFFF;
    NGT::DistanceSorter<>::Object result;
    NGT::DistanceSorter<>::Object target;
    target.id = targetID;

    while (true) {

      auto *neighborIDs      = quantizedNodes[target.id].ids;
      size_t neighborIDsSize = quantizedNodes[target.id].nOfObjects;
#ifdef NGT_REVISED_QUANTIZED_DISTANCE
      size_t neighborSize = neighborIDsSize > 32 ? neighborIDsSize & ~0x1Fu : neighborIDsSize;
#else
      size_t neighborSize = neighborIDs.size();
#endif
      auto *qobjs = quantizedNodes[target.id].objects;
#ifdef NGTQG_PREFETCH
      {
        size_t size = (quantizedIndex.getQuantizer().divisionNo * neighborSize) / 2;
        prefetchShort(static_cast<uint8_t *>(qobjs), size);
      }
#endif
#ifdef NGTQ_QBG
      quantizedObjectDistance(qobjs, ds, neighborSize, cache[0]);
#else
      quantizedObjectDistance(qobjs, ds, neighborSize, cache[1]);
#endif
      auto *__restrict__ dsPtr  = ds;
      auto *__restrict__ idsPtr = neighborIDs;
      for (size_t idx = 0; idx < neighborSize; idx++) {
        uint32_t distance = dsPtr[idx];
        auto objid        = idsPtr[idx];
        if (distance <= explorationRadius) {
          if (LIKELY(distanceChecked.visit(objid))) {
            continue;
          }
#ifdef NGT_VISIT_COUNT
          sc.visitCount++;
#endif
          result.id       = objid;
          result.distance = distance;
          int pushResult  = uncheckedPtr->push(result);
          if (pushResult != 0) {
            uint32_t rad      = uncheckedPtr->getMaxDistance();
            radius            = rad;
            explorationRadius = (radius * explorationCoefficient) >> explorationShift;
          }
        }
      }
      NGT::DistanceSorter<>::Object nobj;
      bool stat = !uncheckedPtr->pop(nobj);
      if (stat) break;
      target.id       = nobj.id;
      target.distance = uncheckedPtr->getCurrentDistance();
      if (target.distance > explorationRadius) {
        break;
      }
    }
    size_t expandedSize = sc.resultExpansion > 1.0 ? sc.size * sc.resultExpansion : sc.size;
    NGT::DistanceSorter<>::Object final_results[expandedSize];
    size_t resultCount = uncheckedPtr->getObjects(final_results);
    if (sc.resultIsAvailable()) {
      NGT::ObjectDistances &qresults = sc.getResult();
      if (sc.resultExpansion < 1.0) {
        qresults.resize(resultCount);
        for (size_t i = 0; i < resultCount; i++) {
          qresults[i].id       = final_results[i].id;
          qresults[i].distance = 0.0;
        }
      } else {
        {
          NGT::ObjectRepository &objectRepository  = NGT::Index::getObjectSpace().getRepository();
          NGT::ObjectSpace::Comparator &comparator = NGT::Index::getObjectSpace().getComparator();
          qresults.resize(resultCount);
#if defined(NGTQG_X86SIMDSORT)
          float distances[resultCount];
          uint32_t ids[resultCount];
#endif
          for (size_t i = 0; i < resultCount; i++) {
#ifdef NGTQG_PREFETCH
            size_t offset = 2;
            if (static_cast<size_t>(i + offset) < resultCount) {
              size_t id = final_results[i + offset].id;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
              NGT::PersistentObject &o = *objectRepository.get(id);
#else
              NGT::Object &o = *objectRepository[id];
#endif
              _mm_prefetch(&o[0] + prefetchSize, _MM_HINT_T0);
            }
#endif
            auto id        = final_results[i].id;
            qresults[i].id = id;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            NGT::PersistentObject &obj = *objectRepository.get(id);
#else
            NGT::Object &obj = *objectRepository[id];
#endif
#if defined(NGTQG_X86SIMDSORT)
            distances[i] = comparator(sc.object, obj);
            ids[i]       = qresults[i].id;
#else
            qresults[i].distance = comparator(sc.object, obj);
#endif
          }
#ifdef NGTQG_X86SIMDSORT
          {
            size_t k = sc.size;
            if (quantizedIndex.getQuantizer().property.orderedSearchRanking == true) {
              x86simdsort::keyvalue_partial_sort(distances, ids, k, resultCount, false, false);
            } else {
              x86simdsort::keyvalue_select(distances, ids, k, resultCount, false, false);
            }
            for (size_t i = 0; i < k; i++) {
              qresults[i].id       = ids[i];
              qresults[i].distance = distances[i];
            }
          }
#else
          if (quantizedIndex.getQuantizer().property.orderedSearchRanking == true) {
            std::partial_sort(qresults.begin(), qresults.begin() + sc.size, qresults.end());
          } else {
            std::nth_element(qresults.begin(), qresults.begin() + sc.size, qresults.end());
          }
#endif

          if (specifiedRadius < std::numeric_limits<float>::max()) {
            auto pos =
                std::upper_bound(qresults.begin(), qresults.end(), NGT::ObjectDistance(0, specifiedRadius));
            qresults.resize(distance(qresults.begin(), pos));
          }
        }
        if (sc.size < qresults.size()) {
          qresults.resize(sc.size);
        }
      }
    } else {
      while (!sc.workingResult.empty()) {
        sc.workingResult.pop();
      }
      if (sc.resultExpansion >= 1.0) {
        {
          NGT::ObjectRepository &objectRepository  = NGT::Index::getObjectSpace().getRepository();
          NGT::ObjectSpace::Comparator &comparator = NGT::Index::getObjectSpace().getComparator();
          for (size_t i = 0; i < resultCount; i++) {
            auto id = final_results[i].id;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            NGT::PersistentObject &obj = *objectRepository.get(id);
#else
            NGT::Object &obj = *objectRepository[id];
#endif
            NGT::ObjectDistance o(id, comparator(sc.object, obj));
            sc.workingResult.push(o);
          }
          while (sc.workingResult.size() > sc.size) {
            sc.workingResult.pop();
          }
          if (specifiedRadius < std::numeric_limits<float>::max()) {
            while (sc.workingResult.top().distance > specifiedRadius && !sc.workingResult.empty()) {
              sc.workingResult.pop();
            }
          }
        }
      } else {
        for (size_t i = 0; i < resultCount; i++) {
          auto id = final_results[i].id;
          NGT::ObjectDistance o(id, 0.0);
          sc.workingResult.push(o);
        }
      }
    }
  }
#endif

  void searchClassic(NGT::GraphIndex &index, NGTQG::SearchContainer &sc, NGT::ObjectDistances &seeds) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    NGTThrowException("NGTQG is not available for SHARED.");
#endif
    if (sc.size == 0) {
      while (!sc.workingResult.empty())
        sc.workingResult.pop();
      return;
    }
    if (seeds.size() == 0) {
      index.getSeedsFromGraph(index.getObjectSpace().getRepository(), seeds);
    }
    if (sc.expectedAccuracy > 0.0) {
      sc.setEpsilon(getEpsilonFromExpectedAccuracy(sc.expectedAccuracy));
    }

    try {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR) || !defined(NGT_GRAPH_READ_ONLY_GRAPH)
      index.NeighborhoodGraph::search(sc, seeds);
#else
      searchQuantizedGraph(static_cast<NGT::NeighborhoodGraph &>(index), sc, seeds);
#endif
    } catch (NGT::Exception &err) {
      std::cerr << err.what() << std::endl;
      NGT::Exception e(err);
      throw e;
    }
  }

  void searchClassic(NGTQG::SearchQuery &sq) {
    NGT::GraphAndTreeIndex &index = static_cast<NGT::GraphAndTreeIndex &>(getIndex());
    NGT::Object *query            = Index::allocateQuery(sq);
    try {
      NGTQG::SearchContainer sc(sq, *query);
      sc.distanceComputationCount = 0;
      sc.visitCount               = 0;
      NGT::ObjectDistances seeds;
      try {
        index.getSeedsFromTree(sc, seeds);
      } catch (...) {
      }
      NGTQG::Index::searchClassic(static_cast<NGT::GraphIndex &>(index), sc, seeds);
      sq.workingResult            = std::move(sc.workingResult);
      sq.distanceComputationCount = sc.distanceComputationCount;
      sq.visitCount               = sc.visitCount;
    } catch (NGT::Exception &err) {
      deleteObject(query);
      throw err;
    }
    deleteObject(query);
  }

#ifdef NGTQG_CLASSIC_SEARCH
  void searchUsingDistanceSorter(NGTQG::SearchQuery &sq) {
    NGTThrowException("searchUsingDistanceBucket is unavailable.");
  }
#else
  void searchUsingDistanceSorter(NGTQG::SearchQuery &sq) {
#ifdef NGTQG_TIMER
    NGT::Timer timer_whole_func;
    timer_whole_func.start();
#endif
    NGT::GraphAndTreeIndex &index = static_cast<NGT::GraphAndTreeIndex &>(getIndex());
    NGT::Object *query            = Index::allocateQuery(sq);
    NGT::Node::NodeID seedLeafNodeID;
    try {
      NGTQG::SearchContainer sc(sq, *query);
      sc.distanceComputationCount = 0;
      sc.visitCount               = 0;
      NGT::ObjectDistances seeds;
      try {
        NGT::DVPTree::SearchContainer tso(sc.object);
        tso.mode                     = NGT::DVPTree::SearchContainer::SearchLeaf;
        tso.radius                   = 0.0;
        tso.size                     = 1;
        tso.distanceComputationCount = 0;
        tso.visitCount               = 0;
        try {
          index.NGT::DVPTree::search(tso);
        } catch (NGT::Exception &err) {
          std::stringstream msg;
          msg << "Cannot search for tree.:" << err.what();
          NGTThrowException(msg);
        }
        seedLeafNodeID = tso.nodeID.getID();
      } catch (NGT::Exception &err) {
        std::stringstream msg;
        msg << "Cannot get seeds from the tree.:" << err.what();
        NGTThrowException(msg);
      }
      size_t nOfObjects = NGT::Index::getObjectSpace().getRepository().size() - 1;
      auto seedNodeID   = seedLeafNodeID + nOfObjects;
      NGTQG::Index::searchQuantizedGraphUsingDistanceSorter(static_cast<NGT::GraphIndex &>(index), sc,
                                                            seedNodeID);
      sq.distanceComputationCount = sc.distanceComputationCount;
      sq.visitCount               = sc.visitCount;
    } catch (NGT::Exception &err) {
      deleteObject(query);
      throw err;
    }
    deleteObject(query);
  }
#endif

  void search(NGTQG::SearchQuery &sq) {
#ifdef NGTQG_CLASSIC_SEARCH
    searchClassic(sq);
#else
    if (quantizedIndex.getQuantizer().property.treeQuantization == true) {
      searchUsingDistanceSorter(sq);
    } else {
      searchClassic(sq);
    }
#endif
  }

  static size_t getNumberOfSubvectors(size_t dimension, size_t dimensionOfSubvector) {
    if (dimensionOfSubvector == 0) {
      dimensionOfSubvector = dimension > 400 ? 2 : 1;
      dimensionOfSubvector = (dimension % dimensionOfSubvector == 0) ? dimensionOfSubvector : 1;
    }
    if (dimension % dimensionOfSubvector != 0) {
      std::stringstream msg;
      msg << "Quantizer::getNumOfSubvectors: dimensionOfSubvector is invalid. " << dimension << " : "
          << dimensionOfSubvector;
      NGTThrowException(msg);
    }
    return dimension / dimensionOfSubvector;
  }

  static void buildQuantizedGraph(const std::string indexPath, size_t maxNumOfEdges = 128) {
    const std::string qgPath(indexPath + "/" + getQGDirectoryName());
    NGTQ::Index quantizedIndex(qgPath, false);
    NGTQG::QuantizedGraphRepository quantizedGraph(quantizedIndex);
    {
      struct stat st;
      std::string qgGraphPath(qgPath + "/grp");
      if (stat(qgGraphPath.c_str(), &st) == 0) {
        stringstream msg;
        msg << "Already exists. " << qgGraphPath;
        NGTThrowException(msg);
      } else {
        NGT::GraphRepository graph;
        NGT::GraphIndex::loadGraph(indexPath, graph);
        quantizedGraph.construct(graph, quantizedIndex, maxNumOfEdges);
        quantizedGraph.save(qgPath);
      }
    }

    std::cerr << "Quantized graph is completed." << std::endl;
    std::cerr << "  vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
    std::cerr << "  peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
  }

#ifndef NGTQ_QBG
  static void buildQuantizedObjects(const std::string quantizedIndexPath, NGT::ObjectSpace &objectSpace,
                                    bool insertion = false) {
    NGTQ::Index quantizedIndex(quantizedIndexPath);
    NGTQ::Quantizer &quantizer = quantizedIndex.getQuantizer();

    {
      std::vector<float> meanObject(objectSpace.getDimension(), 0);
      quantizedIndex.getQuantizer().globalCodebookIndex.insert(meanObject);
      quantizedIndex.getQuantizer().globalCodebookIndex.createIndex(8);
    }

    vector<pair<NGT::Object *, size_t>> objects;
    for (size_t id = 1; id < objectSpace.getRepository().size(); id++) {
      if (id % 100000 == 0) {
        std::cerr << "Processed " << id << " objects." << std::endl;
      }
      std::vector<float> object;
      try {
        objectSpace.getObject(id, object);
      } catch (...) {
        std::cerr << "NGTQG::buildQuantizedObjects: Warning! Cannot get the object. " << id << std::endl;
        continue;
      }
      quantizer.insert(object, objects, id);
    }
    if (objects.size() > 0) {
      quantizer.insert(objects);
    }

    quantizedIndex.save();
    quantizedIndex.close();
  }
#endif

#ifdef NGTQ_QBG
  static void createQuantizedGraphFrame(const std::string quantizedIndexPath, size_t dimension,
                                        size_t pseudoDimension, size_t dimensionOfSubvector) {
#else
  static void createQuantizedGraphFrame(const std::string quantizedIndexPath, size_t dimension,
                                        size_t dimensionOfSubvector) {
#endif
    NGTQ::Property property;
    NGT::Property globalProperty;
    NGT::Property localProperty;

    property.threadSize           = 24;
    property.globalRange          = 0;
    property.localRange           = 0;
    property.dataType             = NGTQ::DataTypeFloat;
    property.distanceType         = NGTQ::DistanceType::DistanceTypeL2;
    property.singleLocalCodebook  = false;
    property.batchSize            = 1000;
    property.centroidCreationMode = NGTQ::CentroidCreationModeStatic;
#ifdef NGTQ_QBG
    property.localCentroidCreationMode = NGTQ::CentroidCreationModeStatic;
#else
    property.localCentroidCreationMode = NGTQ::CentroidCreationModeDynamicKmeans;
#endif
    property.globalCentroidLimit              = 1;
    property.localCentroidLimit               = 16;
    property.localClusteringSampleCoefficient = 100;
#ifdef NGTQ_QBG
    property.genuineDimension = dimension;
    if (pseudoDimension == 0) {
      property.dimension = dimension;
    } else {
      property.dimension = pseudoDimension;
    }
#else
    property.dimension = dimension;
#endif
    property.localDivisionNo = NGTQG::Index::getNumberOfSubvectors(property.dimension, dimensionOfSubvector);
    globalProperty.edgeSizeForCreation        = 10;
    globalProperty.edgeSizeForSearch          = 40;
    globalProperty.indexType                  = NGT::Property::GraphAndTree;
    globalProperty.insertionRadiusCoefficient = 1.1;

    localProperty.indexType                  = NGT::Property::GraphAndTree;
    localProperty.insertionRadiusCoefficient = 1.1;

    NGTQ::Index::create(quantizedIndexPath, property, globalProperty, localProperty);
  }
#ifdef NGTQ_QBG
  static void create(const std::string indexPath, QBG::BuildParameters &buildParameters);
#endif

  static void create(const std::string indexPath, size_t dimensionOfSubvector, size_t pseudoDimension) {
    NGT::Index index(indexPath);
    std::string quantizedIndexPath = indexPath + "/" + getQGDirectoryName();
    struct stat st;
    if (stat(quantizedIndexPath.c_str(), &st) == 0) {
      std::stringstream msg;
      msg << "QuantizedGraph::create: Quantized graph is already existed. " << indexPath;
      NGTThrowException(msg);
    }
    NGT::Property ngtProperty;
    index.getProperty(ngtProperty);
#ifdef NGTQ_QBG
    int align = 16;
    if (pseudoDimension == 0) {
      pseudoDimension = ((ngtProperty.dimension - 1) / align + 1) * align;
    }
    if (ngtProperty.dimension > static_cast<int>(pseudoDimension)) {
      std::stringstream msg;
      msg << "QuantizedGraph::quantize: the specified pseudo dimension is smaller than the genuine "
             "dimension. "
          << ngtProperty.dimension << ":" << pseudoDimension << std::endl;
      NGTThrowException(msg);
    }
    if (pseudoDimension % align != 0) {
      std::stringstream msg;
      msg << "QuantizedGraph::quantize: the specified pseudo dimension should be a multiple of " << align
          << ". " << pseudoDimension << std::endl;
      NGTThrowException(msg);
    }
    createQuantizedGraphFrame(quantizedIndexPath, ngtProperty.dimension, pseudoDimension,
                              dimensionOfSubvector);
#else
    createQuantizedGraphFrame(quantizedIndexPath, ngtProperty.dimension, dimensionOfSubvector);
#endif
    return;
  }

  static void append(const std::string indexPath, QBG::BuildParameters &buildParameters);

#ifdef NGTQ_QBG
  static void quantize(const std::string indexPath, size_t dimensionOfSubvector, size_t maxNumOfEdges,
                       bool verbose = false);

  static void realign(const std::string indexPath, size_t maxNumOfEdges, bool verbose = false) {
    NGT::StdOstreamRedirector redirector(!verbose);
    redirector.begin();
    {
      std::string quantizedIndexPath = indexPath + "/" + getQGDirectoryName();
      struct stat st;
      if (stat(quantizedIndexPath.c_str(), &st) != 0) {
        std::stringstream msg;
        msg << "QuantizedGraph::quantize: Quantized graph is already existed. " << quantizedIndexPath;
        NGTThrowException(msg);
      }
      if (maxNumOfEdges == 0) {
        NGTThrowException("QuantizedGraph::quantize: The maximum number of edges is zero.");
      }
      buildQuantizedGraph(indexPath, maxNumOfEdges);
    }
    redirector.end();
  }
#else
  static void quantize(const std::string indexPath, float dimensionOfSubvector, size_t maxNumOfEdges,
                       bool verbose = false) {
    NGT::StdOstreamRedirector redirector(!verbose);
    redirector.begin();
    {
      NGT::Index index(indexPath);
      NGT::ObjectSpace &objectSpace  = index.getObjectSpace();
      std::string quantizedIndexPath = indexPath + "/" + getQGDirectoryName();
      struct stat st;
      if (stat(quantizedIndexPath.c_str(), &st) != 0) {
        NGT::Property ngtProperty;
        index.getProperty(ngtProperty);
        createQuantizedGraphFrame(quantizedIndexPath, ngtProperty.dimension, dimensionOfSubvector);
        buildQuantizedObjects(quantizedIndexPath, objectSpace);
        if (maxNumOfEdges != 0) {
          buildQuantizedGraph(indexPath, maxNumOfEdges);
        }
      }
    }
    redirector.end();
  }
#endif

  static const std::string getQGDirectoryName() { return "qg"; }

  const bool readOnly;
  const std::string path;
  NGTQ::Index quantizedIndex;
  NGTQ::Index blobIndex;

  QuantizedGraphRepository quantizedGraph;

  std::vector<NGT::DistanceSorter<>> candidateNodePools;
  std::vector<BooleanVectorByEpoch> visitPools;
};

} // namespace NGTQG

#endif
