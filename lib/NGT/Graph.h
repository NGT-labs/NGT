//
// Copyright (C) 2015 Yahoo Japan Corporation
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

#include <bitset>

#include "NGT/defines.h"
#include "NGT/Common.h"
#include "NGT/ObjectSpaceRepository.h"

#include "NGT/HashBasedBooleanSet.h"

#ifndef NGT_GRAPH_CHECK_VECTOR
#include <unordered_set>
#endif

#ifdef NGT_GRAPH_UNCHECK_STACK
#include <stack>
#endif

#ifndef NGT_EXPLORATION_COEFFICIENT
#define NGT_EXPLORATION_COEFFICIENT 1.1
#endif

#ifndef NGT_INSERTION_EXPLORATION_COEFFICIENT
#define NGT_INSERTION_EXPLORATION_COEFFICIENT 1.1
#endif

#ifndef NGT_TRUNCATION_THRESHOLD
#define NGT_TRUNCATION_THRESHOLD 50
#endif

#ifndef NGT_SEED_SIZE
#define NGT_SEED_SIZE 10
#endif

#ifndef NGT_CREATION_EDGE_SIZE
#define NGT_CREATION_EDGE_SIZE 10
#endif

namespace NGT {
class Property;

typedef GraphNode GRAPH_NODE;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
class GraphRepository : public PersistentRepository<GRAPH_NODE> {
#else
class GraphRepository : public Repository<GRAPH_NODE> {
#endif

 public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  typedef PersistentRepository<GRAPH_NODE> VECTOR;
#else
  typedef Repository<GRAPH_NODE> VECTOR;

  GraphRepository() : prevsize(0) { initialize(); }
  virtual ~GraphRepository() { destruct(); }
  void initialize() {
    destruct();
    prevsize = new vector<unsigned short>;
  }
  void destruct() {
    deleteAll();
    if (prevsize != 0) {
      delete prevsize;
      prevsize = 0;
    }
  }
#endif

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  void open(const std::string &file, size_t sharedMemorySize) {
    SharedMemoryAllocator &allocator = VECTOR::getAllocator();
    off_t *entryTable                = (off_t *)allocator.construct(file, sharedMemorySize);
    if (entryTable == 0) {
      entryTable = (off_t *)construct();
      allocator.setEntry(entryTable);
    }
    assert(entryTable != 0);
    this->initialize(entryTable);
  }

  void *construct() {
    SharedMemoryAllocator &allocator = VECTOR::getAllocator();
    off_t *entryTable                = new (allocator) off_t[2];
    entryTable[0]                    = allocator.getOffset(PersistentRepository<GRAPH_NODE>::construct());
    entryTable[1]                    = allocator.getOffset(new (allocator) Vector<unsigned short>);
    return entryTable;
  }

  void initialize(void *e) {
    SharedMemoryAllocator &allocator = VECTOR::getAllocator();
    off_t *entryTable                = (off_t *)e;
    array                            = (ARRAY *)allocator.getAddr(entryTable[0]);
    PersistentRepository<GRAPH_NODE>::initialize(allocator.getAddr(entryTable[0]));
    prevsize = (Vector<unsigned short> *)allocator.getAddr(entryTable[1]);
  }
#endif

  void insert(ObjectID id, ObjectDistances &objects) {
    GRAPH_NODE *r = allocate();
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    (*r).copy(objects, VECTOR::getAllocator());
#else
    *r = objects;
#endif
    try {
      put(id, r);
    } catch (Exception &exp) {
      delete r;
      throw exp;
    }
    if (id >= prevsize->size()) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      prevsize->resize(id + 1, VECTOR::getAllocator(), 0);
#else
      prevsize->resize(id + 1, 0);
#endif
    } else {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      (*prevsize).at(id, VECTOR::getAllocator()) = 0;
#else
      (*prevsize)[id] = 0;
#endif
    }
    return;
  }

  inline GRAPH_NODE *get(ObjectID fid, size_t &minsize) {
    GRAPH_NODE *rs = VECTOR::get(fid);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    minsize = (*prevsize).at(fid, VECTOR::getAllocator());
#else
    minsize = (*prevsize)[fid];
#endif
    return rs;
  }
  void serialize(std::ofstream &os) {
    VECTOR::serialize(os);
    Serializer::write(os, *prevsize);
  }
  void deserialize(std::ifstream &is) {
    VECTOR::deserialize(is);
    Serializer::read(is, *prevsize);
  }
  void show() {
    for (size_t i = 0; i < this->size(); i++) {
      std::cout << "Show graph " << i << " ";
      if ((*this)[i] == 0) {
        std::cout << std::endl;
        continue;
      }
      for (size_t j = 0; j < (*this)[i]->size(); j++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        std::cout << (*this)[i]->at(j, VECTOR::getAllocator()).id << ":"
                  << (*this)[i]->at(j, VECTOR::getAllocator()).distance << " ";
#else
        std::cout << (*this)[i]->at(j).id << ":" << (*this)[i]->at(j).distance << " ";
#endif
      }
      std::cout << std::endl;
    }
  }

 public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  Vector<unsigned short> *prevsize;
#else
  std::vector<unsigned short> *prevsize;
#endif
}; // namespace NGT

#ifdef NGT_GRAPH_READ_ONLY_GRAPH
#ifdef NGT_GRAPH_COMPACT_READ_ONLY_GRAPH
typedef std::vector<uint32_t> ReadOnlyGraphNode;
#else
class ReadOnlyGraphNode : public std::vector<std::pair<uint32_t, PersistentObject *>> {
  typedef std::vector<std::pair<uint32_t, PersistentObject *>> PARENT;

 public:
  ReadOnlyGraphNode() : reservedSize(0), usedSize(0) {}
  void reserve(size_t s) {
    PARENT::resize(s);
    for (size_t i = reservedSize; i < s; i++) {
      (*this)[i].first = 0;
    }
    reservedSize = s;
  }
  void push_back(std::pair<uint32_t, PersistentObject *> node) {
    (*this)[usedSize] = node;
    usedSize++;
  }
  size_t size() { return usedSize; }
  void resize(size_t s) {
    if (s <= usedSize) {
      for (size_t i = s; i < usedSize; i++) {
        (*this)[i].first = 0;
      }
      usedSize = s;
    } else {
      std::cerr << "ReadOnlyGraphNode: Not implemented" << std::endl;
      abort();
    }
  }
  size_t reservedSize;
  size_t usedSize;
};
#endif // NGT_GRAPH_COMPACT_READ_ONLY_GRAPH

class SearchGraphRepository : public std::vector<ReadOnlyGraphNode> {
 public:
  SearchGraphRepository() {}
  bool isEmpty(size_t idx) { return (*this)[idx].empty(); }

  void deserialize(std::ifstream &is, ObjectRepository &objectRepository) {
    if (!is.is_open()) {
      NGTThrowException("NGT::SearchGraph: Not open the specified stream yet.");
    }
    clear();
    size_t s;
    NGT::Serializer::read(is, s);
    resize(s);
    for (size_t id = 0; id < s; id++) {
      char type;
      NGT::Serializer::read(is, type);
      switch (type) {
      case '-': break;
      case '+': {
        ObjectDistances node;
        node.deserialize(is);
        ReadOnlyGraphNode &searchNode = at(id);
        searchNode.reserve(node.size());
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        for (auto ni = node.begin(); ni != node.end(); ni++) {
          std::cerr << "not implement" << std::endl;
          abort();
        }
#else
        for (auto ni = node.begin(); ni != node.end(); ni++) {
          try {
#ifdef NGT_GRAPH_COMPACT_READ_ONLY_GRAPH
            searchNode.push_back((*ni).id);
#else
            searchNode.push_back(std::pair<uint32_t, Object *>((*ni).id, objectRepository.get((*ni).id)));
#endif // NGT_GRAPH_COMPACT_READ_ONLY_GRAPH
          } catch (NGT::Exception &err) {
            std::stringstream msg;
            msg << "Cannot push. " << (*ni).id << " <= ";
            for (auto nix = node.begin(); nix != node.end(); nix++) {
              msg << (*nix).id << " ";
            }
            msg << ": " << err.what();
            NGTThrowException(msg);
          }
        }
#endif
      } break;
      default: {
        assert(type == '-' || type == '+');
        break;
      }
      }
    }
  }
};

#endif // NGT_GRAPH_READ_ONLY_GRAPH

class NeighborhoodGraph {
 public:
  enum GraphType {
    GraphTypeNone   = 0,
    GraphTypeANNG   = 1,
    GraphTypeKNNG   = 2,
    GraphTypeBKNNG  = 3,
    GraphTypeONNG   = 4,
    GraphTypeIANNG  = 5, // Improved ANNG
    GraphTypeDNNG   = 6,
    GraphTypeRANNG  = 7,
    GraphTypeRIANNG = 8
  };

  enum SeedType {
    SeedTypeNone         = 0,
    SeedTypeRandomNodes  = 1,
    SeedTypeFixedNodes   = 2,
    SeedTypeFirstNode    = 3,
    SeedTypeAllLeafNodes = 4
  };

  enum EpsilonType { EpsilonTypeNone = 0, EpsilonTypeByQuery = 1, EpsilonTypeResultSize = 2 };

  enum IdenticalObjectEdgeType {
    IdenticalObjectEdgeTypeNone           = 0,
    IdenticalObjectEdgeTypeDirectedEdge   = 1,
    IdenticalObjectEdgeTypeUndirectedEdge = 2,
  };

#ifdef NGT_GRAPH_READ_ONLY_GRAPH
  class Search {
   public:
    static void (*getMethod(NGT::ObjectSpace::DistanceType dtype, NGT::ObjectSpace::ObjectType otype,
                            size_t size))(NGT::NeighborhoodGraph &, NGT::SearchContainer &,
                                          NGT::ObjectDistances &) {
      if (size < 5000000) {
        switch (otype) {
        case NGT::ObjectSpace::ObjectTypeUnset:
        case NGT::ObjectSpace::Float:
          switch (dtype) {
          case NGT::ObjectSpace::DistanceTypeNormalizedCosine: return normalizedCosineSimilarityFloat;
          case NGT::ObjectSpace::DistanceTypeCosine: return cosineSimilarityFloat;
          case NGT::ObjectSpace::DistanceTypeNormalizedAngle: return normalizedAngleFloat;
          case NGT::ObjectSpace::DistanceTypeAngle: return angleFloat;
          case NGT::ObjectSpace::DistanceTypeNormalizedL2: return normalizedL2Float;
          case NGT::ObjectSpace::DistanceTypeL2: return l2Float;
          case NGT::ObjectSpace::DistanceTypeL1: return l1Float;
          case NGT::ObjectSpace::DistanceTypeSparseJaccard: return sparseJaccardFloat;
          case NGT::ObjectSpace::DistanceTypePoincare: return poincareFloat; // added by Nyapicom
          case NGT::ObjectSpace::DistanceTypeLorentz: return lorentzFloat;   // added by Nyapicom
          default: return l2Float;
          }
          break;
        case NGT::ObjectSpace::Uint8:
          switch (dtype) {
          case NGT::ObjectSpace::DistanceTypeHamming: return hammingUint8;
          case NGT::ObjectSpace::DistanceTypeJaccard: return jaccardUint8;
          case NGT::ObjectSpace::DistanceTypeL2: return l2Uint8;
          case NGT::ObjectSpace::DistanceTypeL1: return l1Uint8;
          default: return l2Uint8;
          }
          break;
#ifdef NGT_HALF_FLOAT
        case NGT::ObjectSpace::Float16:
          switch (dtype) {
          case NGT::ObjectSpace::DistanceTypeNormalizedCosine: return normalizedCosineSimilarityFloat16;
          case NGT::ObjectSpace::DistanceTypeCosine: return cosineSimilarityFloat16;
          case NGT::ObjectSpace::DistanceTypeNormalizedAngle: return normalizedAngleFloat16;
          case NGT::ObjectSpace::DistanceTypeAngle: return angleFloat16;
          case NGT::ObjectSpace::DistanceTypeNormalizedL2: return normalizedL2Float16;
          case NGT::ObjectSpace::DistanceTypeL2: return l2Float16;
          case NGT::ObjectSpace::DistanceTypeL1: return l1Float16;
          case NGT::ObjectSpace::DistanceTypeSparseJaccard: return sparseJaccardFloat16;
          case NGT::ObjectSpace::DistanceTypePoincare: return poincareFloat16; // added by Nyapicom
          case NGT::ObjectSpace::DistanceTypeLorentz: return lorentzFloat16;   // added by Nyapicom
          default: return l2Float16;
          }
          break;
#endif
        case NGT::ObjectSpace::Qsuint8:
          switch (dtype) {
          case NGT::ObjectSpace::DistanceTypeL2: return l2Qsint8;
          case NGT::ObjectSpace::DistanceTypeInnerProduct: return innerProductQsint8;
          case NGT::ObjectSpace::DistanceTypeNormalizedCosine: return normalizedCosineSimilarityQsint8;
          default: return l2Qsint8;
          }
          break;
#ifdef NGT_PQ4
        case NGT::ObjectSpace::Qint4:
          switch (dtype) {
          case NGT::ObjectSpace::DistanceTypeL2: return l2Qint4;
          case NGT::ObjectSpace::DistanceTypeCosine: return cosineSimilarityQint4;
          case NGT::ObjectSpace::DistanceTypeInnerProduct: return innerProductQint4;
          case NGT::ObjectSpace::DistanceTypeNormalizedCosine: return normalizedCosineSimilarityQint4;
          default: return l2Qint4;
          }
          break;
#endif
        default: {
          std::stringstream msg;
          msg << "NGT::Graph::Search: Not supported object type. " << otype;
          NGTThrowException(msg);
          break;
        }
        }
        return l1Uint8;
      } else {
        switch (otype) {
        case NGT::ObjectSpace::Float:
          switch (dtype) {
          case NGT::ObjectSpace::DistanceTypeNormalizedCosine:
            return normalizedCosineSimilarityFloatForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeCosine: return cosineSimilarityFloatForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeNormalizedAngle: return normalizedAngleFloatForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeAngle: return angleFloatForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeNormalizedL2: return normalizedL2FloatForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeL2: return l2FloatForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeL1: return l1FloatForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeSparseJaccard: return sparseJaccardFloatForLargeDataset;
          case NGT::ObjectSpace::DistanceTypePoincare: return poincareFloatForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeLorentz: return lorentzFloatForLargeDataset;
          default: return l2FloatForLargeDataset;
          }
          break;
        case NGT::ObjectSpace::Uint8:
          switch (dtype) {
          case NGT::ObjectSpace::DistanceTypeHamming: return hammingUint8ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeJaccard: return jaccardUint8ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeL2: return l2Uint8ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeL1: return l1Uint8ForLargeDataset;
          default: return l2Uint8ForLargeDataset;
          }
          break;
#ifdef NGT_HALF_FLOAT
        case NGT::ObjectSpace::Float16:
          switch (dtype) {
          case NGT::ObjectSpace::DistanceTypeNormalizedCosine:
            return normalizedCosineSimilarityFloat16ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeCosine: return cosineSimilarityFloat16ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeNormalizedAngle: return normalizedAngleFloat16ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeAngle: return angleFloat16ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeNormalizedL2: return normalizedL2Float16ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeL2: return l2Float16ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeL1: return l1Float16ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeSparseJaccard: return sparseJaccardFloat16ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypePoincare: return poincareFloat16ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeLorentz: return lorentzFloat16ForLargeDataset;
          default: return l2Float16ForLargeDataset;
          }
#endif
        case NGT::ObjectSpace::Qsuint8:
          switch (dtype) {
          case NGT::ObjectSpace::DistanceTypeL2: return l2Qsint8ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeInnerProduct: return innerProductQsint8ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeNormalizedCosine:
            return normalizedCosineSimilarityQsint8ForLargeDataset;
          default: return l2Qsint8ForLargeDataset;
          }
          break;
#ifdef NGT_PQ4
        case NGT::ObjectSpace::Qint4:
          switch (dtype) {
          case NGT::ObjectSpace::DistanceTypeL2: return l2Qint4ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeCosine: return cosineSimilarityQint4ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeInnerProduct: return innerProductQint4ForLargeDataset;
          case NGT::ObjectSpace::DistanceTypeNormalizedCosine:
            return normalizedCosineSimilarityQint4ForLargeDataset;
          default: return l2Qint4;
          }
          break;
#endif
        default: {
          std::stringstream msg;
          msg << "NGT::Graph::Search: Not supported object type. " << otype;
          NGTThrowException(msg);
          break;
        }
        }
        return l1Uint8ForLargeDataset;
      }
    }
    static void l1Uint8(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void l2Uint8(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void l1Float(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void l2Float(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void hammingUint8(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void jaccardUint8(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void sparseJaccardFloat(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                   ObjectDistances &seeds);
    static void cosineSimilarityFloat(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                      ObjectDistances &seeds);
    static void angleFloat(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void normalizedCosineSimilarityFloat(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                ObjectDistances &seeds);
    static void normalizedAngleFloat(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                     ObjectDistances &seeds);
    static void normalizedL2Float(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void poincareFloat(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                              ObjectDistances &seeds); // added by Nyapicom
    static void lorentzFloat(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                             ObjectDistances &seeds); // added by Nyapicom
#ifdef NGT_HALF_FLOAT
    static void l1Float16(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void l2Float16(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void sparseJaccardFloat16(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                     ObjectDistances &seeds);
    static void cosineSimilarityFloat16(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                        ObjectDistances &seeds);
    static void angleFloat16(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void normalizedCosineSimilarityFloat16(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                  ObjectDistances &seeds);
    static void normalizedAngleFloat16(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                       ObjectDistances &seeds);
    static void normalizedL2Float16(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                    ObjectDistances &seeds);
    static void poincareFloat16(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                ObjectDistances &seeds); // added by Nyapicom
    static void lorentzFloat16(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                               ObjectDistances &seeds); // added by Nyapicom
#endif
    static void l2Qsint8(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void innerProductQsint8(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                   ObjectDistances &seeds);
    static void normalizedCosineSimilarityQsint8(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                 ObjectDistances &seeds);
#ifdef NGT_PQ4
    static void l2Qint4(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void cosineSimilarityQint4(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                      ObjectDistances &seeds);
    static void innerProductQint4(NeighborhoodGraph &graph, NGT::SearchContainer &sc, ObjectDistances &seeds);
    static void normalizedCosineSimilarityQint4(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                ObjectDistances &seeds);
#endif

    static void l1Uint8ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                       ObjectDistances &seeds);
    static void l2Uint8ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                       ObjectDistances &seeds);
    static void l1FloatForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                       ObjectDistances &seeds);
    static void l2FloatForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                       ObjectDistances &seeds);
    static void hammingUint8ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                            ObjectDistances &seeds);
    static void jaccardUint8ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                            ObjectDistances &seeds);
    static void sparseJaccardFloatForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                  ObjectDistances &seeds);
    static void cosineSimilarityFloatForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                     ObjectDistances &seeds);
    static void angleFloatForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                          ObjectDistances &seeds);
    static void normalizedCosineSimilarityFloatForLargeDataset(NeighborhoodGraph &graph,
                                                               NGT::SearchContainer &sc,
                                                               ObjectDistances &seeds);
    static void normalizedAngleFloatForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                    ObjectDistances &seeds);
    static void normalizedL2FloatForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                 ObjectDistances &seeds);
    static void poincareFloatForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                             ObjectDistances &seeds);
    static void lorentzFloatForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                            ObjectDistances &seeds);
#ifdef NGT_HALF_FLOAT
    static void l1Float16ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                         ObjectDistances &seeds);
    static void l2Float16ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                         ObjectDistances &seeds);
    static void sparseJaccardFloat16ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                    ObjectDistances &seeds);
    static void cosineSimilarityFloat16ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                       ObjectDistances &seeds);
    static void angleFloat16ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                            ObjectDistances &seeds);
    static void normalizedCosineSimilarityFloat16ForLargeDataset(NeighborhoodGraph &graph,
                                                                 NGT::SearchContainer &sc,
                                                                 ObjectDistances &seeds);
    static void normalizedAngleFloat16ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                      ObjectDistances &seeds);
    static void normalizedL2Float16ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                   ObjectDistances &seeds);
    static void poincareFloat16ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                               ObjectDistances &seeds);
    static void lorentzFloat16ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                              ObjectDistances &seeds);
#endif
    static void l2Qsint8ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                        ObjectDistances &seeds);
    static void innerProductQsint8ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                  ObjectDistances &seeds);
    static void normalizedCosineSimilarityQsint8ForLargeDataset(NeighborhoodGraph &graph,
                                                                NGT::SearchContainer &sc,
                                                                ObjectDistances &seeds);
#ifdef NGT_PQ4
    static void l2Qint4ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                       ObjectDistances &seeds);
    static void cosineSimilarityQint4ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                     ObjectDistances &seeds);
    static void innerProductQint4ForLargeDataset(NeighborhoodGraph &graph, NGT::SearchContainer &sc,
                                                 ObjectDistances &seeds);
    static void normalizedCosineSimilarityQint4ForLargeDataset(NeighborhoodGraph &graph,
                                                               NGT::SearchContainer &sc,
                                                               ObjectDistances &seeds);
#endif
  };
#endif

  class Property {
   public:
    Property() { setDefault(); }
    void setDefault() {
      truncationThreshold        = 0;
      edgeSizeForCreation        = NGT_CREATION_EDGE_SIZE;
      edgeSizeForSearch          = 0;
      edgeSizeLimitForCreation   = 5;
      insertionRadiusCoefficient = NGT_INSERTION_EXPLORATION_COEFFICIENT;
      seedSize                   = NGT_SEED_SIZE;
      seedType                   = SeedTypeNone;
      truncationThreadPoolSize   = 8;
      batchSizeForCreation       = 200;
      graphType                  = GraphTypeANNG;
      dynamicEdgeSizeBase        = 30;
      dynamicEdgeSizeRate        = 20;
      buildTimeLimit             = 0.0;
      outgoingEdge               = 10;
      incomingEdge               = 80;
      epsilonType                = EpsilonTypeNone;
      identicalObjectEdgeType    = IdenticalObjectEdgeTypeNone;
    }
    void clear() {
      truncationThreshold        = -1;
      edgeSizeForCreation        = -1;
      edgeSizeForSearch          = -1;
      edgeSizeLimitForCreation   = -1;
      insertionRadiusCoefficient = -1;
      seedSize                   = -1;
      seedType                   = SeedTypeNone;
      truncationThreadPoolSize   = -1;
      batchSizeForCreation       = -1;
      graphType                  = GraphTypeNone;
      dynamicEdgeSizeBase        = -1;
      dynamicEdgeSizeRate        = -1;
      buildTimeLimit             = -1;
      outgoingEdge               = -1;
      incomingEdge               = -1;
      epsilonType                = -1;
      identicalObjectEdgeType    = -1;
    }
    void set(NGT::Property &prop);
    void get(NGT::Property &prop);

    void exportProperty(NGT::PropertySet &p) {
      p.set("IncrimentalEdgeSizeLimitForTruncation", truncationThreshold);
      p.set("EdgeSizeForCreation", edgeSizeForCreation);
      p.set("EdgeSizeForSearch", edgeSizeForSearch);
      p.set("EdgeSizeLimitForCreation", edgeSizeLimitForCreation);
      assert(insertionRadiusCoefficient >= 1.0);
      p.set("EpsilonForCreation", insertionRadiusCoefficient - 1.0);
      p.set("BatchSizeForCreation", batchSizeForCreation);
      p.set("SeedSize", seedSize);
      p.set("TruncationThreadPoolSize", truncationThreadPoolSize);
      p.set("DynamicEdgeSizeBase", dynamicEdgeSizeBase);
      p.set("DynamicEdgeSizeRate", dynamicEdgeSizeRate);
      p.set("BuildTimeLimit", buildTimeLimit);
      p.set("OutgoingEdge", outgoingEdge);
      p.set("IncomingEdge", incomingEdge);
      switch (graphType) {
      case NeighborhoodGraph::GraphTypeKNNG: p.set("GraphType", "KNNG"); break;
      case NeighborhoodGraph::GraphTypeANNG: p.set("GraphType", "ANNG"); break;
      case NeighborhoodGraph::GraphTypeBKNNG: p.set("GraphType", "BKNNG"); break;
      case NeighborhoodGraph::GraphTypeONNG: p.set("GraphType", "ONNG"); break;
      case NeighborhoodGraph::GraphTypeIANNG: p.set("GraphType", "IANNG"); break;
      case NeighborhoodGraph::GraphTypeRANNG: p.set("GraphType", "RANNG"); break;
      case NeighborhoodGraph::GraphTypeRIANNG: p.set("GraphType", "RIANNG"); break;
      default:
        std::cerr << "Graph::exportProperty: Fatal error! Invalid Graph Type. " << graphType << std::endl;
        abort();
      }
      switch (seedType) {
      case NeighborhoodGraph::SeedTypeRandomNodes: p.set("SeedType", "RandomNodes"); break;
      case NeighborhoodGraph::SeedTypeFixedNodes: p.set("SeedType", "FixedNodes"); break;
      case NeighborhoodGraph::SeedTypeFirstNode: p.set("SeedType", "FirstNode"); break;
      case NeighborhoodGraph::SeedTypeNone: p.set("SeedType", "None"); break;
      case NeighborhoodGraph::SeedTypeAllLeafNodes: p.set("SeedType", "AllLeafNodes"); break;
      default:
        std::cerr << "Graph::exportProperty: Fatal error! Invalid Seed Type. " << seedType << std::endl;
        abort();
      }
      switch (epsilonType) {
      case EpsilonTypeNone: p.set("EpsilonType", "None"); break;
      case EpsilonTypeByQuery: p.set("EpsilonType", "ByQuery"); break;
      case EpsilonTypeResultSize: p.set("EpsilonType", "ResultSize"); break;
      default: std::cerr << "Fatal error. Invalid epsilon type. " << epsilonType << std::endl; abort();
      }
      switch (identicalObjectEdgeType) {
      case IdenticalObjectEdgeTypeNone: p.set("IdenticalObjectEdgeType", "None"); break;
      case IdenticalObjectEdgeTypeDirectedEdge: p.set("IdenticalObjectEdgeType", "DirectedEdge"); break;
      case IdenticalObjectEdgeTypeUndirectedEdge: p.set("IdenticalObjectEdgeType", "UndirectedEdge"); break;
      default:
        std::cerr << "Fatal error. Invalid identical object edge type. " << identicalObjectEdgeType
                  << std::endl;
        abort();
      }
    }
    void importProperty(NGT::PropertySet &p) {
      setDefault();
      truncationThreshold        = p.getl("IncrimentalEdgeSizeLimitForTruncation", truncationThreshold);
      edgeSizeForCreation        = p.getl("EdgeSizeForCreation", edgeSizeForCreation);
      edgeSizeForSearch          = p.getl("EdgeSizeForSearch", edgeSizeForSearch);
      edgeSizeLimitForCreation   = p.getl("EdgeSizeLimitForCreation", edgeSizeLimitForCreation);
      insertionRadiusCoefficient = p.getf("EpsilonForCreation", insertionRadiusCoefficient);
      insertionRadiusCoefficient += 1.0;
      batchSizeForCreation     = p.getl("BatchSizeForCreation", batchSizeForCreation);
      seedSize                 = p.getl("SeedSize", seedSize);
      truncationThreadPoolSize = p.getl("TruncationThreadPoolSize", truncationThreadPoolSize);
      dynamicEdgeSizeBase      = p.getl("DynamicEdgeSizeBase", dynamicEdgeSizeBase);
      dynamicEdgeSizeRate      = p.getl("DynamicEdgeSizeRate", dynamicEdgeSizeRate);
      buildTimeLimit           = p.getf("BuildTimeLimit", buildTimeLimit);
      outgoingEdge             = p.getl("OutgoingEdge", outgoingEdge);
      incomingEdge             = p.getl("IncomingEdge", incomingEdge);
      PropertySet::iterator it = p.find("GraphType");
      if (it != p.end()) {
        if (it->second == "KNNG") graphType = NeighborhoodGraph::GraphTypeKNNG;
        else if (it->second == "ANNG")
          graphType = NeighborhoodGraph::GraphTypeANNG;
        else if (it->second == "BKNNG")
          graphType = NeighborhoodGraph::GraphTypeBKNNG;
        else if (it->second == "ONNG")
          graphType = NeighborhoodGraph::GraphTypeONNG;
        else if (it->second == "IANNG")
          graphType = NeighborhoodGraph::GraphTypeIANNG;
        else if (it->second == "RANNG")
          graphType = NeighborhoodGraph::GraphTypeRANNG;
        else if (it->second == "RIANNG")
          graphType = NeighborhoodGraph::GraphTypeRIANNG;
        else {
          std::cerr << "Graph::importProperty: Fatal error! Invalid Graph Type. " << it->second << std::endl;
          abort();
        }
      }
      it = p.find("SeedType");
      if (it != p.end()) {
        if (it->second == "RandomNodes") seedType = NeighborhoodGraph::SeedTypeRandomNodes;
        else if (it->second == "FixedNodes")
          seedType = NeighborhoodGraph::SeedTypeFixedNodes;
        else if (it->second == "FirstNode")
          seedType = NeighborhoodGraph::SeedTypeFirstNode;
        else if (it->second == "None")
          seedType = NeighborhoodGraph::SeedTypeNone;
        else if (it->second == "AllLeafNodes")
          seedType = NeighborhoodGraph::SeedTypeAllLeafNodes;
        else {
          std::cerr << "Graph::importProperty: Fatal error! Invalid Seed Type. " << it->second << std::endl;
          abort();
        }
      }
      it = p.find("EpsilonType");
      if (it != p.end()) {
        if (it->second == "None") {
          epsilonType = EpsilonTypeNone;
        } else if (it->second == "ByQuery") {
          epsilonType = EpsilonTypeByQuery;
        } else if (it->second == "ResultSize") {
          epsilonType = EpsilonTypeResultSize;
        } else {
          std::cerr << "Invalid Epsilon Type in the property. " << it->first << ":" << it->second
                    << std::endl;
        }
      } else {
        std::cerr << "Not found \"EpsilonType\"" << std::endl;
        epsilonType = EpsilonTypeNone;
      }
      it = p.find("IdenticalObjectEdgeType");
      if (it != p.end()) {
        if (it->second == "None") {
          identicalObjectEdgeType = IdenticalObjectEdgeTypeNone;
        } else if (it->second == "DirectedEdge") {
          identicalObjectEdgeType = IdenticalObjectEdgeTypeDirectedEdge;
        } else if (it->second == "UndirectedEdge") {
          identicalObjectEdgeType = IdenticalObjectEdgeTypeUndirectedEdge;
        } else {
          std::cerr << "Invalid Identical Object Edge Type in the property. " << it->first << ":"
                    << it->second << std::endl;
        }
      } else {
        identicalObjectEdgeType = IdenticalObjectEdgeTypeNone;
      }
    }
    friend std::ostream &operator<<(std::ostream &os, const Property &p) {
      os << "truncationThreshold=" << p.truncationThreshold << std::endl;
      os << "edgeSizeForCreation=" << p.edgeSizeForCreation << std::endl;
      os << "edgeSizeForSearch=" << p.edgeSizeForSearch << std::endl;
      os << "edgeSizeLimitForCreation=" << p.edgeSizeLimitForCreation << std::endl;
      os << "insertionRadiusCoefficient=" << p.insertionRadiusCoefficient << std::endl;
      os << "insertionRadiusCoefficient=" << p.insertionRadiusCoefficient << std::endl;
      os << "seedSize=" << p.seedSize << std::endl;
      os << "seedType=" << p.seedType << std::endl;
      os << "truncationThreadPoolSize=" << p.truncationThreadPoolSize << std::endl;
      os << "batchSizeForCreation=" << p.batchSizeForCreation << std::endl;
      os << "graphType=" << p.graphType << std::endl;
      os << "dynamicEdgeSizeBase=" << p.dynamicEdgeSizeBase << std::endl;
      os << "dynamicEdgeSizeRate=" << p.dynamicEdgeSizeRate << std::endl;
      os << "outgoingEdge=" << p.outgoingEdge << std::endl;
      os << "incomingEdge=" << p.incomingEdge << std::endl;
      os << "epsilonType=" << p.epsilonType << std::endl;
      os << "identicalObjectEdgeType=" << p.identicalObjectEdgeType << std::endl;
      return os;
    }

    int16_t truncationThreshold;
    int16_t edgeSizeForCreation;
    int16_t edgeSizeForSearch;
    int16_t edgeSizeLimitForCreation;
    double insertionRadiusCoefficient;
    int16_t seedSize;
    SeedType seedType;
    int16_t truncationThreadPoolSize;
    int16_t batchSizeForCreation;
    GraphType graphType;
    int16_t dynamicEdgeSizeBase;
    int16_t dynamicEdgeSizeRate;
    float buildTimeLimit;
    int16_t outgoingEdge;
    int16_t incomingEdge;
    int16_t epsilonType;
    int16_t identicalObjectEdgeType;
  };

  NeighborhoodGraph() : objectSpace(0) {
    property.truncationThreshold = NGT_TRUNCATION_THRESHOLD;
    // initialize random to generate random seeds
#ifdef NGT_ENABLE_TIME_SEED_FOR_RANDOM
    struct timeval randTime;
    gettimeofday(&randTime, 0);
    srand(randTime.tv_usec);
#endif
  }

  inline GraphNode *getNode(ObjectID fid, size_t &minsize) { return repository.get(fid, minsize); }
  inline GraphNode *getNode(ObjectID fid) { return repository.VECTOR::get(fid); }
  void insertNode(ObjectID id, ObjectDistances &objects) {
    switch (property.graphType) {
    case GraphTypeANNG:
      if (property.identicalObjectEdgeType == IdenticalObjectEdgeTypeNone) {
        insertANNGNode(id, objects);
      } else {
        insertANNGNodeForIdenticalObjects(id, objects);
      }
      break;
    case GraphTypeRANNG: insertANNGNode(id, objects); break;
    case GraphTypeIANNG:
    case GraphTypeRIANNG: insertIANNGNode(id, objects); break;
    case GraphTypeONNG: insertONNGNode(id, objects); break;
    case GraphTypeKNNG: insertKNNGNode(id, objects); break;
    case GraphTypeBKNNG: insertBKNNGNode(id, objects); break;
    case GraphTypeNone: NGTThrowException("NGT::insertNode: GraphType is not specified."); break;
    default: NGTThrowException("NGT::insertNode: GraphType is invalid."); break;
    }
  }

  void insertBKNNGNode(ObjectID id, ObjectDistances &results) {
    if (repository.isEmpty(id)) {
      repository.insert(id, results);
    } else {
      GraphNode &rs = *getNode(id);
      for (ObjectDistances::iterator ri = results.begin(); ri != results.end(); ri++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        rs.push_back((*ri), repository.allocator);
#else
        rs.push_back((*ri));
#endif
      }
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      std::sort(rs.begin(repository.allocator), rs.end(repository.allocator));
      ObjectID prev = 0;
      for (GraphNode::iterator ri = rs.begin(repository.allocator); ri != rs.end(repository.allocator);) {
        if (prev == (*ri).id) {
          ri = rs.erase(ri, repository.allocator);
          continue;
        }
        prev = (*ri).id;
        ri++;
      }
#else
      std::sort(rs.begin(), rs.end());
      ObjectID prev = 0;
      for (GraphNode::iterator ri = rs.begin(); ri != rs.end();) {
        if (prev == (*ri).id) {
          ri = rs.erase(ri);
          continue;
        }
        prev = (*ri).id;
        ri++;
      }
#endif
    }
    for (ObjectDistances::iterator ri = results.begin(); ri != results.end(); ri++) {
      assert(id != (*ri).id);
      addBKNNGEdge((*ri).id, id, (*ri).distance);
    }
    return;
  }

  void insertKNNGNode(ObjectID id, ObjectDistances &results) { repository.insert(id, results); }

  void insertANNGNode(ObjectID id, ObjectDistances &results) {
    repository.insert(id, results);
    std::queue<ObjectID> truncateQueue;
    for (ObjectDistances::iterator ri = results.begin(); ri != results.end(); ri++) {
      assert(id != (*ri).id);
      if (addEdge((*ri).id, id, (*ri).distance)) {
        truncateQueue.push((*ri).id);
      }
    }
    while (!truncateQueue.empty()) {
      ObjectID tid = truncateQueue.front();
      truncateEdges(tid);
      truncateQueue.pop();
    }
    return;
  }

  void filterDuplicatesInResults(ObjectDistances &results) {
    if (results.size() <= 1) {
      return;
    }
    ObjectDistances filtered;
    size_t i = 0;
    while (i < results.size()) {
      float currentDistance = results[i].distance;

      size_t j = i + 1;
      while (j < results.size() && results[j].distance == currentDistance) {
        j++;
      }

      if (j - i == 1) {
        filtered.push_back(results[i]);
      } else {
        std::vector<std::tuple<NGT::Object *, ObjectID, int32_t>> groups;

        for (size_t k = i; k < j; k++) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          NGT::Object *obj = objectSpace->allocateObject(*getObjectRepository().get(results[k].id));
#else
          auto *obj = getObjectRepository().get(results[k].id);
#endif
          int32_t edges = getNode(results[k].id)->size();
          bool found    = false;

          for (auto &g : groups) {
            if (objectSpace->compareWithL1(*std::get<0>(g), *obj) == 0.0) {
              if (edges > std::get<2>(g)) {
                std::get<1>(g) = results[k].id;
                std::get<2>(g) = edges;
              }
              found = true;
              break;
            }
          }

          if (!found) {
            groups.emplace_back(obj, results[k].id, edges);
          }
        }

        for (auto &g : groups) {
          filtered.push_back(ObjectDistance(std::get<1>(g), currentDistance));
        }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        for (auto &g : groups) {
          objectSpace->deleteObject(std::get<0>(g));
        }
#endif
      }

      i = j;
    }
    results = std::move(filtered);
  }

  void insertANNGNodeForIdenticalObjects(ObjectID id, ObjectDistances &results) {
    filterDuplicatesInResults(results);
    if (results.size() == 0 || results[0].distance != 0.0) {
      insertANNGNode(id, results);
      return;
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    NGT::Object *insertedObj = objectSpace->allocateObject(*getObjectRepository().get(id));
#else
    NGT::Object *insertedObj = getObjectRepository().get(id);
#endif
    auto *firstResultObj = getObjectRepository().get(results[0].id);
    if (objectSpace->compareWithL1(*insertedObj, *firstResultObj) != 0.0) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      objectSpace->deleteObject(insertedObj);
#endif
      insertANNGNode(id, results);
      return;
    }
    int32_t max    = -1;
    ObjectID maxid = 0;
    for (ObjectDistances::iterator ri = results.begin(); ri != results.end(); ri++) {
      auto *resultObj = getObjectRepository().get((*ri).id);
      if (objectSpace->compareWithL1(*insertedObj, *resultObj) != 0.0) break;
      GraphNode &node = *getNode((*ri).id);
      if (static_cast<int32_t>(node.size()) > max) {
        max   = node.size();
        maxid = (*ri).id;
      }
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    objectSpace->deleteObject(insertedObj);
#endif
    if (max < 0) {
      std::stringstream msg;
      msg << "Fatal inner error. Cannot find the identical object. id=" << id << std::endl;
      NGTThrowException(msg);
    }
    ObjectDistances edges;
    if (property.identicalObjectEdgeType == IdenticalObjectEdgeTypeUndirectedEdge) {
      edges.push_back(ObjectDistance(maxid, 0.0));
    }
    repository.insert(id, edges);
    addEdge(maxid, id, 0.0);
    return;
  }
  void insertIANNGNode(ObjectID id, ObjectDistances &results) {
    repository.insert(id, results);
    size_t nOfEdges = std::max(property.incomingEdge, property.outgoingEdge);
    nOfEdges        = nOfEdges != 0 ? nOfEdges : property.edgeSizeForCreation;
    for (ObjectDistances::iterator ri = results.begin(); ri != results.end(); ri++) {
      assert(id != (*ri).id);
      addEdgeWithDeletion((*ri).id, id, (*ri).distance, nOfEdges);
    }
    return;
  }

  void deleteShortcutEdges(ObjectDistances &srcNode) {
    std::vector<bool> removedEdge(srcNode.size(), false);
    for (uint32_t rank = 1; rank < srcNode.size(); rank++) {
      auto dstNodeID       = srcNode[rank].id;
      auto dstNodeDistance = srcNode[rank].distance;
#ifdef NGT_SHORTCUT_REDUCTION_WITH_ANGLE
      auto dstNodeDistance2 = dstNodeDistance * dstNodeDistance;
#endif
      bool found = false;
      for (size_t sni = 0; sni < srcNode.size() && sni < rank; sni++) {
        if (removedEdge[sni]) continue;
        auto pathNodeID = srcNode[sni].id;
#ifdef NGT_SHORTCUT_REDUCTION_WITH_ANGLE
        auto srcNodeDistance2 = srcNode[sni].distance * srcNode[sni].distance;
#else
        if (srcNode[sni].distance >= dstNodeDistance) continue;
#endif
        NGT::GraphNode &pathNode = *getNode(pathNodeID);
        for (size_t pni = 0; pni < pathNode.size(); pni++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          auto nodeID = pathNode.at(pni, repository.allocator).id;
#else
          auto nodeID = pathNode[pni].id;
#endif
          if (nodeID != dstNodeID) continue;
#ifdef NGT_SHORTCUT_REDUCTION_WITH_ANGLE
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          auto pathNodeDistance2 = pathNode.at(pni, outGraph.repository.allocator).distance *
                                   pathNode.at(pni, repository.allocator).distance;
#else
          auto pathNodeDistance2 = pathNode[pni].distance * pathNode[pni].distance;
#endif
          auto v1 = srcNodeDistance2 + pathNodeDistance2 - dstNodeDistance2;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          auto v2 = 2.0 * srcNode.at(sni, outGraph.repository.allocator).distance *
                    pathNode.at(pni, repository.allocator).distance;
#else
          auto v2 = 2.0 * srcNode[sni].distance * pathNode[pni].distance;
#endif
          if (cosAlpha >= range) {
            break;
          }
#else
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          if (pathNode.at(pni, repository.allocator).distance >= dstNodeDistance) break;
#else
          if (pathNode[pni].distance >= dstNodeDistance) break;
#endif
#ifdef NGT_SHORTCUT_REDUCTION_WITH_ADDITIONAL_CONDITION
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          if (srcNode[sni].allocator).distance + pathNode.at(pni, repository.allocator).distance >= dstNodeDistance * range) break;
#else
          if (srcNode[sni].distance + pathNode[pni].distance >= dstNodeDistance * range) break;
#endif
#endif
#endif
          found = true;
          break;
        }
        if (found) {
          removedEdge[rank] = true;
        }
      }
    }
    {
      size_t idx = 0;
      for (auto e = srcNode.begin(); e != srcNode.end(); idx++) {
        if (removedEdge[idx]) {
          e = srcNode.erase(e);
        } else {
          ++e;
        }
      }
    }
  }

  void insertONNGNode(ObjectID id, ObjectDistances &results) {
    if (property.truncationThreshold != 0) {
      std::stringstream msg;
      msg << "NGT::insertONNGNode: truncation should be disabled!" << std::endl;
      NGTThrowException(msg);
    }
    int count = 0;
    for (ObjectDistances::iterator ri = results.begin(); ri != results.end(); ri++, count++) {
      assert(id != (*ri).id);
      if (count >= property.incomingEdge) {
        break;
      }
      addEdge((*ri).id, id, (*ri).distance);
    }
    if (static_cast<int>(results.size()) > property.outgoingEdge) {
      results.resize(property.outgoingEdge);
    }
    repository.insert(id, results);
  }

  void removeEdgesReliably(ObjectID id);

  int truncateEdgesOptimally(ObjectID id, GraphNode &results, size_t truncationSize);

  int truncateEdges(ObjectID id) {
    GraphNode &results = *getNode(id);
    if (results.size() == 0) {
      return -1;
    }

    size_t truncationSize = NGT_TRUNCATION_THRESHOLD;
    if (truncationSize < (size_t)property.edgeSizeForCreation) {
      truncationSize = property.edgeSizeForCreation;
    }
    return truncateEdgesOptimally(id, results, truncationSize);
  }

  // setup edgeSize
  inline size_t getEdgeSize(NGT::SearchContainer &sc) {
    int64_t esize   = sc.edgeSize == -1 ? property.edgeSizeForSearch : sc.edgeSize;
    size_t edgeSize = INT_MAX;

    if (esize == 0) {
      edgeSize = INT_MAX;
    } else if (esize > 0) {
      edgeSize = esize;
    } else if (esize == -2) {
      double add =
          pow(10, (sc.explorationCoefficient - 1.0) * static_cast<float>(property.dynamicEdgeSizeRate));
      edgeSize = add >= static_cast<double>(INT_MAX) ? INT_MAX : property.dynamicEdgeSizeBase + add;
    } else {
      std::stringstream msg;
      msg << "NGT::getEdgeSize: Invalid edge size parameters " << sc.edgeSize << ":"
          << property.edgeSizeForSearch;
      NGTThrowException(msg);
    }
    return edgeSize;
  }

  void search(NGT::SearchContainer &sc, ObjectDistances &seeds);

#ifdef NGT_GRAPH_READ_ONLY_GRAPH
  template <typename COMPARATOR, typename CHECK_LIST>
  void searchReadOnlyGraph(NGT::SearchContainer &sc, ObjectDistances &seeds);
#endif

  void removeEdge(ObjectID fid, ObjectID rmid) {
    GraphNode &rs = *getNode(fid);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    for (GraphNode::iterator ri = rs.begin(repository.allocator); ri != rs.end(repository.allocator); ri++) {
      if ((*ri).id == rmid) {
        rs.erase(ri, repository.allocator);
        break;
      }
    }
#else
    for (GraphNode::iterator ri = rs.begin(); ri != rs.end(); ri++) {
      if ((*ri).id == rmid) {
        rs.erase(ri);
        break;
      }
    }
#endif
  }

  void removeEdge(GraphNode &node, ObjectDistance &edge) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    GraphNode::iterator ni =
        std::lower_bound(node.begin(repository.allocator), node.end(repository.allocator), edge);
    if (ni != node.end(repository.allocator) && (*ni).id == edge.id) {
      node.erase(ni, repository.allocator);
#else
    GraphNode::iterator ni = std::lower_bound(node.begin(), node.end(), edge);
    if (ni != node.end() && (*ni).id == edge.id) {
      node.erase(ni);
#endif
      return;
    }
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    if (ni == node.end(repository.allocator)) {
#else
    if (ni == node.end()) {
#endif
      std::stringstream msg;
      msg << "NGT::removeEdge: Cannot found " << edge.id;
      NGTThrowException(msg);
    } else {
      std::stringstream msg;
      msg << "NGT::removeEdge: Cannot found " << (*ni).id << ":" << edge.id;
      NGTThrowException(msg);
    }
  }

  void removeNode(ObjectID id) { repository.erase(id); }

  class BooleanVector : public std::vector<bool> {
   public:
    inline BooleanVector(size_t s) : std::vector<bool>(s, false) {}
    inline void insert(size_t i) { std::vector<bool>::operator[](i) = true; }
  };

#ifdef NGT_GRAPH_VECTOR_RESULT
  typedef ObjectDistances ResultSet;
#else
  typedef NGT::ResultSet ResultSet;
#endif

#if defined(NGT_GRAPH_CHECK_BOOLEANSET)
  typedef BooleanSet DistanceCheckedSet;
#elif defined(NGT_GRAPH_CHECK_VECTOR)
  typedef BooleanVector DistanceCheckedSet;
#elif defined(NGT_GRAPH_CHECK_HASH_BASED_BOOLEAN_SET)
typedef HashBasedBooleanSet<uint32_t> DistanceCheckedSet;
#else
class DistanceCheckedSet : public unordered_set<ObjectID> {
 public:
  bool operator[](ObjectID id) { return find(id) != end(); }
};
#endif

  typedef HashBasedBooleanSet<uint32_t> DistanceCheckedSetForLargeDataset;

  class NodeWithPosition : public ObjectDistance {
   public:
    NodeWithPosition(uint32_t p = 0) : position(p) {}
    NodeWithPosition(ObjectDistance &o) : ObjectDistance(o), position(0) {}
    NodeWithPosition &operator=(const NodeWithPosition &n) {
      ObjectDistance::operator=(static_cast<const ObjectDistance &>(n));
      position = n.position;
      assert(id != 0);
      return *this;
    }
    uint32_t position;
  };

#ifdef NGT_GRAPH_UNCHECK_STACK
  typedef std::stack<ObjectDistance> UncheckedSet;
#else
#ifdef NGT_GRAPH_BETTER_FIRST_RESTORE
  typedef std::priority_queue<NodeWithPosition, std::vector<NodeWithPosition>, std::greater<NodeWithPosition>>
      UncheckedSet;
#else
  typedef std::priority_queue<ObjectDistance, std::vector<ObjectDistance>, std::greater<ObjectDistance>>
      UncheckedSet;
#endif
#endif
  void setupDistances(NGT::SearchContainer &sc, ObjectDistances &seeds);
  void setupDistances(NGT::SearchContainer &sc, ObjectDistances &seeds, NGT::ObjectSpace::Comparator &comp);
  void setupDistances(NGT::SearchContainer &sc, ObjectDistances &seeds,
                      double (&comparator)(const void *, const void *, size_t));

  void setupSeeds(SearchContainer &sc, ObjectDistances &seeds, ResultSet &results, UncheckedSet &unchecked,
                  DistanceCheckedSet &distanceChecked);

#if !defined(NGT_GRAPH_CHECK_HASH_BASED_BOOLEAN_SET)
  void setupSeeds(SearchContainer &sc, ObjectDistances &seeds, ResultSet &results, UncheckedSet &unchecked,
                  DistanceCheckedSetForLargeDataset &distanceChecked);
#endif

  int getEdgeSize() { return property.edgeSizeForCreation; }

  ObjectRepository &getObjectRepository() { return objectSpace->getRepository(); }

  ObjectSpace &getObjectSpace() { return *objectSpace; }
#ifdef NGT_REFINEMENT
  ObjectSpace &getRefinementObjectSpace() { return *refinementObjectSpace; }
#endif
  void deleteInMemory() {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    assert(0);
#else
    for (std::vector<NGT::GraphNode *>::iterator i = repository.begin(); i != repository.end(); i++) {
      if ((*i) != 0) {
        delete (*i);
      }
    }
    repository.clear();
#endif
  }

 protected:
  void addBKNNGEdge(ObjectID target, ObjectID addID, Distance addDistance) {
    if (repository.isEmpty(target)) {
      ObjectDistances objs;
      objs.push_back(ObjectDistance(addID, addDistance));
      repository.insert(target, objs);
      return;
    }
    addEdge(target, addID, addDistance, false);
  }

 public:
  void addEdge(GraphNode &node, ObjectID addID, Distance addDistance, bool identityCheck = true) {
    ObjectDistance obj(addID, addDistance);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    GraphNode::iterator ni =
        std::lower_bound(node.begin(repository.allocator), node.end(repository.allocator), obj);
    if ((ni != node.end(repository.allocator)) && ((*ni).id == addID)) {
      if (identityCheck) {
        std::stringstream msg;
        msg << "NGT::addEdge: already existed! " << (*ni).id << ":" << addID;
        NGTThrowException(msg);
      }
      return;
    }
#else
    GraphNode::iterator ni = std::lower_bound(node.begin(), node.end(), obj);
    if ((ni != node.end()) && ((*ni).id == addID)) {
      if (identityCheck) {
        std::stringstream msg;
        msg << "NGT::addEdge: already existed! " << (*ni).id << ":" << addID;
        NGTThrowException(msg);
      }
      return;
    }
#endif
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    node.insert(ni, obj, repository.allocator);
#else
    node.insert(ni, obj);
#endif
  }

  // identityCheck is checking whether the same edge has already added to the node.
  // return whether truncation is needed that means the node has too many edges.
  bool addEdge(ObjectID target, ObjectID addID, Distance addDistance, bool identityCheck = true) {
    size_t minsize  = 0;
    GraphNode &node = property.truncationThreshold == 0 ? *getNode(target) : *getNode(target, minsize);
    try {
      addEdge(node, addID, addDistance, identityCheck);
    } catch (NGT::Exception &err) {
      std::stringstream msg;
      msg << " Cannot add the edge. " << target << "->" << addID << ". " << err.what();
      NGTThrowException(msg);
    }
    if ((size_t)property.truncationThreshold != 0 &&
        node.size() - minsize > (size_t)property.truncationThreshold) {
      return true;
    }
    return false;
  }

  void addEdgeWithDeletion(ObjectID target, ObjectID addID, Distance addDistance, size_t kEdge,
                           bool identityCheck = true) {
    GraphNode &node = *getNode(target);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    try {
      while (node.size() >= kEdge && node.back(repository.allocator).distance > addDistance) {
        removeEdge(node, node.back(repository.allocator));
      }
    } catch (Exception &exp) {
      std::stringstream msg;
      msg << "addEdge: Cannot remove. (b) " << target << "," << addID << ","
          << node.at(kEdge - 1, repository.allocator).id;
      msg << ":" << exp.what();
      NGTThrowException(msg.str());
    }
#else
    try {
      while (node.size() >= kEdge && node.back().distance > addDistance) {
        removeEdge(node, node.back());
      }
    } catch (Exception &exp) {
      std::stringstream msg;
      msg << "addEdge: Cannot remove. (b) " << target << "," << addID << "," << node[kEdge - 1].distance;
      msg << ":" << exp.what();
      NGTThrowException(msg.str());
    }
#endif
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
    if (node.size() < kEdge) {
      addEdge(node, addID, addDistance, identityCheck);
      if (node.capacity() > kEdge) {
        node.shrink_to_fit();
      }
    }
#endif
  }

#ifdef NGT_GRAPH_READ_ONLY_GRAPH
  void loadSearchGraph(const std::string &database) {
    std::ifstream isg(database + "/grp");
    NeighborhoodGraph::searchRepository.deserialize(isg, NeighborhoodGraph::getObjectRepository());
  }
#endif

  void checkEdgeLengths(size_t n) {
    NGT::GraphRepository &graph = repository;
    NGT::ObjectRepository &repo = objectSpace->getRepository();
    auto &comparator            = objectSpace->getComparator();
    size_t nOfEdges             = 0;
    size_t nOfDifferentEdges    = 0;
    for (size_t id = 1; id < graph.size(); id++) {
      if (repo.size() <= id) break;
      if (repo[id] == 0) continue;
      if (graph.isEmpty(id)) continue;
      NGT::GraphNode *node = 0;
      try {
        node = getNode(id);
      } catch (NGT::Exception &err) {
        std::cerr << "checkEdgeLengths: Warning! Cannot get the node. " << id << std::endl;
        continue;
      }
      NGT::PersistentObject *obj = repo.get(id);
      for (size_t e = 0; e < node->size(); e++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        NGT::ObjectDistance &n = (*node).at(e, graph.allocator);
#else
        NGT::ObjectDistance &n = (*node)[e];
#endif
        if (n.id == 0) continue;
        if (!repo.isEmpty(n.id)) {
          nOfEdges++;
          float d = comparator(*obj, *repo.get(n.id));
          if (d != n.distance) {
            nOfDifferentEdges++;
          }
        }
      }
      if (n != 0 && nOfEdges > n) break;
    }
    if (nOfDifferentEdges != 0) {
      std::cerr << "checkEdgeLengths: Warning! The indexed edge lengths are different. " << nOfDifferentEdges
                << "/" << nOfEdges << "." << std::endl;
    }
  }

 public:
  GraphRepository repository;
  ObjectSpace *objectSpace;
#ifdef NGT_REFINEMENT
  ObjectSpace *refinementObjectSpace;
#endif
#ifdef NGT_GRAPH_READ_ONLY_GRAPH
  SearchGraphRepository searchRepository;
#endif

  NeighborhoodGraph::Property property;

}; // NeighborhoodGraph

} // namespace NGT
