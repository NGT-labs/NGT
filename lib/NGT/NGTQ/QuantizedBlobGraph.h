//
// Copyright (C) 2021 Yahoo Japan Corporation
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

#ifdef NGTQ_QBG
#include "NGT/NGTQ/QuantizedGraph.h"
#include "NGT/NGTQ/Optimizer.h"
#include "NGT/NGTQ/HierarchicalKmeans.h"

#include <thread>

namespace QBG {
class CreationParameters {
 public:
  CreationParameters() { setDefault(); }
  void setDefault() {
    numOfObjects       = 0;
    threadSize         = 24;
    numOfLocalClusters = 16;
    dimension          = 0;
#ifdef NGTQ_QBG
    genuineDimension     = 0;
    dimensionOfSubvector = 1;
    genuineDataType      = ObjectFile::DataTypeFloat;
#endif
    dataType                         = NGTQ::DataTypeFloat;
    distanceType                     = NGTQ::DistanceType::DistanceTypeL2;
    singleLocalCodebook              = false;
    numOfSubvectors                  = 0;
    batchSize                        = 1000;
    centroidCreationMode             = NGTQ::CentroidCreationModeStaticLayer;
    localCentroidCreationMode        = NGTQ::CentroidCreationModeStatic;
    localIDByteSize                  = 1;
    localClusteringSampleCoefficient = 10;
    refinementDataType               = NGTQ::DataTypeNone;
    orderedSearchRanking             = true;
    localClusterDataType             = NGTQ::ClusterDataTypePQ4;
    scalarQuantizationClippingRate   = 0.01;
    scalarQuantizationNoOfSamples    = 0;
#ifdef NGTQ_QUANTIZED_TREE
    maxObjectsPerNode = -1;
#endif

    globalEdgeSizeForCreation        = 10;
    globalEdgeSizeForSearch          = 40;
    globalIndexType                  = NGT::Property::GraphAndTree;
    globalInsertionRadiusCoefficient = 1.1;
    globalGraphType                  = NGT::NeighborhoodGraph::GraphTypeANNG;
    globalObjectType                 = NGT::ObjectSpace::ObjectType::Float;

    localIndexType                  = NGT::Property::GraphAndTree;
    localInsertionRadiusCoefficient = 1.1;
    localGraphType                  = NGT::NeighborhoodGraph::GraphTypeANNG;

    verbose = false;
  }

  static void setProperties(CreationParameters &creation, NGTQ::Property &property,
                            NGT::Property &globalProperty, NGT::Property &localProperty) {
    property.threadSize          = creation.threadSize;
    property.globalCentroidLimit = 0;
    property.localCentroidLimit  = creation.numOfLocalClusters;
    property.dimension           = creation.dimension;
    property.globalRange         = 0;
    property.localRange          = 0;
    property.localCentroidLimit  = creation.numOfLocalClusters;
#ifdef NGTQ_QBG
    property.genuineDimension = creation.genuineDimension;
    property.genuineDataType  = creation.genuineDataType;
#endif
    property.dataType                         = creation.dataType;
    property.distanceType                     = creation.distanceType;
    property.singleLocalCodebook              = false;
    property.localDivisionNo                  = creation.numOfSubvectors;
    property.batchSize                        = creation.batchSize;
    property.centroidCreationMode             = creation.centroidCreationMode;
    property.localCentroidCreationMode        = creation.localCentroidCreationMode;
    property.localIDByteSize                  = creation.localIDByteSize;
    property.localClusteringSampleCoefficient = creation.localClusteringSampleCoefficient;
    property.localClusterDataType             = creation.localClusterDataType;
    property.scalarQuantizationClippingRate   = creation.scalarQuantizationClippingRate;
    property.scalarQuantizationNoOfSamples    = creation.scalarQuantizationNoOfSamples;
    property.refinementDataType               = creation.refinementDataType;
    property.orderedSearchRanking             = creation.orderedSearchRanking;
    globalProperty.edgeSizeForCreation        = creation.globalEdgeSizeForCreation;
    globalProperty.edgeSizeForSearch          = creation.globalEdgeSizeForSearch;
    globalProperty.indexType                  = creation.globalIndexType;
    globalProperty.insertionRadiusCoefficient = creation.globalInsertionRadiusCoefficient;
    globalProperty.graphType                  = creation.globalGraphType;
    globalProperty.objectType                 = creation.globalObjectType;
    globalProperty.seedSize                   = 0;
    localProperty.indexType                   = creation.localIndexType;
    localProperty.insertionRadiusCoefficient  = creation.localInsertionRadiusCoefficient;
    localProperty.graphType                   = creation.localGraphType;
    if (property.localCentroidLimit >= 0xFF) {
      if (property.localIDByteSize < 2) {
        property.localIDByteSize = 2;
      }
    } else if (property.localCentroidLimit >= 0xFFFF) {
      property.localIDByteSize = 4;
    }
    property.dimension       = property.dimension == 0 ? property.genuineDimension : property.dimension;
    property.localDivisionNo = property.localDivisionNo == 0 ? property.dimension : property.localDivisionNo;
  }

  size_t numOfObjects;
  size_t threadSize;
  size_t numOfLocalClusters;
  size_t dimension;
#ifdef NGTQ_QBG
  size_t genuineDimension;
  size_t dimensionOfSubvector;
  ObjectFile::DataType genuineDataType;
#endif
  NGTQ::DataType dataType;
  NGTQ::DistanceType distanceType;
  bool singleLocalCodebook;
  size_t numOfSubvectors;
  size_t batchSize;
  NGTQ::CentroidCreationMode centroidCreationMode;
  NGTQ::CentroidCreationMode localCentroidCreationMode;
  size_t localIDByteSize;
  size_t localClusteringSampleCoefficient;
  NGTQ::DataType refinementDataType;
  bool orderedSearchRanking;
  NGTQ::ClusterDataType localClusterDataType;
  float scalarQuantizationClippingRate;
  size_t scalarQuantizationNoOfSamples;
#ifdef NGTQ_QUANTIZED_TREE
  int64_t maxObjectsPerNode;
#endif

  size_t globalEdgeSizeForCreation;
  size_t globalEdgeSizeForSearch;
  NGT::Property::IndexType globalIndexType;
  float globalInsertionRadiusCoefficient;
  NGT::Property::GraphType globalGraphType;
  NGT::ObjectSpace::ObjectType globalObjectType;

  NGT::Property::IndexType localIndexType;
  float localInsertionRadiusCoefficient;
  NGT::Property::GraphType localGraphType;

  bool verbose;
};

class HierarchicalClusteringParameters {
 public:
  HierarchicalClusteringParameters() { setDefault(); }
  void setDefault() {
    maxSize                = 1000;
    numOfObjects           = 0;
    numOfClusters          = 2;
    numOfTotalClusters     = 0;
    numOfTotalBlobs        = 0;
    clusterID              = -1;
    initMode               = NGT::Clustering::InitializationModeKmeansPlusPlus;
    numOfRandomObjects     = 0;
    numOfFirstObjects      = 0;
    numOfFirstClusters     = 0;
    numOfSecondObjects     = 0;
    numOfSecondClusters    = 0;
    numOfThirdObjects      = 0;
    numOfThirdClusters     = 0;
    extractCentroid        = false;
    clusteringType         = QBG::HierarchicalKmeans::ClusteringTypeThreeLayer;
    epsilonExplorationSize = 1000;
    expectedRecall         = 0.98;

    verbose = false;
  }

  size_t maxSize;
  size_t numOfObjects;
  size_t numOfClusters;
  size_t numOfTotalClusters;
  size_t numOfTotalBlobs;
  int32_t clusterID;

  NGT::Clustering::InitializationMode initMode;

  size_t numOfRandomObjects;

  size_t numOfFirstObjects;
  size_t numOfFirstClusters;
  size_t numOfSecondObjects;
  size_t numOfSecondClusters;
  size_t numOfThirdObjects;
  size_t numOfThirdClusters;
  bool extractCentroid;

  QBG::HierarchicalKmeans::ClusteringType clusteringType;
  size_t epsilonExplorationSize;
  float expectedRecall;

  bool verbose;
};

class OptimizationParameters {
 public:
  OptimizationParameters() { setDefault(); }
  void setDefault() {
    clusteringType                   = NGT::Clustering::ClusteringTypeKmeansWithoutNGT;
    initMode                         = NGT::Clustering::InitializationModeHead;
    timelimit                        = 24 * 1 * 60.0 * 60.0;
    iteration                        = 1000;
    clusterIteration                 = 400;
    clusterSizeConstraint            = false;
    clusterSizeConstraintCoefficient = 10.0;
    convergenceLimitTimes            = 5;
    numOfObjects                     = 1000;
    numOfClusters                    = 0;
    numOfSubvectors                  = 0;
    numOfMatrices                    = 1;
    seedNumberOfSteps                = 2;
    seedStep                         = 10;
    reject                           = 0.9;
    repositioning                    = false;
    rotation                         = true;
    globalType                       = QBG::Optimizer::GlobalTypeNone;
    randomizedObjectExtraction       = true;
    showClusterInfo                  = false;
    unifiedPQ                        = false;

    verbose = false;
  }
  NGT::Clustering::ClusteringType clusteringType;
  NGT::Clustering::InitializationMode initMode;

  float timelimit;
  size_t iteration;
  size_t clusterIteration;
  bool clusterSizeConstraint;
  float clusterSizeConstraintCoefficient;
  size_t convergenceLimitTimes;
  size_t numOfObjects;
  size_t numOfClusters;
  size_t numOfSubvectors;
  size_t numOfMatrices;
  size_t seedNumberOfSteps;
  size_t seedStep;
  float reject;
  bool repositioning;
  bool rotation;
  QBG::Optimizer::GlobalType globalType;
  bool randomizedObjectExtraction;
  bool showClusterInfo;
  bool unifiedPQ;
  bool verbose;
};

class BuildParameters {
 public:
  BuildParameters() { setDefault(); }

  void setDefault() {
    creation.setDefault();
    hierarchicalClustering.setDefault();
    optimization.setDefault();
  }

  void setProperties(NGTQ::Property &property, NGT::Property &globalProperty, NGT::Property &localProperty) {
    CreationParameters::setProperties(creation, property, globalProperty, localProperty);
  }

  void setVerbose(bool s) {
    creation.verbose               = s;
    hierarchicalClustering.verbose = s;
    optimization.verbose           = s;
    verbose                        = s;
  }

  CreationParameters creation;
  HierarchicalClusteringParameters hierarchicalClustering;
  OptimizationParameters optimization;

  bool verbose;
};

class SearchContainer : public NGT::SearchContainer {
 public:
  SearchContainer(NGT::Object &q)
      : NGT::SearchContainer(q), cutback(0.0), graphExplorationSize(50), exactResultSize(0),
        blobExplorationCoefficient(1.0), numOfProbes(5), refinementExpansion(0.0) {}
  SearchContainer()
      : NGT::SearchContainer(*reinterpret_cast<NGT::Object *>(0)), cutback(0.0), graphExplorationSize(50),
        exactResultSize(0), blobExplorationCoefficient(1.0), numOfProbes(5), refinementExpansion(0.0) {}
  SearchContainer(SearchContainer &sc, NGT::Object &q) : NGT::SearchContainer(q) {
    QBG::SearchContainer::operator=(sc);
  }
  SearchContainer &operator=(SearchContainer &sc) {
    NGT::SearchContainer::operator=(sc);
    cutback                    = sc.cutback;
    graphExplorationSize       = sc.graphExplorationSize;
    exactResultSize            = sc.exactResultSize;
    blobExplorationCoefficient = sc.blobExplorationCoefficient;
    numOfProbes                = sc.numOfProbes;
    refinementExpansion        = sc.refinementExpansion;
    objectVector               = sc.objectVector;
    return *this;
  }
  void setCutback(float c) { cutback = c; }
  void setGraphExplorationSize(size_t size) { graphExplorationSize = size; }
  void setExactResultSize(size_t esize) { exactResultSize = esize; }
  void setBlobEpsilon(float c) { blobExplorationCoefficient = c + 1.0; }
  void setNumOfProbes(size_t p) { numOfProbes = p; }
  void setObjectVector(std::vector<float> &query) { objectVector = query; }
  void setRefinementExpansion(float re) { refinementExpansion = re; }
  float cutback;
  size_t graphExplorationSize;
  size_t exactResultSize;
  float blobExplorationCoefficient;
  size_t numOfProbes;
  float refinementExpansion;
  std::vector<float> objectVector;
};

class BatchSearchContainer : public SearchContainer {
 public:
  BatchSearchContainer(NGT::Object &q) : SearchContainer(q), objectVectors(0), numOfQueries(0) {}
  BatchSearchContainer() : objectVectors(0), numOfQueries(0) {}
  BatchSearchContainer(SearchContainer &sc, NGT::Object &q)
      : SearchContainer(sc, q), objectVectors(0), numOfQueries(0) {}

  void setObjectVectors(void *qs, size_t nq, size_t dim) {
    objectVectors = reinterpret_cast<float *>(qs);
    numOfQueries  = nq;
    dimension     = dim;
  }
  void *getQuery(size_t idx) { return objectVectors + dimension * idx; }
  NGT::ObjectDistances &getBatchResult(size_t i) { return batchResult[i]; }
  std::vector<NGT::ObjectDistances> &getBatchResult() { return batchResult; }

  float *objectVectors;
  size_t numOfQueries;
  size_t dimension;
  std::vector<NGT::ObjectDistances> batchResult;
};

class QuantizedBlobGraphRepository : public NGTQG::QuantizedGraphRepository {
 public:
  QuantizedBlobGraphRepository(NGTQ::Index &quantizedIndex)
      : NGTQG::QuantizedGraphRepository(quantizedIndex) {}

  void construct(NGTQ::Index &quantizedIndex) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    std::cerr << "construct: Not implemented" << std::endl;
    abort();
#else

    (*this).resize(quantizedIndex.getInvertedIndexSize());
    NGT::Timer timer;
    timer.start();
    for (size_t gid = 1; gid < quantizedIndex.getInvertedIndexSize(); gid++) {
      if (gid % 10000 == 0) {
        timer.stop();
        std::cerr << "The number of processed blobs=" << gid
                  << " VmSize=" << NGT::Common::getProcessVmSizeStr() << " Elapsed time=" << timer
                  << std::endl;
        timer.restart();
      }
      NGTQ::InvertedIndexEntry<uint16_t> invertedIndexObjects(numOfSubspaces);
      quantizedIndex.getQuantizer().extractInvertedIndexObject(invertedIndexObjects, gid);
      quantizedIndex.getQuantizer().eraseInvertedIndexObject(gid);
      if (invertedIndexObjects.size() == at(gid).nOfObjects) {
        size_t idx = 0;
        for (; idx < invertedIndexObjects.size(); idx++) {
          if (invertedIndexObjects[idx].id != at(gid).ids[idx]) {
            break;
          }
        }
        if (idx == invertedIndexObjects.size()) {
          continue;
        }
      }
      rearrange(invertedIndexObjects, (*this)[gid], quantizedIndex.getQuantizer());
    }
#endif
  }

  static void rearrangeObjects(NGTQ::InvertedIndexEntry<uint16_t> &invertedIndexObjects,
                               NGTQG::QuantizedNode &rearrangedObjects, NGTQ::Quantizer &quantizer,
                               std::vector<uint32_t> &ids) {
    rearrangedObjects.subspaceID  = invertedIndexObjects.subspaceID;
    auto &quantizedObjectDistance = quantizer.getQuantizedObjectDistance();
    size_t streamSize;
    size_t idsSize   = sizeof(uint32_t) * ids.size();
    uint8_t *objects = quantizedObjectDistance.generateRearrangedObjects(invertedIndexObjects, &streamSize);
    rearrangedObjects.objects = new uint8_t[streamSize + idsSize];
    rearrangedObjects.ids =
        reinterpret_cast<uint32_t *>(static_cast<uint8_t *>(rearrangedObjects.objects) + streamSize);
    memcpy(rearrangedObjects.objects, objects, streamSize);
    delete[] objects;
    memcpy(rearrangedObjects.ids, ids.data(), sizeof(uint32_t) * ids.size());
    rearrangedObjects.nOfObjects = ids.size();
  }

  static void rearrangeObjects(NGTQ::InvertedIndexEntry<uint16_t> &invertedIndexObjects,
                               NGTQG::QuantizedNode &rearrangedObjects) {
    NGTQ::QuantizedObjectProcessingStream quantizedStream(invertedIndexObjects.numOfSubvectors,
                                                          invertedIndexObjects.size());
    quantizedStream.arrange(invertedIndexObjects);
    rearrangedObjects.subspaceID = invertedIndexObjects.subspaceID;
    rearrangedObjects.objects    = quantizedStream.compressIntoUint4();
  }

  static void rearrange(NGTQ::InvertedIndexEntry<uint16_t> &invertedIndexObjects,
                        NGTQG::QuantizedNode &rearrangedObjects, NGTQ::Quantizer &quantizer) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    std::cerr << "construct: Not implemented" << std::endl;
    abort();
#else
    if (invertedIndexObjects.numOfSubvectors == 0) {
      NGTThrowException("# of subvectors is zero.");
    }
    NGT::Timer timer;
    timer.start();
    {
      rearrangedObjects.clear();
      std::vector<uint32_t> ids;
      ids.reserve(invertedIndexObjects.size());
      for (size_t oidx = 0; oidx < invertedIndexObjects.size(); oidx++) {
        ids.emplace_back(invertedIndexObjects[oidx].id);
      }
      rearrangeObjects(invertedIndexObjects, rearrangedObjects, quantizer, ids);
    }
#endif
  }

  static void rearrange(NGTQ::QuantizedObjectSet &quantizedObjects, NGTQG::QuantizedNode &rearrangedObjects) {
    NGTQ::InvertedIndexEntry<uint16_t> iie;
    iie.set(quantizedObjects);
    rearrange(iie, rearrangedObjects, *reinterpret_cast<NGTQ::Quantizer *>(0));
  }

  void extractRemovedIdSet(size_t objectListSize, std::vector<uint32_t> &removedIDs) {
    std::vector<bool> exist(objectListSize);
    size_t count           = 0;
    size_t duplicatedCount = 0;
    for (auto &blob : *this) {
      for (size_t i = 0; i < blob.nOfObjects; i++) {
        auto id = blob.ids[i];
        if (id >= exist.size()) {
          stringstream msg;
          msg << "ID in the blob is invalid. " << id << ":" << objectListSize;
          NGTThrowException(msg);
        }
        if (exist.at(id)) {
          if (duplicatedCount == 0) {
            std::cerr << "Warning: the object is duplicated. " << id << std::endl;
          }
          duplicatedCount++;
        } else {
          count++;
          exist.at(id) = true;
        }
      }
    }
    if (duplicatedCount > 0) {
      std::cerr << "Warning: # of duplicated objects is " << duplicatedCount << "." << std::endl;
    }
    {
      removedIDs.clear();
      removedIDs.reserve(objectListSize - count);
      if (objectListSize > 1) {
        for (uint32_t id = objectListSize - 1; id > 0; id--) {
          if (!exist[id]) {
            removedIDs.push_back(id);
          }
        }
      }
      std::sort(removedIDs.rbegin(), removedIDs.rend());
    }
  }
};

class Index : public NGTQ::Index {
 public:
  Index(const std::string &indexPath, bool prebuilt = false, bool verbose = false,
        NGTQ::DataType refinementDataType = NGTQ::DataTypeAny)
      : NGTQ::Index(indexPath, prebuilt, refinementDataType), path(indexPath), quantizedBlobGraph(*this) {
    searchable = false;
    NGT::StdOstreamRedirector redirector(!verbose);
    redirector.begin();
    try {
      load();
      searchable = true;
    } catch (NGT::Exception &err) {
      if (prebuilt) {
        stringstream msg;
        msg << "QBG::Index: No quantized blob graph. " << err.what();
        NGTThrowException(msg);
      } else {
      }
    }
    redirector.end();
  }

  ~Index() {}

  bool &getVerbose() { return verbose; }

#ifdef NGTQ_QBG
  static void create(const std::string &index, BuildParameters &buildParameters,
                     std::vector<float> *rotation = 0, const std::string objectFile = "") {
    create(index, buildParameters.creation, rotation, objectFile);
  }
  static void create(const std::string &index, CreationParameters &creation, std::vector<float> *rotation = 0,
                     const std::string objectFile = "") {
    NGTQ::Property property;
    NGT::Property globalProperty;
    NGT::Property localProperty;
    CreationParameters::setProperties(creation, property, globalProperty, localProperty);
    property.quantizerType = NGTQ::QuantizerTypeQBG;
    property.graphType     = NGTQ::GraphTypeBlobGraph;
    NGTQ::Index::create(index, property, globalProperty, localProperty, rotation, objectFile);
  }
#endif
#ifdef NGTQ_QBG
  static void initialize(NGTQ::Property &property, NGT::Property &globalProperty,
                         NGT::Property &localProperty) {
    QBG::CreationParameters params;
    QBG::CreationParameters::setProperties(params, property, globalProperty, localProperty);
  }
#endif

  static void create(const std::string &index, NGTQ::Property &property, NGT::Property &globalProperty,
#ifdef NGTQ_QBG
                     NGT::Property &localProperty, std::vector<float> *rotation,
                     const std::string &objectFile) {
#else
                     NGT::Property &localProperty) {
#endif
    property.quantizerType = NGTQ::QuantizerTypeQBG;
#ifdef NGTQ_QBG
    NGTQ::Index::create(index, property, globalProperty, localProperty, rotation, objectFile);
#else
    NGTQ::Index::create(index, property, globalProperty, localProperty);
#endif
  }

  static void load(const std::string &indexPath, const std::vector<std::vector<float>> &quantizerCodebook,
                   const std::vector<float> &rotation) {
    NGTQ::Index index(indexPath);
    index.getQuantizer().loadQuantizationCodebookAndRotation(quantizerCodebook, rotation);
  }

  void insert(const size_t id, std::vector<float> &object) {
    getQuantizer().objectList.put(id, object, &getQuantizer().globalCodebookIndex.getObjectSpace());
  }

  template <typename T> NGT::ObjectID append(std::vector<T> &object) {
    NGT::ObjectID id = getQuantizer().objectList.size();
    id               = id == 0 ? 1 : id;
    if (typeid(T) == typeid(float)) {
      auto &obj = *reinterpret_cast<std::vector<float> *>(&object);
      getQuantizer().objectList.put(id, obj, &getQuantizer().globalCodebookIndex.getObjectSpace());
    } else {
      std::vector<float> obj(object.begin(), object.end());
      getQuantizer().objectList.put(id, obj, &getQuantizer().globalCodebookIndex.getObjectSpace());
    }
    return id;
  }

  static void append(const std::string &indexName, // index file
                     const std::string &data,      // data file
                     size_t dataSize = 0,          // data size
                     bool verbose    = false);

  static void append(const std::string &indexName,  // index file
                     NGT::ObjectSpace &objectSpace, // object space including objects
                     bool verbose = false);

  static void preprocessingForNGT(std::string &indexPath, std::string &objectPath, bool verbose = false);

  static void appendBinary(const std::string &indexName, // index file
                           const std::string &data,      // data file
                           size_t dataSize = 0,          // data size
                           bool verbose    = false);

  void remove(NGT::ObjectID id) {
    std::vector<uint32_t> ids;
    ids.emplace_back(id);
    remove(ids);
  }

  void remove(std::vector<NGT::ObjectID> &ids) {
    auto &quantizer = getQuantizer();
    auto &gcodebook = static_cast<NGT::GraphAndTreeIndex &>(quantizer.globalCodebookIndex.getIndex());
    for (auto id : ids) {
      if (id >= quantizer.objectList.size()) {
        std::stringstream msg;
        msg << "remove: the specified object does not exist. " << id;
        NGTThrowException(msg);
      }
      auto pi = std::lower_bound(removedIDs.rbegin(), removedIDs.rend(), id);
      if (pi != removedIDs.rend() || *pi == id) {
        std::stringstream msg;
        msg << "remove: the specified object is already removed. " << id;
        NGTThrowException(msg);
      }
    }
    vector<pair<std::vector<float>, size_t>> objects;
    objects.reserve(ids.size());

    for (size_t idx = 0; idx < ids.size(); idx++) {
      auto id = ids[idx];
      std::vector<float> object;
      quantizer.objectList.get(id, object, &gcodebook.getObjectSpace());
      objects.push_back(pair<std::vector<float>, size_t>(object, id));
    }
    vector<NGT::Index::InsertionResult> gids;
    NGTQ::Quantizer::searchIndex(gcodebook, objects, gids);

    for (size_t bidx = 0; bidx < gids.size(); bidx++) {
      auto blobID             = gids[bidx].id;
      auto &rearrangedObjects = quantizedBlobGraph[blobID];
      size_t rmidx            = 0;
      for (; rmidx < rearrangedObjects.nOfObjects; rmidx++) {
        if (rearrangedObjects.ids[rmidx] == ids[bidx]) {
          break;
        }
      }
      if (rmidx == rearrangedObjects.nOfObjects) {
        std::stringstream msg;
        msg << "remove: Not found the specified ID. " << ids[bidx];
        NGTThrowException(msg);
      }
      NGTQ::InvertedIndexEntry<uint16_t> invertedIndexObjects;
      quantizer.getQuantizedObjectDistance().restoreIntoInvertedIndex(
          invertedIndexObjects, quantizedBlobGraph.numOfSubspaces, rearrangedObjects.nOfObjects,
          rearrangedObjects.ids, rearrangedObjects.objects);
      invertedIndexObjects.erase(invertedIndexObjects.begin() + rmidx);

      std::vector<uint32_t> ids(rearrangedObjects.ids, rearrangedObjects.ids + rearrangedObjects.nOfObjects);
      ids.erase(ids.begin() + rmidx);
      rearrangedObjects.clear();
      size_t streamSize;
      size_t idsSize = sizeof(uint32_t) * ids.size();
      uint8_t *objects =
          quantizer.getQuantizedObjectDistance().generateRearrangedObjects(invertedIndexObjects, &streamSize);
      rearrangedObjects.objects = new uint8_t[streamSize + idsSize];
      rearrangedObjects.ids =
          reinterpret_cast<uint32_t *>(static_cast<uint8_t *>(rearrangedObjects.objects) + streamSize);
      memcpy(rearrangedObjects.objects, objects, streamSize);
      delete[] objects;
      memcpy(rearrangedObjects.ids, ids.data(), sizeof(uint32_t) * ids.size());
      rearrangedObjects.nOfObjects = ids.size();
    }
  }

  void insertObjectsToBlob(NGT::ObjectID blobID,
                           std::vector<std::pair<std::vector<float>, size_t>> &objects) {
    auto &quantizer         = getQuantizer();
    auto &rearrangedObjects = quantizedBlobGraph[blobID];
    auto subspaceID         = quantizedBlobGraph[blobID].subspaceID;
    NGTQ::InvertedIndexEntry<uint16_t> invertedIndexObjects;
    quantizer.getQuantizedObjectDistance().restoreIntoInvertedIndex(
        invertedIndexObjects, quantizedBlobGraph.numOfSubspaces, rearrangedObjects.nOfObjects,
        rearrangedObjects.ids, rearrangedObjects.objects);
    std::vector<uint32_t> idsback(rearrangedObjects.ids,
                                  rearrangedObjects.ids + rearrangedObjects.nOfObjects);
    for (auto &b : objects) {
      auto &object = b.first;
      auto id      = b.second;
      NGTQ::Object tobject(object, id, subspaceID);
      NGTQ::QuantizedObject quantizedObject;
      quantizer.encode(subspaceID, tobject, quantizedObject);
      invertedIndexObjects.pushBack(id, quantizedObject);
      idsback.emplace_back(id);
    }
    rearrangedObjects.clear();
    size_t streamSize;
    size_t idsSize = sizeof(uint32_t) * idsback.size();
    uint8_t *objs =
        quantizer.getQuantizedObjectDistance().generateRearrangedObjects(invertedIndexObjects, &streamSize);
    rearrangedObjects.objects = new uint8_t[streamSize + idsSize];
    rearrangedObjects.ids =
        reinterpret_cast<uint32_t *>(static_cast<uint8_t *>(rearrangedObjects.objects) + streamSize);
    memcpy(rearrangedObjects.objects, objs, streamSize);
    delete[] objs;
    memcpy(rearrangedObjects.ids, idsback.data(), sizeof(uint32_t) * idsback.size());
    rearrangedObjects.nOfObjects = idsback.size();
  }

  template <typename T> NGT::ObjectID insert(std::vector<T> &object) {
    std::vector<std::vector<T>> objects;
    std::vector<NGT::ObjectID> ids;
    objects.emplace_back(object);
    insert(objects, ids);
    if (ids.size() != 1) {
      std::stringstream msg;
      msg << "Fatal inner error. Cannot set the ID. size=" << ids.size();
      NGTThrowException(msg);
    }
    return ids[0];
  }

  template <typename T> void insert(std::vector<std::vector<T>> &objects, std::vector<NGT::ObjectID> &ids) {
    if (!searchable) {
      std::stringstream msg;
      msg << "The specified index is NOT completely built yet. Insert is available for a built index.";
      NGTThrowException(msg);
    }

    auto &quantizer = getQuantizer();
    if (quantizer.objectList.size() == 0) {
      std::stringstream msg;
      msg << "The specified index is empty. Insert is available for a built index.";
      NGTThrowException(msg);
    }

    std::vector<uint32_t> rmids;
    std::vector<std::pair<std::vector<float>, size_t>> floatObjects;
    for (auto &obj : objects) {
      uint32_t id = quantizer.objectList.size();
      if (!removedIDs.empty()) {
        auto removedID = removedIDs.back();
        if (removedID == 0 || removedID >= id) {
          std::stringstream msg;
          msg << "Fatal inner error. The removed ID is invalid. " << removedID;
          NGTThrowException(msg);
        }
        id = removedID;
        removedIDs.pop_back();
        rmids.push_back(id);
      }
      ids.push_back(id);
      if ((quantizer.property.distanceType == NGTQ::DistanceType::DistanceTypeInnerProduct) &&
          (obj.size() + 1 == quantizer.objectList.genuineDimension)) {
        obj.emplace_back(0);
      }
      if (obj.size() != quantizer.property.genuineDimension) {
        ids.clear();
        std::stringstream msg;
        msg << "The specified vector size is invalid. " << obj.size() << ":"
            << quantizer.objectList.genuineDimension;
        NGTThrowException(msg);
      }
      if (typeid(T) == typeid(float)) {
        floatObjects.emplace_back(std::make_pair(obj, id));
      } else {
        std::vector<float> ftmpobj;
        ftmpobj.insert(ftmpobj.begin(), obj.begin(), obj.end());
        floatObjects.emplace_back(std::make_pair(ftmpobj, id));
      }
      auto &os = quantizer.globalCodebookIndex.getObjectSpace();
      quantizer.objectList.put(id, floatObjects.back().first, &os);
      if (floatObjects.back().first.size() != os.getPaddedDimension()) {
        floatObjects.back().first.resize(os.getPaddedDimension(), 0);
      }
    }
    auto &gcodebook = static_cast<NGT::GraphAndTreeIndex &>(quantizer.globalCodebookIndex.getIndex());
    vector<NGT::Index::InsertionResult> gids;
    NGTQ::Quantizer::searchIndex(gcodebook, floatObjects, gids);

    if (gids.size() != floatObjects.size()) {
      ids.clear();
      NGTThrowException("Fatal inner error. Something wrong.");
    }
    std::unordered_map<uint32_t, std::vector<std::pair<std::vector<float>, size_t>>> batchObjects;
    for (size_t idx = 0; idx < floatObjects.size(); idx++) {
      auto gid = gids[idx].id;
      auto i   = batchObjects.find(gid);
      if (i == batchObjects.end()) {
        std::vector<pair<std::vector<float>, size_t>> value;
        value.emplace_back(floatObjects[idx]);
        batchObjects.insert(make_pair(gid, value));
      } else {
        (*i).second.emplace_back(floatObjects[idx]);
      }
    }
    std::vector<std::unordered_map<uint32_t, vector<pair<std::vector<float>, size_t>>>::iterator>
        vbatchObjects;
    for (auto it = batchObjects.begin(); it != batchObjects.end(); ++it) {
      vbatchObjects.emplace_back(it);
    }
#pragma omp parallel for
    for (size_t idx = 0; idx < vbatchObjects.size(); idx++) {
      auto &it    = vbatchObjects[idx];
      auto blobID = (*it).first;
      insertObjectsToBlob(blobID, (*it).second);
    }
    return;
  }

  float getApproximateDistances(std::vector<float> &query,
                                NGTQG::RearrangedQuantizedObjectSet &quantizedObjects, size_t subspaceID,
                                std::vector<float> &distances) {
    if (query.empty()) {
      NGTThrowException("The specified query is empty.");
    }
    auto &quantizer = this->getQuantizer();
    if (quantizer.getNumOfLocalClusters() != 16) {
      std::stringstream msg;
      msg << "# of the local clusters is not 16. " << quantizer.getNumOfLocalClusters();
      NGTThrowException(msg);
    }
    distances.clear();
    auto noOfObjects = quantizedObjects.nOfObjects;
    if (noOfObjects == 0) {
      return 0.0;
    }
    auto rotatedQuery             = query;
    auto &quantizedObjectDistance = quantizer.getQuantizedObjectDistance();
    quantizedObjectDistance.rotation->mul(rotatedQuery.data());
    NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 lookupTable;
    quantizedObjectDistance.initialize(lookupTable);
    quantizedObjectDistance.createDistanceLookup(rotatedQuery.data(), subspaceID, lookupTable);
    distances.resize(NGTQ::QuantizedObjectProcessingStream::getNumOfAlignedObjects(noOfObjects));
    auto minDistance =
        quantizedObjectDistance(quantizedObjects.objects, distances.data(), noOfObjects, lookupTable);
    distances.resize(noOfObjects);
    return minDistance;
  }

  void getApproximateDistances(std::vector<float> &query, NGTQ::QuantizedObjectSet &quantizedObjects,
                               size_t subspaceID, std::vector<float> &distances) {
    if (query.empty()) {
      NGTThrowException("The specified query is empty.");
    }
    auto &quantizer = this->getQuantizer();
    distances.clear();
    auto noOfObjects = quantizedObjects.size();
    if (noOfObjects == 0) {
      return;
    }
    auto rotatedQuery             = query;
    auto &quantizedObjectDistance = quantizer.getQuantizedObjectDistance();
    quantizedObjectDistance.rotation->mul(rotatedQuery.data());
    NGTQ::QuantizedObjectDistance::DistanceLookupTable lookupTable;
    quantizedObjectDistance.initialize(lookupTable);
    quantizedObjectDistance.createDistanceLookup(rotatedQuery.data(), subspaceID, lookupTable);
    distances.resize(noOfObjects);
    if (quantizer.localIDByteSize == 1) {
      NGTQ::InvertedIndexEntry<uint8_t> iie;
      iie.set(quantizedObjects);
      for (size_t idx = 0; idx < iie.size(); idx++) {
        distances[idx] = quantizedObjectDistance(&iie[idx].localID[0], lookupTable);
      }
    } else if (quantizer.localIDByteSize == 2) {
      NGTQ::InvertedIndexEntry<uint16_t> iie;
      iie.set(quantizedObjects);
      for (size_t idx = 0; idx < iie.size(); idx++) {
        distances[idx] = quantizedObjectDistance(&iie[idx].localID[0], lookupTable);
      }
    } else if (quantizer.localIDByteSize == 4) {
      NGTQ::InvertedIndexEntry<uint32_t> iie;
      iie.set(quantizedObjects);
      for (size_t idx = 0; idx < iie.size(); idx++) {
        distances[idx] = quantizedObjectDistance(&iie[idx].localID[0], lookupTable);
      }
    }
  }

  static void appendFromObjectRepository(const std::string &ngtIndex, // QG
                                         const std::string &qgIndex,  // NGT
                                         bool verbose = false) {
    NGT::StdOstreamRedirector redirector(!verbose);
    redirector.begin();

    NGT::Index ngt(ngtIndex);
    QBG::Index qg(qgIndex);
    auto &objectSpace = ngt.getObjectSpace();
    size_t size       = objectSpace.getRepository().size();
    for (size_t id = 1; id < size; ++id) {
      std::vector<float> object;
      try {
        objectSpace.getObject(id, object);
      } catch (...) {
        std::cerr << "append: Info: removed object. " << id << std::endl;
      }
      qg.insert(id, object);
    }
    cerr << "end of insertion." << endl;
    qg.save();
    qg.close();
    redirector.end();
  }

  static void expandBlob(std::string qbgIndexPath, std::string clusterCentroidsPath,
                         NGT::SearchContainer &ngtSearchContainer, QBG::SearchContainer &qbgSearchContainer,
                         float rate, NGTQ::DataType refinementDataType, bool verbose = false) {

    auto extractNeighbors = [](std::vector<std::vector<float>> &objects, std::vector<uint32_t> &sizes,
                               QBG::Index &qbg, size_t &gidx, NGT::SearchContainer &searchContainer,
                               QBG::SearchContainer qbgSearchContainer,
                               std::vector<std::vector<NGT::ObjectID>> &nearestNeighbors) {
      NGT::Index &gcodebook = qbg.getQuantizer().globalCodebookIndex;
#pragma omp parallel for
      for (size_t oidx = 0; oidx < objects.size(); oidx++) {
        //-/std::cerr << "oidx=" << oidx << std::endl;
        auto gtarget = gidx + oidx;
        {
          NGT::SearchQuery sq(objects[oidx]);
          if (gtarget >= gcodebook.getObjectRepositorySize()) {
            std::stringstream msg;
            msg << "Cluster centroids file has more entries than global codebook. " << gtarget << ":"
                << gcodebook.getObjectRepositorySize();
            NGTThrowException(msg);
          }
          static_cast<NGT::SearchContainer &>(sq) = searchContainer;
          NGT::ObjectDistances neighbors;
          sq.setResults(&neighbors);
          gcodebook.search(sq);
          if (gtarget + 1 != neighbors[0].id) {
            std::cerr << "extpandClusters: Warning! " << gtarget << ":" << neighbors[0].id << std::endl;
            auto found = false;
            for (size_t i = 1; i < neighbors.size(); i++) {
              std::cerr << neighbors[i].id << ":" << neighbors[i].distance << std::endl;
              if (gtarget + 1 == neighbors[i].id) {
                found = true;
                std::cerr << "Found" << std::endl;
                break;
              }
            }
            if (!found) {
              std::cerr << "extpandClusters: Strong warning! " << gtarget << std::endl;
            }
            neighbors[0].id = gtarget + 1;
          }
        }
        {
          NGT::ObjectDistances neighbors;
          QBG::SearchContainer sc(qbgSearchContainer);
          sc.setObjectVector(objects[oidx]);
          sc.setSize(sizes[oidx]);
          sc.setResults(&neighbors);
          qbg.searchInTwoSteps(sc);
          for (auto &n : neighbors) {
            nearestNeighbors[gtarget].emplace_back(n.id);
          }
        }
      }
      gidx += objects.size();
      objects.clear();
      sizes.clear();
    };

    NGT::StdOstreamRedirector redirector(!verbose);
    redirector.begin();

    auto prebuilt = false;
    QBG::Index qbg(qbgIndexPath, prebuilt, verbose, refinementDataType);
    if (clusterCentroidsPath.empty()) {
      clusterCentroidsPath = QBG::Index::getStoredBlobFile(qbgIndexPath);
    }
    std::ifstream stream(clusterCentroidsPath);
    if (!stream) {
      std::stringstream msg;
      msg << "Cannot open the centroid list file. " << clusterCentroidsPath;
      NGTThrowException(msg);
    }
    auto &quantizer = qbg.getQuantizer();
    auto &gcodebook = quantizer.globalCodebookIndex;
    std::string line;
    if (gcodebook.getObjectRepositorySize() == 0) {
      NGTThrowException("Global codebook index is empty.");
    }

    if (verbose) {
      std::cerr << "qbg search container size=" << qbgSearchContainer.size << std::endl;
      std::cerr << "repo size=" << gcodebook.getObjectRepositorySize() << std::endl;
    }

    std::vector<std::vector<NGT::ObjectID>> nearestNeighbors(gcodebook.getObjectRepositorySize() - 1);
    std::vector<std::vector<float>> objects;
    std::vector<uint32_t> sizes;
    size_t gidx = 0;
    while (getline(stream, line)) {
      std::vector<std::string> tokens;
      NGT::Common::tokenize(line, tokens, " \t");
      std::vector<float> object;
      for (auto &token : tokens) {
        object.emplace_back(NGT::Common::strtof(token));
      }
      objects.emplace_back(object);
      size_t rsize = qbg.quantizedBlobGraph[gidx + 1].nOfObjects;
      if (rate <= 0.0) {
        rsize += qbgSearchContainer.size;
      } else {
        rsize *= 1.0 + rate;
      }
      rsize = ((rsize + 15) / 16) * 16;
      sizes.emplace_back(rsize);
      if (objects.size() == 10) {
        extractNeighbors(objects, sizes, qbg, gidx, ngtSearchContainer, qbgSearchContainer, nearestNeighbors);
      }
    }
    if (objects.size() > 0) {
      extractNeighbors(objects, sizes, qbg, gidx, ngtSearchContainer, qbgSearchContainer, nearestNeighbors);
    }
    size_t nOfAddedObjects = 0;
    for (size_t gidx = 0; gidx < nearestNeighbors.size(); gidx++) {
      NGT::ObjectID blobID    = gidx + 1;
      auto &rearrangedObjects = qbg.quantizedBlobGraph[blobID];
      auto &ids               = rearrangedObjects.ids;
      std::unordered_set<NGT::ObjectID> blob(ids, ids + rearrangedObjects.nOfObjects);
      std::vector<std::pair<std::vector<float>, size_t>> objects;
      size_t rsize = qbg.quantizedBlobGraph[gidx + 1].nOfObjects;
      if (rate <= 0.0) {
        rsize += qbgSearchContainer.size;
      } else {
        rsize *= 1.0 + rate;
      }
      rsize = ((rsize + 15) / 16) * 16;
      rsize -= qbg.quantizedBlobGraph[gidx + 1].nOfObjects;
      for (auto &id : nearestNeighbors[gidx]) {
        if (objects.size() == rsize) break;
        if (blob.find(id) == blob.end()) {
          std::vector<float> object;
          qbg.getQuantizer().objectList.get(id, object);
          objects.emplace_back(std::make_pair(object, id));
        }
      }
      nOfAddedObjects += objects.size();
      qbg.insertObjectsToBlob(blobID, objects);
    }
    if (verbose) {
      std::cerr << "# of added objects=" << nOfAddedObjects
                << " the mean # of added objects=" << nOfAddedObjects / nearestNeighbors.size() << std::endl;
    }
    qbg.save();
    redirector.end();
  }

  void getSeeds(NGT::Index &index, NGT::Object *object, NGT::ObjectDistances &seeds, size_t noOfSeeds) {
    auto &graph = static_cast<NGT::GraphAndTreeIndex &>(index.getIndex());
    NGT::SearchContainer sc(*object);
    sc.setResults(&seeds);
    sc.setSize(noOfSeeds);
    sc.setEpsilon(0.0);
    sc.setEdgeSize(-2);
    graph.search(sc);
  }

  NGT::Distance getDistance(void *objects, std::vector<float> &distances, size_t noOfObjects,
                            NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 &lut) {
    auto &quantizedObjectDistance = getQuantizer().getQuantizedObjectDistance();
#ifdef NGTQBG_MIN
    auto min = quantizedObjectDistance(objects, distances.data(), noOfObjects, lut);
#else
    quantizedObjectDistance(objects, distances.data(), noOfObjects, lut);
#endif
#ifdef NGTQBG_MIN
    return min;
#endif
  }

  template <typename T>
  std::tuple<NGT::Distance, NGT::Distance> judge(NGTQG::QuantizedNode &ivi, size_t k, NGT::Distance radius,
                                                 NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 &lut,
                                                 T &result, size_t &foundCount, void *query = 0,
                                                 std::unique_ptr<NGTQ::BooleanSet> *checkedIDs = 0) {
    auto noOfObjects              = ivi.nOfObjects;
    auto &quantizedObjectDistance = getQuantizer().getQuantizedObjectDistance();
    std::vector<float> distances(quantizedObjectDistance.getNumOfAlignedObjects(noOfObjects));
    if (checkedIDs != 0) {
      for (size_t idx = 0; idx < ivi.nOfObjects; idx++) {
        auto id = ivi.ids[idx];
        if ((**checkedIDs)[id]) {
          distances[idx] = 1.0;
        } else {
          (**checkedIDs).set(id);
        }
      }
    }
#ifdef NGTQBG_MIN
    float distance = quantizedObjectDistance(ivi.objects, &distances[0], noOfObjects, lut, query);
#else
    quantizedObjectDistance(ivi.objects, &distances[0], noOfObjects, lut, query);
#endif
#ifdef NGTQBG_MIN
    if (distance >= radius) {
      return std::make_pair(distance, radius);
    }
#endif
    bool found = false;
    for (size_t i = 0; i < noOfObjects; i++) {
      if (distances[i] <= radius) {
        result.push(NGT::ObjectDistance(ivi.ids[i], distances[i]));
        found = true;
        if (result.size() > k) {
          result.pop();
        }
      }
    }
    if (result.size() >= k) {
      radius = result.top().distance;
    }
    if (found) foundCount++;
#ifdef NGTQBG_MIN
    return std::make_pair(distance, radius);
#else
    return std::make_pair(0.0, radius);
#endif
  }

  static float refineDistances(NGTQ::Quantizer &quantizer, NGT::NeighborhoodGraph::ResultSet &result,
                               NGT::ObjectDistances &qresults, size_t exactResultSize,
                               std::unique_ptr<std::vector<float>> &resizedQuery) {
    float err;
    NGT::ObjectSpace *objectSpace;
    if (quantizer.refinementObjectSpace != 0) {
      objectSpace = quantizer.refinementObjectSpace;
    } else if (quantizer.refinementObjectSpaceForObjectList != 0) {
      objectSpace = quantizer.refinementObjectSpaceForObjectList;
    } else {
      std::stringstream msg;
      msg << "Fatal inner error! Any refinement object space is unavailable.";
      NGTThrowException(msg);
    }
    NGT::ResultPriorityQueue qres;
    if (objectSpace->getObjectType() == typeid(float)) {
      err = refineDistances<float>(quantizer, result, qres, exactResultSize, resizedQuery);
    } else if (objectSpace->getObjectType() == typeid(uint8_t)) {
      err = refineDistances<uint8_t>(quantizer, result, qres, exactResultSize, resizedQuery);
    } else if (objectSpace->getObjectType() == typeid(NGT::float16)) {
      err = refineDistances<NGT::float16>(quantizer, result, qres, exactResultSize, resizedQuery);
    } else {
      std::stringstream msg;
      msg << "refineDistances: Fatal error! Invalid datatype. " << objectSpace->getObjectType().name()
          << std::endl;
      NGTThrowException(msg);
    }
    qresults.resize(qres.size());
    for (int i = qresults.size() - 1; i >= 0; i--) {
      qresults[i] = qres.top();
      qres.pop();
    }
    return err;
  }

  static float refineDistances(NGTQ::Quantizer &quantizer, NGT::NeighborhoodGraph::ResultSet &result,
                               NGT::ResultPriorityQueue &qresults, size_t exactResultSize,
                               std::unique_ptr<std::vector<float>> &resizedQuery) {
    auto &objectSpace = *quantizer.refinementObjectSpace;
    if (objectSpace.getObjectType() == typeid(float)) {
      return refineDistances<float>(quantizer, result, qresults, exactResultSize, resizedQuery);
    } else if (objectSpace.getObjectType() == typeid(uint8_t)) {
      return refineDistances<uint8_t>(quantizer, result, qresults, exactResultSize, resizedQuery);
    } else if (objectSpace.getObjectType() == typeid(NGT::float16)) {
      return refineDistances<NGT::float16>(quantizer, result, qresults, exactResultSize, resizedQuery);
    } else {
      std::stringstream msg;
      msg << "refineDistances: Fatal error! Invalid datatype. " << objectSpace.getObjectType().name()
          << std::endl;
      NGTThrowException(msg);
    }
  }

  template <typename T>
  static float refineDistances(NGTQ::Quantizer &quantizer, NGT::NeighborhoodGraph::ResultSet &result,
                               NGT::ResultPriorityQueue &qresults, size_t exactResultSize,
                               std::unique_ptr<std::vector<float>> &resizedQuery) {
    qresults = NGT::ResultPriorityQueue();
#ifdef NGTQ_OBJECT_IN_MEMORY
    if (quantizer.refinementObjectSpace != 0) {
      auto &os         = *quantizer.refinementObjectSpace;
      auto &repo       = os.getRepository();
      auto &comparator = os.getComparator();
      auto *q          = os.allocateNormalizedObject(*resizedQuery);
      while (!result.empty()) {
        auto r = result.top();
        result.pop();
        {
          r.distance = comparator(*q, *repo.get(r.id));
          qresults.push(r);
        }
      }
      os.deleteObject(q);
    } else if (quantizer.refinementObjectSpaceForObjectList != 0) {
#endif
      auto threadid    = omp_get_thread_num();
      auto &os         = *quantizer.refinementObjectSpaceForObjectList;
      auto &comparator = os.getComparator();
      auto *q          = os.allocateNormalizedObject(*resizedQuery);
      while (!result.empty()) {
        auto r = result.top();
        result.pop();
        std::vector<T> object;
#ifdef MULTIPLE_OBJECT_LISTS
        quantizer.objectList.get(threadid, r.id, object);
#else
      quantizer.objectList.get(r.id, object);
#endif
        auto *o    = os.allocateNormalizedObject(object);
        r.distance = comparator(*q, *o);
        os.deleteObject(o);
        qresults.push(r);
      }
      os.deleteObject(q);
#ifdef NGTQ_OBJECT_IN_MEMORY
    }
#endif
    while (qresults.size() > exactResultSize) {
      qresults.pop();
    }
    return 0.0;
  }

  void searchInTwoSteps(QBG::BatchSearchContainer &searchContainer) {
    NGTThrowException("Not implemented yet.");
  }

  void searchInTwoSteps(QBG::SearchContainer &searchContainer) {
    auto parameterSize            = searchContainer.size;
    auto parameterExactResultSize = searchContainer.size;
    if (searchContainer.refinementExpansion >= 1.0) {
      parameterSize *= searchContainer.refinementExpansion;
    } else {
      parameterExactResultSize = 0;
    }
    NGT::ObjectDistances blobs;
    auto &quantizer               = getQuantizer();
    auto &globalIndex             = quantizer.globalCodebookIndex;
    auto &quantizedObjectDistance = quantizer.getQuantizedObjectDistance();
    if (searchContainer.objectVector.size() == 0) {
      NGTThrowException("search: object is null.");
    }
    auto dimension                  = getQuantizer().globalCodebookIndex.getObjectSpace().getDimension();
    std::vector<float> rotatedQuery = searchContainer.objectVector;
    if (rotatedQuery.size() < dimension) {
      if (rotatedQuery.size() == quantizer.property.genuineDimension ||
          rotatedQuery.size() + 1 == quantizer.property.genuineDimension) {
        rotatedQuery.resize(dimension);
      }
    }
    std::unique_ptr<std::vector<float>> resizedQuery = nullptr;
    if (parameterExactResultSize > 0) {
      std::unique_ptr<std::vector<float>> tmp(new std::vector<float>(rotatedQuery));
      resizedQuery = std::move(tmp);
    }
    NGT::Object *query = 0;
    try {
      query = allocateObject(rotatedQuery);
    } catch (NGT::Exception &err) {
      std::stringstream msg;
      msg << "search : allocate query for global. dimension=" << searchContainer.objectVector.size() << " "
          << err.what();
      NGTThrowException(msg);
    }
    {
      NGT::SearchContainer gsc(*query);
      gsc.setResults(&blobs);
      gsc.setEpsilon(searchContainer.blobExplorationCoefficient - 1.0);
      gsc.setSize(searchContainer.numOfProbes);
      globalIndex.search(gsc);
      if (blobs.empty()) {
        std::stringstream msg;
        msg << "Error! No blobs can be searched.";
        msg << " global index size=" << globalIndex.getObjectRepositorySize();
        msg << " size=" << gsc.size << " # of probes=" << searchContainer.numOfProbes;
        NGTThrowException(msg);
      }
    }
#if defined(NGTQG_ROTATION)
    if (quantizedObjectDistance.rotation != 0) {
      quantizedObjectDistance.rotation->mul(rotatedQuery.data());
    }
#endif
    void *selectiveQuery = rotatedQuery.data();
    uint8_t scalarQuantizedObject[rotatedQuery.size()];
    auto objectType = getScalarQuantizationType();
    if (objectType != NGT::ObjectSpace::ObjectTypeNone) {
      auto dimension = rotatedQuery.size();
      float sqobj[dimension];
      memcpy(sqobj, rotatedQuery.data(), dimension * sizeof(float));
      auto offset = getQuantizer().property.scalarQuantizationOffset;
      auto scale  = getQuantizer().property.scalarQuantizationScale;
      NGT::ObjectSpace::quantizeToQint8(sqobj, dimension, scalarQuantizedObject, objectType, offset, scale);
      selectiveQuery = scalarQuantizedObject;
    }

    std::unordered_map<size_t, NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8> luts;
    size_t foundCount    = 0;
    size_t k             = parameterSize;
    NGT::Distance radius = FLT_MAX;
    NGT::ResultSetWithCheck result;
#ifdef NGTQBG_COARSE_BLOB
    NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 lookupTable;
    quantizedObjectDistance.initialize(lookupTable);
#endif
    std::unique_ptr<NGTQ::BooleanSet> checkedIDs = nullptr;
    if (quantizer.objectList.size() < 5000000) {
      std::unique_ptr<NGTQ::BooleanVector> tmp(new NGTQ::BooleanVector(quantizer.objectList.size()));
      checkedIDs = std::move(tmp);
    } else {
      std::unique_ptr<NGTQ::BooleanHash> tmp(new NGTQ::BooleanHash(quantizer.objectList.size()));
      checkedIDs = std::move(tmp);
    }
    for (size_t idx = 0; idx < blobs.size(); idx++) {
#ifdef NGTQBG_COARSE_BLOB
      NGT::Distance blobDistance            = std::numeric_limits<NGT::Distance>::max();
      auto graphNodeID                      = blobs[idx].id;
      auto &graphNodeToInvertedIndexEntries = quantizer.getGraphNodeToInvertedIndexEntries();
      auto beginIvtID                       = graphNodeToInvertedIndexEntries[graphNodeID - 1] + 1;
      auto endIvtID                         = graphNodeToInvertedIndexEntries[graphNodeID] + 1;
      for (auto blobID = beginIvtID; blobID < endIvtID; blobID++) {
        auto subspaceID = quantizedBlobGraph[blobID].subspaceID;
        quantizedObjectDistance.createDistanceLookup(selectiveQuery, subspaceID, lookupTable);
        NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 &lut = lookupTable;
#else
      {
        auto blobID     = blobs[idx].id;
        auto subspaceID = quantizedBlobGraph[blobID].subspaceID;
        auto luti       = luts.find(subspaceID);
        if (luti == luts.end()) {
          luts.insert(std::make_pair(subspaceID, NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8()));
          luti = luts.find(subspaceID);
          quantizedObjectDistance.initialize((*luti).second);
          quantizedObjectDistance.createDistanceLookup(rotatedQuery.data(), subspaceID, (*luti).second);
        }
        NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 &lut = (*luti).second;
#endif
        NGT::Distance bd;
        std::tie(bd, radius) = judge(quantizedBlobGraph[blobID], k, radius, lut, result, foundCount,
                                     selectiveQuery, &checkedIDs);
#ifdef NGTQBG_COARSE_BLOB
        if (bd < blobDistance) {
          blobDistance = bd;
        }
#else
#endif
      }
#ifdef NGTQBG_MIN
#endif
    }
    if (searchContainer.resultIsAvailable()) {
      if (parameterExactResultSize > 0) {
        NGT::ObjectDistances &qresults = searchContainer.getResult();
        refineDistances(quantizer, result, qresults, parameterExactResultSize, resizedQuery);
      } else {
        searchContainer.getResult().moveFrom(result);
      }
    } else {
      if (parameterExactResultSize > 0) {
        refineDistances(quantizer, result, searchContainer.workingResult, parameterExactResultSize,
                        resizedQuery);
      } else {
        searchContainer.workingResult = std::move(result);
      }
    }
    deleteObject(query);
  } // namespace QBG

  void searchInOneStep(QBG::SearchContainer &searchContainer) {
    auto &globalIndex = getQuantizer().globalCodebookIndex;
    auto &globalGraph = static_cast<NGT::GraphAndTreeIndex &>(globalIndex.getIndex());
    NGT::ObjectDistances seeds;
    const size_t dimension = globalIndex.getObjectSpace().getPaddedDimension();
    if (dimension > searchContainer.objectVector.size()) {
      searchContainer.objectVector.resize(dimension);
    }
    NGT::Object query(searchContainer.objectVector, globalIndex.getObjectSpace());
    SearchContainer sc(searchContainer, query);
    globalGraph.getSeedsFromTree(sc, seeds);
    if (seeds.empty()) {
      globalGraph.getRandomSeeds(globalGraph.repository, seeds, 20);
    }
    searchInOneStep(sc, seeds);
    searchContainer.workingResult = std::move(sc.workingResult);
  }

  void searchInOneStep(QBG::SearchContainer &searchContainer, NGT::ObjectDistances &seeds) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    std::cerr << "searchBlobGraph: Not implemented. " << std::endl;
    abort();
#else
    if (!searchable) {
      std::stringstream msg;
      msg << "The specified index is not now searchable. ";
      NGTThrowException(msg);
    }
    auto parameterSize            = searchContainer.size;
    auto parameterExactResultSize = searchContainer.size;
    if (searchContainer.refinementExpansion >= 1.0) {
      parameterSize *= searchContainer.refinementExpansion;
    } else {
      parameterExactResultSize = 0;
    }

    auto &quantizer   = getQuantizer();
    auto &globalIndex = quantizer.globalCodebookIndex;
    auto &globalGraph = static_cast<NGT::GraphAndTreeIndex &>(globalIndex.getIndex());
    auto &objectSpace = globalIndex.getObjectSpace();

    if (globalGraph.searchRepository.empty()) {
      NGTThrowException("QBG:Index: graph repository is empty.");
    }
    if (searchContainer.explorationCoefficient == 0.0) {
      searchContainer.explorationCoefficient = NGT_EXPLORATION_COEFFICIENT;
    }

    const auto requestedSize = parameterSize;
    searchContainer.size     = std::numeric_limits<uint32_t>::max();

    // setup edgeSize
    size_t edgeSize = globalGraph.getEdgeSize(searchContainer);

    NGT::NeighborhoodGraph::UncheckedSet untracedNodes;

    NGT::NeighborhoodGraph::DistanceCheckedSet distanceChecked(globalGraph.searchRepository.size());
    NGT::NeighborhoodGraph::ResultSet results;

    if (objectSpace.getObjectType() == typeid(float)) {
      globalGraph.setupDistances(searchContainer, seeds, NGT::PrimitiveComparator::L2Float::compare);
    } else if (objectSpace.getObjectType() == typeid(uint8_t)) {
      globalGraph.setupDistances(searchContainer, seeds, NGT::PrimitiveComparator::L2Uint8::compare);
#ifdef NGT_HALF_FLOAT
    } else if (objectSpace.getObjectType() == typeid(NGT::float16)) {
      globalGraph.setupDistances(searchContainer, seeds, NGT::PrimitiveComparator::L2Float16::compare);
    }
#endif
    std::sort(seeds.begin(), seeds.end());
    NGT::ObjectDistance currentNearestBlob = seeds.front();
    NGT::Distance explorationRadius =
        searchContainer.blobExplorationCoefficient * currentNearestBlob.distance;
    std::priority_queue<NGT::ObjectDistance, std::vector<NGT::ObjectDistance>,
                        std::greater<NGT::ObjectDistance>>
        discardedObjects;
    untracedNodes.push(seeds.front());
    distanceChecked.insert(seeds.front().id);
    for (size_t i = 1; i < seeds.size(); i++) {
      untracedNodes.push(seeds[i]);
      distanceChecked.insert(seeds[i].id);
      discardedObjects.push(seeds[i]);
    }
    size_t explorationSize        = 1;
    auto &quantizedObjectDistance = quantizer.getQuantizedObjectDistance();
    std::unordered_map<size_t, NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8> luts;
    auto dimension                  = getQuantizer().globalCodebookIndex.getObjectSpace().getDimension();
    std::vector<float> rotatedQuery = searchContainer.objectVector;
    if (rotatedQuery.size() < dimension) {
      if (rotatedQuery.size() == quantizer.property.genuineDimension ||
          rotatedQuery.size() + 1 == quantizer.property.genuineDimension) {
        rotatedQuery.resize(dimension);
      }
    }
    std::unique_ptr<std::vector<float>> resizedQuery = nullptr;
    if (parameterExactResultSize > 0) {
      std::unique_ptr<std::vector<float>> tmp(new std::vector<float>(rotatedQuery));
      resizedQuery = std::move(tmp);
    }
    quantizedObjectDistance.rotation->mul(rotatedQuery.data());
    NGT::Distance radius = searchContainer.radius;
    if (requestedSize >= std::numeric_limits<int32_t>::max()) {
      radius *= searchContainer.explorationCoefficient;
    }
    NGT::ReadOnlyGraphNode *nodes = globalGraph.searchRepository.data();
    NGT::ObjectDistance target;
    const size_t prefetchSize   = objectSpace.getPrefetchSize();
    const size_t prefetchOffset = objectSpace.getPrefetchOffset();
#ifdef NGTQBG_COARSE_BLOB
    NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 lookupTable;
    quantizedObjectDistance.initialize(lookupTable);
#endif
    for (;;) {
      if (untracedNodes.empty() || untracedNodes.top().distance > explorationRadius) {
        explorationSize++;
        NGT::Distance blobDistance = std::numeric_limits<NGT::Distance>::max();
#ifdef NGTQBG_COARSE_BLOB
        auto graphNodeID                      = currentNearestBlob.id;
        auto &graphNodeToInvertedIndexEntries = quantizer.getGraphNodeToInvertedIndexEntries();
        auto beginIvtID                       = graphNodeToInvertedIndexEntries[graphNodeID - 1] + 1;
        auto endIvtID                         = graphNodeToInvertedIndexEntries[graphNodeID] + 1;
        for (auto blobID = beginIvtID; blobID < endIvtID; blobID++) {
          auto subspaceID = quantizedBlobGraph[blobID].subspaceID;
          quantizedObjectDistance.createDistanceLookup(rotatedQuery.data(), subspaceID, lookupTable);
          NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 &lut = lookupTable;
#else
          {
            auto blobID     = currentNearestBlob.id;
            auto subspaceID = quantizedBlobGraph[blobID].subspaceID;
            auto luti       = luts.find(subspaceID);
            if (luti == luts.end()) {
              luts.insert(
                  std::make_pair(subspaceID, NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8()));
              luti = luts.find(subspaceID);
              quantizedObjectDistance.initialize((*luti).second);
              quantizedObjectDistance.createDistanceLookup(rotatedQuery.data(), subspaceID, (*luti).second);
            }
            NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 &lut = (*luti).second;
#endif
          size_t foundCount;
          NGT::Distance bd;
          std::tie(bd, radius) =
              judge(quantizedBlobGraph[blobID], requestedSize, radius, lut, results, foundCount);
#ifdef NGTQBG_COARSE_BLOB
          if (bd < blobDistance) {
            blobDistance = bd;
          }
#else
            blobDistance = bd;
#endif
        }

#ifdef NGTQBG_MIN
        if (blobDistance > radius * searchContainer.explorationCoefficient) {
          break;
        }
#endif
        if (explorationSize > searchContainer.graphExplorationSize) {
          break;
        }
        if (discardedObjects.empty()) {
          break;
        }
        currentNearestBlob = discardedObjects.top();
        discardedObjects.pop();
        explorationRadius = searchContainer.blobExplorationCoefficient * currentNearestBlob.distance;
        continue;
      }
      target = untracedNodes.top();
      untracedNodes.pop();

      auto *neighbors      = &nodes[target.id];
      auto *neighborptr    = &(*neighbors)[0];
      size_t neighborSize  = neighbors->size() < edgeSize ? neighbors->size() : edgeSize;
      auto *neighborendptr = neighborptr + neighborSize;

#ifdef NGT_GRAPH_COMPACT_READ_ONLY_GRAPH
      NGT::ObjectRepository &objectRepository =
          quantizer.globalCodebookIndex.getObjectSpace().getRepository();
      pair<uint32_t, NGT::PersistentObject *> nsPtrs[neighborSize];
#else
        pair<uint32_t, NGT::PersistentObject *> *nsPtrs[neighborSize];
#endif
      size_t nsPtrsSize = 0;
#ifndef PREFETCH_DISABLE
      for (; neighborptr < neighborendptr; ++neighborptr) {
#ifdef NGT_VISIT_COUNT
        searchContainer.visitCount++;
#endif
#ifdef NGT_GRAPH_COMPACT_READ_ONLY_GRAPH
        if (!distanceChecked[*neighborptr]) {
          nsPtrs[nsPtrsSize].first  = *neighborptr;
          nsPtrs[nsPtrsSize].second = objectRepository.get(*neighborptr);
          distanceChecked.insert(*neighborptr);
#else
        if (!distanceChecked[(*(neighborptr)).first]) {
          distanceChecked.insert((*(neighborptr)).first);
          nsPtrs[nsPtrsSize] = neighborptr;
#endif
          if (nsPtrsSize < prefetchOffset) {
#ifdef NGT_GRAPH_COMPACT_READ_ONLY_GRAPH
            unsigned char *ptr = reinterpret_cast<unsigned char *>(nsPtrs[nsPtrsSize].second);
#else
            unsigned char *ptr = reinterpret_cast<unsigned char *>(nsPtrs[nsPtrsSize]->second);
#endif
            NGT::MemoryCache::prefetch(ptr, prefetchSize);
          }
          nsPtrsSize++;
        }
      }
#endif
#ifdef PREFETCH_DISABLE
      for (; neighborptr < neighborendptr; ++neighborptr) {
#else
        for (size_t idx = 0; idx < nsPtrsSize; idx++) {
#endif
#ifdef PREFETCH_DISABLE
        if (distanceChecked[(*(neighborptr)).first]) {
          continue;
        }
        distanceChecked.insert((*(neighborptr)).first);
#else

#ifdef NGT_GRAPH_COMPACT_READ_ONLY_GRAPH
          auto *neighborptr = &nsPtrs[idx];
#else
          auto *neighborptr = nsPtrs[idx];
#endif
          if (idx + prefetchOffset < nsPtrsSize) {
#ifdef NGT_GRAPH_COMPACT_READ_ONLY_GRAPH
            unsigned char *ptr = reinterpret_cast<unsigned char *>(nsPtrs[idx + prefetchOffset].second);
#else
            unsigned char *ptr = reinterpret_cast<unsigned char *>((*(nsPtrs[idx + prefetchOffset])).second);
#endif
            NGT::MemoryCache::prefetch(ptr, prefetchSize);
          }
#endif
#ifdef NGT_DISTANCE_COMPUTATION_COUNT
        searchContainer.distanceComputationCount++;
#endif
        NGT::Distance distance = objectSpace.getComparator()(searchContainer.object, *neighborptr->second);
        NGT::ObjectDistance r;
        r.set(neighborptr->first, distance);
        untracedNodes.push(r);
        if (distance < currentNearestBlob.distance) {
          discardedObjects.push(currentNearestBlob);
          currentNearestBlob = r;
          explorationRadius  = searchContainer.blobExplorationCoefficient * currentNearestBlob.distance;
        } else {
          discardedObjects.push(r);
        }
      }
    }

    if (searchContainer.resultIsAvailable()) {
      if (parameterExactResultSize > 0) {
        NGT::ObjectDistances &qresults = searchContainer.getResult();
        refineDistances(quantizer, results, qresults, parameterExactResultSize, resizedQuery);
      } else {
        searchContainer.getResult().moveFrom(results);
      }
    } else {
      if (parameterExactResultSize > 0) {
        refineDistances(quantizer, results, searchContainer.workingResult, parameterExactResultSize,
                        resizedQuery);
      } else {
        searchContainer.workingResult = std::move(results);
      }
    }
#endif
  }

#ifdef NGTQ_OBGRAPH
  void searchOBGraph(QBG::SearchContainer &searchContainer) {
    if (!searchable) {
      NGTThrowException("The specified index is not now searchable.");
    }
    if (getQuantizer().property.graphType != NGTQ::GraphTypeObjectBlobGraph) {
      std::stringstream msg;
      msg << "The specified index is not an object-blob graph. Type=" << getQuantizer().property.graphType;
      NGTThrowException(msg);
    }
    auto &globalIndex = getQuantizer().globalCodebookIndex;
    auto &globalGraph = static_cast<NGT::GraphAndTreeIndex &>(globalIndex.getIndex());
    NGT::ObjectDistances seeds;
    const size_t dimension = globalIndex.getObjectSpace().getPaddedDimension();
    if (dimension > searchContainer.objectVector.size()) {
      searchContainer.objectVector.resize(dimension);
    }
    NGT::Object query(searchContainer.objectVector, globalIndex.getObjectSpace());
    SearchContainer sc(searchContainer, query);
    globalGraph.getSeedsFromTree(sc, seeds);
    if (seeds.empty()) {
      globalGraph.getRandomSeeds(globalGraph.repository, seeds, 20);
    }
    globalGraph.setupDistances(sc, seeds);
    std::sort(seeds.begin(), seeds.end());
    if (seeds.size() > 1) {
      seeds.resize(1);
    }
    searchOBGraph(searchContainer, seeds);
  }

  void searchOBGraph(QBG::SearchContainer &searchContainer, NGT::ObjectDistances &seedBlob) {
    if (!searchable) {
      NGTThrowException("The specified index is not now searchable.");
    }
    auto parameterSize            = searchContainer.size;
    auto parameterExactResultSize = searchContainer.size;
    if (searchContainer.refinementExpansion >= 1.0) {
      parameterSize *= searchContainer.refinementExpansion;
    } else {
      parameterExactResultSize = 0;
    }

    auto &quantizer               = getQuantizer();
    auto &globalIndex             = quantizer.globalCodebookIndex;
    auto &globalGraph             = static_cast<NGT::GraphAndTreeIndex &>(globalIndex.getIndex());
    auto &quantizedObjectDistance = quantizer.getQuantizedObjectDistance();
    auto dimension                = getQuantizer().globalCodebookIndex.getObjectSpace().getDimension();

    if (globalGraph.searchRepository.empty()) {
      NGTThrowException("QBG:Index: graph repository is empty.");
    }

    if (searchContainer.explorationCoefficient == 0.0) {
      searchContainer.explorationCoefficient = NGT_EXPLORATION_COEFFICIENT;
    }

    // setup edgeSize
    std::vector<float> rotatedQuery = searchContainer.objectVector;
    if (rotatedQuery.size() < dimension) {
      if (rotatedQuery.size() == quantizer.property.genuineDimension ||
          rotatedQuery.size() + 1 == quantizer.property.genuineDimension) {
        rotatedQuery.resize(dimension);
      }
    }

    std::unique_ptr<std::vector<float>> resizedQuery = nullptr;
    if (parameterExactResultSize > 0) {
      std::unique_ptr<std::vector<float>> tmp(new std::vector<float>(rotatedQuery));
      resizedQuery = std::move(tmp);
    }
#if defined(NGTQG_ROTATION)
    if (quantizedObjectDistance.rotation != nullptr) {
      quantizedObjectDistance.rotation->mul(rotatedQuery.data());
    }
#endif

    void *selectiveQuery = rotatedQuery.data();
    uint8_t scalarQuantizedObject[rotatedQuery.size()];
    auto objectType = getScalarQuantizationType();
    if (objectType != NGT::ObjectSpace::ObjectTypeNone) {
      auto dimension = rotatedQuery.size();
      float sqobj[dimension];
      memcpy(sqobj, rotatedQuery.data(), dimension * sizeof(float));
      auto offset = getQuantizer().property.scalarQuantizationOffset;
      auto scale  = getQuantizer().property.scalarQuantizationScale;
      NGT::ObjectSpace::quantizeToQint8(sqobj, dimension, scalarQuantizedObject, objectType, offset, scale);
      selectiveQuery = scalarQuantizedObject;
    }

    std::unordered_map<size_t, NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8> luts;
    size_t k                        = parameterSize;
    NGT::Distance radius            = std::numeric_limits<NGT::Distance>::max();
    NGT::Distance explorationRadius = radius;
    NGT::ResultSetWithCheck result;

    NGT::PriorityQueue<NGT::ObjectDistance> targetBlobs;

    NGT::NeighborhoodGraph::ResultSet results;
    std::vector<float> blobDistances(quantizedBlobGraph.size(), std::numeric_limits<float>::max());
    targetBlobs.emplace(NGT::ObjectDistance(0, std::numeric_limits<float>::max()));
    targetBlobs.emplace(seedBlob.front());
#ifdef NGTQBG_COARSE_BLOB
    NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 lookupTable;
    quantizedObjectDistance.initialize(lookupTable);
#endif
    while (!targetBlobs.empty()) {
      auto currentBlobIndex = targetBlobs.top();
      if (currentBlobIndex.distance > explorationRadius) {
        break;
      }
      targetBlobs.pop();
      if (blobDistances[currentBlobIndex.id] < 0) {
        continue;
      }
      blobDistances[currentBlobIndex.id] = -1;
      {
#ifdef NGTQBG_COARSE_BLOB
        NGT::Distance blobDistance            = std::numeric_limits<NGT::Distance>::max();
        auto graphNodeID                      = currentBlobIndex.id;
        auto &graphNodeToInvertedIndexEntries = quantizer.getGraphNodeToInvertedIndexEntries();
        auto beginIvtID                       = graphNodeToInvertedIndexEntries[graphNodeID - 1] + 1;
        auto endIvtID                         = graphNodeToInvertedIndexEntries[graphNodeID] + 1;
        for (auto blobID = beginIvtID; blobID < endIvtID; blobID++) {
          auto subspaceID = quantizedBlobGraph[blobID].subspaceID;
          quantizedObjectDistance.createDistanceLookup(selectiveQuery, subspaceID, lookupTable);
          NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 &lut = lookupTable;
#else
        auto blobID     = currentBlobIndex.id;
        auto subspaceID = quantizedBlobGraph[blobID].subspaceID;
        auto luti       = luts.find(subspaceID);
        if (luti == luts.end()) {
          luts.insert(std::make_pair(subspaceID, NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8()));
          luti = luts.find(subspaceID);
          quantizedObjectDistance.initialize((*luti).second);
          quantizedObjectDistance.createDistanceLookup(rotatedQuery.data(), subspaceID, (*luti).second);
        }
        NGTQ::QuantizedObjectDistance::DistanceLookupTableUint8 &lut = (*luti).second;
#endif
          {
            auto &currentBlob = quantizedBlobGraph[blobID];
            auto noOfObjects  = currentBlob.ids.size();
            {
              uint8_t *bobjs = static_cast<uint8_t *>(currentBlob.objects);
              size_t size    = ((noOfObjects - 1) / (NGTQ_SIMD_BLOCK_SIZE * NGTQ_BATCH_SIZE) + 1) *
                            (NGTQ_SIMD_BLOCK_SIZE * NGTQ_BATCH_SIZE);
              size /= 2;
              size *= getQuantizer().divisionNo;
              NGT::MemoryCache::prefetch(bobjs, size);
            }
            auto &quantizedObjectDistance = getQuantizer().getQuantizedObjectDistance();
            std::vector<float> distances(quantizedObjectDistance.getNumOfAlignedObjects(noOfObjects));
#ifdef NGTQBG_MIN
            float minDistance = quantizedObjectDistance(currentBlob.objects, distances.data(), noOfObjects,
                                                        lut, selectiveQuery);
#else
          quantizedObjectDistance(currentBlob.objects, distances.data(), noOfObjects, lut, selectiveQuery);
#endif
#undef COUNT
#ifdef NGTQBG_MIN
            if (minDistance <= radius)
#endif
            {
              for (size_t i = 0; i < noOfObjects; i++) {
                if (distances[i] <= radius) {
                  if (result.size() >= k) {
                    result.push_pop(NGT::ObjectDistance(currentBlob.ids[i], distances[i]));
                    radius            = result.top().distance;
                    explorationRadius = searchContainer.explorationCoefficient * radius;
                  } else {
                    result.emplace(NGT::ObjectDistance(currentBlob.ids[i], distances[i]));
                  }
                }
              }
            }
#ifdef NGTQBG_MIN
            if (minDistance <= explorationRadius)
#endif
            {
              for (size_t i = 0; i < noOfObjects; i++) {
                if (distances[i] <= explorationRadius) {
                  for (auto &bid : currentBlob.blobIDs[i]) {
                    if (blobDistances[bid] > distances[i]) {
                      blobDistances[bid] = distances[i];
                      if (targetBlobs.c.back().distance > explorationRadius) {
                        targetBlobs.pop_back();
                      }
                      targetBlobs.emplace(NGT::ObjectDistance(bid, distances[i]));
                    }
                  }
                }
              }
            }
          }
#ifdef NGTQBG_COARSE_BLOB
          if (bd < blobDistance) {
            blobDistance = bd;
          }
#endif
        }
      }
      if (searchContainer.resultIsAvailable()) {
        if (parameterExactResultSize > 0) {
          NGT::ObjectDistances &qresults = searchContainer.getResult();
          refineDistances(quantizer, result, qresults, parameterExactResultSize, resizedQuery);
        } else {
          searchContainer.getResult().moveFrom(result);
        }
      } else {
        if (parameterExactResultSize > 0) {
          refineDistances(quantizer, result, searchContainer.workingResult, parameterExactResultSize,
                          resizedQuery);
        } else {
          searchContainer.workingResult = std::move(result);
        }
      }
    }
#endif

    void search(QBG::SearchContainer & searchContainer) { searchInOneStep(searchContainer); }
    void save() { quantizedBlobGraph.save(path); }

    void load() {
      if (quantizedBlobGraph.stat(path)) {
        quantizedBlobGraph.load(path);
        auto objectListSize = getQuantizer().objectList.size();
        std::cerr << "pass objectList.size=" << objectListSize << std::endl;
        quantizedBlobGraph.extractRemovedIdSet(objectListSize, removedIDs);
      } else {
        NGTThrowException("Not found the rearranged inverted index. [" + path + "]");
      }
    }

    static void buildNGTQ(const std::string &indexPath, bool verbose = false) {
      load(indexPath, QBG::Index::getQuantizerCodebookFile(indexPath), "", "", "", verbose);
      buildNGTQ(indexPath, "", "-", "-", 1, 0, verbose);
      if (verbose) {
        std::cerr << "NGTQ and NGTQBG indices are completed." << std::endl;
        std::cerr << "  vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
        std::cerr << "  peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
      }
    }

    static void build(const std::string &indexPath, bool verbose = false) {
      load(indexPath, "", "", "", "", verbose);
      buildNGTQ(indexPath, "", "", "", 1, 0, verbose);
      buildQBG(indexPath, verbose);
      if (verbose) {
        std::cerr << "NGTQ and NGTQBG indices are completed." << std::endl;
        std::cerr << "  vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
        std::cerr << "  peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
      }
    }

    static void build(const std::string &indexPath, std::string quantizerCodebookFile = "",
                      std::string codebookIndexFile = "", std::string objectIndexFile = "",
                      size_t beginID = 1, size_t endID = 0, bool verbose = false) {
      buildNGTQ(indexPath, quantizerCodebookFile, codebookIndexFile, objectIndexFile, beginID, endID,
                verbose);
      buildQBG(indexPath, verbose);
      std::cerr << "NGTQ and NGTQBG indices are completed." << std::endl;
      std::cerr << "  vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
      std::cerr << "  peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
    }

    static void build(const std::string &indexPath, std::vector<std::vector<float>> &quantizerCodebook,
                      std::vector<uint32_t> &codebookIndex, std::vector<std::vector<uint32_t>> &objectIndex,
                      size_t beginID = 1, size_t endID = 0, bool verbose = false) {
      buildNGTQ(indexPath, quantizerCodebook, codebookIndex, objectIndex, beginID, endID, verbose);
      buildQBG(indexPath);
      std::cerr << "NGTQ and NGTQBG indices are completed." << std::endl;
      std::cerr << "  vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
      std::cerr << "  peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
    }

    static void buildNGTQ(const std::string &indexPath, std::string quantizerCodebookFile = "",
                          std::string codebookIndexFile = "", std::string objectIndexFile = "",
                          size_t beginID = 1, size_t endID = 0, bool verbose = false) {
      std::vector<std::vector<float>> quantizerCodebook;
      std::vector<uint32_t> codebookIndex;
      std::vector<std::vector<uint32_t>> objectIndex;
      {
        std::string codebookPath = quantizerCodebookFile;
        if (codebookPath.empty()) {
          codebookPath = QBG::Index::getQuantizerCodebookFile(indexPath);
        }
        if (codebookPath != "-") {
          std::ifstream stream(codebookPath);
          if (!stream) {
            std::stringstream msg;
            msg << "Cannot open the codebook. " << codebookPath;
            NGTThrowException(msg);
          }
          std::string line;
          while (getline(stream, line)) {
            std::vector<std::string> tokens;
            NGT::Common::tokenize(line, tokens, " \t");
            std::vector<float> object;
            for (auto &token : tokens) {
              object.push_back(NGT::Common::strtof(token));
            }
            if (!quantizerCodebook.empty() && quantizerCodebook[0].size() != object.size()) {
              std::stringstream msg;
              msg << "The specified quantizer codebook is invalid. " << quantizerCodebook[0].size() << ":"
                  << object.size() << ":" << quantizerCodebook.size() << ":" << line;
              NGTThrowException(msg);
            }
            if (!object.empty()) {
              quantizerCodebook.push_back(object);
            }
          }
        }
      }
      {
        std::string codebookIndexPath = codebookIndexFile;
        if (codebookIndexPath.empty()) {
          codebookIndexPath = QBG::Index::getCodebookIndexFile(indexPath);
        }
        if (codebookIndexPath != "-") {
          std::ifstream stream(codebookIndexPath);
          if (!stream) {
            std::stringstream msg;
            msg << "Cannot open the codebook index. " << codebookIndexPath;
            NGTThrowException(msg);
          }
          std::string line;
          while (getline(stream, line)) {
            std::vector<std::string> tokens;
            NGT::Common::tokenize(line, tokens, " \t");
            std::vector<float> object;
            if (tokens.size() != 1) {
              std::stringstream msg;
              msg << "The specified codebook index is invalid. " << line;
              NGTThrowException(msg);
            }
            codebookIndex.push_back(NGT::Common::strtol(tokens[0]));
          }
        }
      }
      {
        std::string objectIndexPath = objectIndexFile;
        if (objectIndexPath.empty()) {
          objectIndexPath = QBG::Index::getObjectIndexFile(indexPath);
        }
        if (objectIndexPath != "-") {
          {
            std::ifstream stream(objectIndexPath);
            if (!stream) {
              std::stringstream msg;
              msg << "Cannot open the codebook index. " << objectIndexPath;
              NGTThrowException(msg);
            }
            size_t nOfObjs = 0;
            std::string line;
            while (getline(stream, line))
              nOfObjs++;
            objectIndex.resize(nOfObjs);
          }
          {
            std::ifstream stream(objectIndexPath);
            if (!stream) {
              std::stringstream msg;
              msg << "Cannot open the codebook index. " << objectIndexPath;
              NGTThrowException(msg);
            }
            std::string line;
            size_t idx = 0;
            while (getline(stream, line)) {
              std::vector<std::string> tokens;
              NGT::Common::tokenize(line, tokens, " \t");
              if (tokens.size() > 0) {
                objectIndex[idx].reserve(tokens.size());
                for (auto &token : tokens) {
                  objectIndex[idx].emplace_back(NGT::Common::strtol(token));
                }
              }
              idx++;
            }
          }
        }
      }
      buildNGTQ(indexPath, quantizerCodebook, codebookIndex, objectIndex, beginID, endID, verbose);
    }

    static void buildNGTQ(const std::string &indexPath, std::vector<std::vector<float>> &quantizerCodebook,
                          std::vector<uint32_t> &codebookIndex,
                          std::vector<std::vector<uint32_t>> &objectIndex, size_t beginID = 1,
                          size_t endID = 0, bool verbose = false) {
      NGT::StdOstreamRedirector redirector(!verbose);
      redirector.begin();
      NGT::Timer timer;
      timer.start();
      NGTQ::Index index(indexPath);
      if ((quantizerCodebook.size() == 0) && (codebookIndex.size() == 0) && (objectIndex.size() == 0)) {
        index.createIndex(beginID, endID);
      } else {
        if (codebookIndex.size() == 0) {
          codebookIndex.resize(quantizerCodebook.size());
        }
        if (codebookIndex.size() == 0) {
          stringstream msg;
          msg << "The specified codebook indexe invalild " << codebookIndex.size();
          NGTThrowException(msg);
        }
        if (objectIndex.size() == 0) {
          size_t size = index.getQuantizer().objectList.size();
          size        = size == 0 ? 0 : size - 1;
          objectIndex.resize(size);
          for (auto &list : objectIndex) {
            list.emplace_back(0);
          }
        }
        index.createIndex(codebookIndex, objectIndex, beginID, endID);
      }

      {
        if (access(QBG::Index::getBlobFile(indexPath).c_str(), F_OK) == 0) {
          const std::string comcp =
              "cp -f " + QBG::Index::getBlobFile(indexPath) + " " + QBG::Index::getStoredBlobFile(indexPath);
          auto stat = system(comcp.c_str());
          if (stat == -1) {
            std::cerr << "Warning. Cannot execute cp. " << comcp << std::endl;
          } else if (WIFEXITED(stat)) {
            int exitStatus = WEXITSTATUS(stat);
            if (exitStatus != 0) {
              std::cerr << "Warning. Cannot cp the blob. " << exitStatus << std::endl;
            }
          }
        }
        char *s = getenv("NGT_NOT_REMOVE_WORKSPACE");
        if (s == 0) {
          const std::string comrmdir = "rm -rf " + indexPath + "/" + getWorkspaceName();
          if (system(comrmdir.c_str()) == -1) {
            std::cerr << "Warning. cannot remove the workspace directory. " << comrmdir << std::endl;
          }
        }
      }

      timer.stop();
      index.save();

      QBG::Optimizer::extractScaleAndOffset(indexPath, -1.0, -1, verbose);

      redirector.end();
      std::cerr << "NGTQ index is completed." << std::endl;
      std::cerr << "  time=" << timer << std::endl;
      std::cerr << "  vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
      std::cerr << "  peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
      std::cerr << "saving..." << std::endl;
    }

    static void buildQBG(const std::string &indexPath, bool verbose = false) {
      NGT::Timer timer;
      timer.start();
      auto readOnly = false;
      QBG::Index index(indexPath, readOnly, verbose);
      try {
        index.load();
        stringstream msg;
        msg << "QBG::Index::buildQBG: The index is already built. ";
        NGTThrowException(msg);
      } catch (...) {
      }
      index.quantizedBlobGraph.construct(index);

      timer.stop();
      if (verbose) {
        std::cerr << "QBG index is completed." << std::endl;
        std::cerr << "  time=" << timer << std::endl;
        std::cerr << "  vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
        std::cerr << "  peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
        std::cerr << "saving..." << std::endl;
      }
      index.save();
    }

    void extract(std::ostream & os, size_t n, bool random = true) {
      if (n == 0) {
        NGTThrowException("QuantizedBlobGraph::extract # of objects is zero.");
      }
      auto &quantizer = getQuantizer();
      size_t dim      = quantizer.property.dimension;
      std::vector<float> object;
      if (random) {
        struct timeval randTime;
        gettimeofday(&randTime, 0);
        srand(randTime.tv_usec);
        if (n > quantizer.objectList.size() / 2) {
          if (n > quantizer.objectList.size() - 1) {
            n = quantizer.objectList.size() - 1;
          }
          size_t pickedObjectCount = 0;
          for (size_t id = 1; id < quantizer.objectList.size(); id++) {
            double random = ((double)rand() + 1.0) / ((double)RAND_MAX + 2.0);
            double p      = static_cast<double>(n - pickedObjectCount) /
                       static_cast<double>(quantizer.objectList.size() - id);
            if (p == 0.0) {
              break;
            }
            if (random <= p) {
              if (!quantizer.objectList.get(id, object, &quantizer.globalCodebookIndex.getObjectSpace())) {
                std::cerr << "Cannot get the object. " << id << std::endl;
                continue;
              }
              if (dim != 0) {
                object.resize(dim, 0.0);
              }
              for (auto v = object.begin(); v != object.end(); ++v) {
                if (v + 1 != object.end()) {
                  os << *v << "\t";
                } else {
                  os << *v << std::endl;
                  ;
                }
              }
              pickedObjectCount++;
              if (pickedObjectCount == n) {
                break;
              }
              if (pickedObjectCount % 100000 == 0) {
                std::cerr << "loaded " << static_cast<float>(pickedObjectCount + 1) / 1000000.0
                          << "M objects." << std::endl;
              }
            }
          }
        } else {
          std::unordered_set<uint32_t> pickedObjects;
          for (size_t cnt = 0; cnt < n; cnt++) {
            size_t id = 0;
            while (true) {
              do {
                double random = ((double)rand() + 1.0) / ((double)RAND_MAX + 2.0);
                id            = floor(quantizer.objectList.size() * random);
              } while (pickedObjects.count(id) > 0 || id >= quantizer.objectList.size());
              if (quantizer.objectList.get(id, object, &quantizer.globalCodebookIndex.getObjectSpace())) {
                pickedObjects.insert(id);
                break;
              } else {
                std::cerr << "Cannot get the object. " << id << std::endl;
              }
            }
            if (cnt + 1 % 100000 == 0) {
              std::cerr << "loaded " << static_cast<float>(cnt + 1) / 1000000.0 << "M objects." << std::endl;
            }
            if (dim != 0) {
              object.resize(dim, 0.0);
            }
            for (auto v = object.begin(); v != object.end(); ++v) {
              if (v + 1 != object.end()) {
                os << *v << "\t";
              } else {
                os << *v << std::endl;
                ;
              }
            }
          }
        }
      } else {
        for (size_t cnt = 1; cnt <= n; cnt++) {
          if (!quantizer.objectList.get(cnt, object, &quantizer.globalCodebookIndex.getObjectSpace())) {
            std::cerr << "Cannot get the object. " << cnt << std::endl;
            continue;
          }
          if (cnt % 100000 == 0) {
            std::cerr << "loaded " << static_cast<float>(cnt) / 1000000.0 << "M objects." << std::endl;
          }
          if (dim != 0) {
            object.resize(dim, 0.0);
          }
          for (auto v = object.begin(); v != object.end(); ++v) {
            if (v + 1 != object.end()) {
              os << *v << "\t";
            } else {
              os << *v << std::endl;
              ;
            }
          }
          if (n > 0 && cnt >= n) {
            break;
          }
        }
      }
    }

    static void load(std::string indexPath, std::string blobs = "", std::string localCodebooks = "",
                     std::string quantizerCodebook = "", std::string rotationPath = "", bool verbose = false,
                     int threadSize = 0) {
      NGT::StdOstreamRedirector redirector(!verbose);
      redirector.begin();
      if (blobs.empty()) {
        blobs = QBG::Index::getBlobFile(indexPath);
      }
      if (localCodebooks.empty()) {
        localCodebooks = QBG::Index::getPQFile(indexPath) + "/" + QBG::Index::getSubvectorPrefix() + "-@";
      }
      if (quantizerCodebook.empty()) {
        quantizerCodebook = QBG::Index::getQuantizerCodebookFile(indexPath);
      }
      if (rotationPath.empty()) {
        rotationPath = QBG::Index::getRotationFile(indexPath);
      }

      threadSize = threadSize == 0 ? std::thread::hardware_concurrency() : threadSize;
      assert(threadSize != 0);

      size_t dataSize = 0;
      NGTQ::Property property;
      property.load(indexPath);
      {
        const std::string src = indexPath + "/" + NGTQ::Quantizer::getGlobalFile();
        const std::string dst = indexPath + "/" + NGTQ::Quantizer::getGlobalFile() + ".bak";
        if (rename(src.c_str(), dst.c_str()) == -1) {
          std::stringstream msg;
          msg << "Error! Moving is failed. " << src << ":" << dst << " " << errno << ":"
              << std::strerror(errno);
          NGTThrowException(msg);
        }
        NGT::Index::appendFromTextObjectFile(dst, blobs, dataSize);
        auto unlog = false;
        NGT::GraphOptimizer graphOptimizer(unlog);
        graphOptimizer.searchParameterOptimization   = false;
        graphOptimizer.prefetchParameterOptimization = false;
        graphOptimizer.accuracyTableGeneration       = false;
        int numOfOutgoingEdges                       = 10;
        int numOfIncomingEdges                       = 120;
        int numOfQueries                             = 200;
        int numOfResultantObjects                    = 20;
        graphOptimizer.set(numOfOutgoingEdges, numOfIncomingEdges, numOfQueries, numOfResultantObjects);
        const std::string rmcom = "rm -rf " + dst;
        try {
          graphOptimizer.execute(dst, src);
        } catch (NGT::Exception &err) {
          if (system(rmcom.c_str()) == -1) {
            std::cerr << "Warning. remove is failed. \"" << rmcom << "\"" << std::endl;
          }
          throw err;
        }
        if (system(rmcom.c_str()) == -1) {
          std::stringstream msg;
          msg << "Error! Removing is failed. \"" << rmcom << "\" " << errno << ":" << std::strerror(errno);
          NGTThrowException(msg);
        }
      }

      if (property.centroidCreationMode != NGTQ::CentroidCreationModeStaticLayer &&
          property.centroidCreationMode != NGTQ::CentroidCreationModeStatic) {
        std::cerr << "Warning. Inspite of not static mode, load the local codebook." << std::endl;
      }
      std::vector<std::string> tokens;
      NGT::Common::tokenize(localCodebooks, tokens, "@");
      if (tokens.size() != 2) {
        NGTThrowException("No @ in the specified local codebook string.");
      }
      for (size_t no = 0; no < property.localDivisionNo; no++) {
        std::stringstream data;
        data << tokens[0] << no << tokens[1];
        std::stringstream localCodebook;
        localCodebook << indexPath << "/" + NGTQ::Quantizer::getLocalPrefix() << no;
        std::cerr << data.str() << "->" << localCodebook.str() << std::endl;
        NGT::Index::append(localCodebook.str(), data.str(), threadSize, dataSize);
      }
      property.localCodebookState = true;
      property.save(indexPath);
#ifdef NGTQ_QBG
      std::vector<std::vector<float>> qCodebook;
      {
        std::ifstream stream(quantizerCodebook);
        if (!stream) {
          std::stringstream msg;
          msg << "Cannot open the codebook. " << quantizerCodebook;
          NGTThrowException(msg);
        }
        std::string line;
        while (getline(stream, line)) {
          std::vector<std::string> tokens;
          NGT::Common::tokenize(line, tokens, " \t");
          std::vector<float> object;
          for (auto &token : tokens) {
            object.push_back(NGT::Common::strtof(token));
          }
          if (!qCodebook.empty() && qCodebook[0].size() != object.size()) {
            std::stringstream msg;
            msg << "The specified quantizer codebook is invalid. " << qCodebook[0].size() << ":"
                << object.size() << ":" << qCodebook.size() << ":" << line;
            NGTThrowException(msg);
          }
          if (!object.empty()) {
            qCodebook.push_back(object);
          }
        }
      }
      {
        cerr << "qbg: loading the rotation..." << endl;
        std::vector<float> rotation;

        std::ifstream stream(rotationPath);
        if (!stream) {
          std::stringstream msg;
          msg << "Cannot open the rotation. " << rotationPath;
          NGTThrowException(msg);
        }
        std::string line;
        while (getline(stream, line)) {
          std::vector<std::string> tokens;
          NGT::Common::tokenize(line, tokens, " \t");
          for (auto &token : tokens) {
            rotation.push_back(NGT::Common::strtof(token));
          }
        }
        QBG::Index::load(indexPath, qCodebook, rotation);
      }
#endif
      redirector.end();
    }

    static void setupObjects(std::string indexPath, size_t nOfObjects, bool verbose) {
      NGTQ::Property property;
      property.load(indexPath);
      if (property.distanceType == NGTQ::DistanceType::DistanceTypeInnerProduct) {
        Optimizer::convertObjectsFromInnerProductToL2(indexPath, nOfObjects, verbose);
      }
      if (property.distanceType == NGTQ::DistanceType::DistanceTypeNormalizedCosine) {
        Optimizer::normalizeObjectsForCosine(indexPath, nOfObjects, verbose);
      }
    }

    NGT::ObjectSpace::ObjectType getScalarQuantizationType() {
      NGT::ObjectSpace::ObjectType objectType = NGT::ObjectSpace::ObjectTypeNone;
      switch (getQuantizer().property.localClusterDataType) {
      case NGTQ::ClusterDataTypeSQSU8: objectType = NGT::ObjectSpace::ObjectType::Qsuint8; break;
      default: objectType = NGT::ObjectSpace::ObjectTypeNone;
      }
      return objectType;
    }

    void saveProperty() { getQuantizer().property.save(path); }

#ifdef NGTQ_OBGRAPH
    static void constructONNGForSetBlobID(size_t outEdge, size_t inEdge, size_t inRank,
                                          std::vector<NGT::ObjectDistances> &results,
                                          std::vector<std::vector<NGT::ObjectID>> &edges);
    static void constructExperimentalGraphForSetBlobID(vector<NGT::ObjectDistances> & results, size_t outEdge,
                                                       float threshold,
                                                       std::vector<std::vector<NGT::ObjectID>> &edges);
    static void setBlobIDs(QBG::Index & index, std::vector<std::vector<NGT::ObjectID>> & edges);

    static void extractEdgesFromGraph(std::string & ngtPath, std::vector<std::vector<NGT::ObjectID>> & edges);

    static void extractEdgesBySearch(QBG::Index & index, std::vector<NGT::ObjectDistances> & results,
                                     size_t outEdge, size_t inEdge, QBG::SearchContainer &sc);

    static void setBlobIDsFromGraph(std::string & indexPath, std::string & ngtPath,
                                    NGTQ::DataType refinementDataType) {
      QBG::Index index(indexPath, false, refinementDataType);
      std::vector<std::vector<NGT::ObjectID>> edges;
      extractEdgesFromGraph(ngtPath, edges);
      setBlobIDs(index, edges);
    }

    static void setBlobIDsFromONNGBySearch(std::string & indexPath, QBG::SearchContainer & sc, size_t outEdge,
                                           size_t inEdge, size_t inRank, NGTQ::DataType refinementDataType) {
      QBG::Index index(indexPath, false, refinementDataType);
      std::vector<std::vector<NGT::ObjectID>> edges;
      std::vector<NGT::ObjectDistances> results;
      extractEdgesBySearch(index, results, outEdge, inEdge, sc);
      constructONNGForSetBlobID(outEdge, inEdge, inRank, results, edges);
      setBlobIDs(index, edges);
    }

    static void setBlobIDsFromExtGraphBySearch(std::string & indexPath, QBG::SearchContainer & sc,
                                               size_t outEdge, size_t inEdge, float threshold,
                                               NGTQ::DataType refinementDataType) {
      QBG::Index index(indexPath, false, refinementDataType);
      std::vector<std::vector<NGT::ObjectID>> edges;
      std::vector<NGT::ObjectDistances> results;
      extractEdgesBySearch(index, results, outEdge, inEdge, sc);
      constructExperimentalGraphForSetBlobID(results, outEdge, threshold, edges);
      setBlobIDs(index, edges);
    }

#endif

    static const std::string getSubvectorPrefix() { return "sv"; }
    static const std::string getHierarchicalClusteringPrefix() { return "hkc"; }
    static const std::string getSecondCentroidSuffix() { return "_2c"; }
    static const std::string getThirdCentroidSuffix() { return "_3c"; }
    static const std::string get3rdTo2ndSuffix() { return "_3to2"; }
    static const std::string getObjTo3rdSuffix() { return "_oto3"; }
    static const std::string getResidualFile() { return "r"; }
    static const std::string getRotatedResidualFile() { return "Rr"; }
    static const std::string getObjectFile() { return "obj"; }
    static const std::string getRotationFile() { return "R"; }
    static const std::string getWorkSpacePrefix(std::string indexPath) {
      return indexPath + "/" + getWorkspaceName();
    }
    static const std::string getTrainObjectFile(std::string indexPath) {
      return getWorkSpacePrefix(indexPath) + "/" + getObjectFile();
    }
    static const std::string getPrefix(std::string indexPath) {
      return getWorkSpacePrefix(indexPath) + "/" + getHierarchicalClusteringPrefix();
    }
    static const std::string getPQFile(std::string indexPath) { return getPrefix(indexPath) + "_opt"; }
#ifdef NGTQBG_COARSE_BLOB
    static const std::string getBlobFile(std::string indexPath) {
      return getPrefix(indexPath) + getSecondCentroidSuffix();
    }
    static const std::string getQuantizerCodebookFile(std::string indexPath) {
      return getPrefix(indexPath) + getThirdCentroidSuffix();
    }
#else
  static const std::string getBlobFile(std::string indexPath) {
    return getPrefix(indexPath) + getThirdCentroidSuffix();
  }
  static const std::string getQuantizerCodebookFile(std::string indexPath) {
    return getPrefix(indexPath) + getSecondCentroidSuffix();
  }
#endif
    static const std::string getCodebookIndexFile(std::string indexPath) {
      return getPrefix(indexPath) + get3rdTo2ndSuffix();
    }
    static const std::string getObjectIndexFile(std::string indexPath) {
      return getPrefix(indexPath) + getObjTo3rdSuffix();
    }
    static const std::string getRotationFile(std::string indexPath) {
      return getPQFile(indexPath) + "/" + getRotationFile();
    }
    static const std::string getStoredBlobFile(std::string indexPath) { return indexPath + "/blbc"; }

    static const std::string getWorkspaceName() { return "ws"; }
    const std::string path;
    QuantizedBlobGraphRepository quantizedBlobGraph;
    bool searchable;
    std::vector<uint32_t> removedIDs;

  }; // namespace QBG

} // namespace QBG

#endif
