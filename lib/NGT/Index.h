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

#include <string>
#include <vector>
#include <map>
#include <set>
#include <bitset>
#include <iomanip>
#include <unordered_set>
#include <thread>

#include <sys/time.h>
#include <sys/stat.h>
#include <stdint.h>

#include "NGT/defines.h"
#include "NGT/Common.h"
#include "NGT/Tree.h"
#include "NGT/Thread.h"
#include "NGT/Graph.h"

namespace NGT {

class Property;

class Index {
 public:
  enum OpenType {
    OpenTypeNone           = 0x00,
    OpenTypeGraphDisabled  = 0x01,
    OpenTypeTreeDisabled   = 0x02,
    OpenTypeObjectDisabled = 0x04
  };

  class Property {
   public:
    typedef ObjectSpace::ObjectType ObjectType;
    typedef ObjectSpace::DistanceType DistanceType;
    typedef NeighborhoodGraph::SeedType SeedType;
    typedef NeighborhoodGraph::GraphType GraphType;
    typedef NeighborhoodGraph::EpsilonType EpsilonType;
    enum ObjectAlignment { ObjectAlignmentNone = 0, ObjectAlignmentTrue = 1, ObjectAlignmentFalse = 2 };
    enum IndexType { IndexTypeNone = 0, GraphAndTree = 1, Graph = 2 };
    enum DatabaseType { DatabaseTypeNone = 0, Memory = 1, MemoryMappedFile = 2 };
    Property() { setDefault(); }
    void setDefault() {
      dimension      = 0;
      threadPoolSize = 32;
      objectType     = ObjectSpace::ObjectType::Float;
#ifdef NGT_REFINEMENT
      refinementObjectType = ObjectSpace::ObjectType::Float;
#endif
      distanceType           = DistanceType::DistanceTypeL2;
      indexType              = IndexType::GraphAndTree;
      objectAlignment        = ObjectAlignment::ObjectAlignmentFalse;
      pathAdjustmentInterval = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      databaseType = DatabaseType::MemoryMappedFile;
      //graphSharedMemorySize	= 512; // MB
      //treeSharedMemorySize	= 512; // MB
      //objectSharedMemorySize	= 512; // MB  512 is up to 50M objects.
      graphSharedMemorySize  = 10240; // MB
      treeSharedMemorySize   = 10240; // MB
      objectSharedMemorySize = 10240; // MB  512 is up to 50M objects.
#else
      databaseType = DatabaseType::Memory;
#endif
      prefetchOffset                = 0;
      prefetchSize                  = 0;
      maxMagnitude                  = -1.0;
      quantizationScale             = 0.0;
      quantizationOffset            = 0.0;
      clippingRate                  = 0.0;
      nOfNeighborsForInsertionOrder = 0;
      epsilonForInsertionOrder      = 0.1;
      leafNodeSize                  = NGT::LeafNode::LeafObjectsSizeMax;
      internalChildrenSize          = NGT::InternalNode::InternalChildrenSizeMax;
    }
    void clear() {
      dimension      = -1;
      threadPoolSize = -1;
      objectType     = ObjectSpace::ObjectTypeNone;
#ifdef NGT_REFINEMENT
      refinementObjectType = ObjectSpace::ObjectTypeNone;
#endif
      distanceType           = DistanceType::DistanceTypeNone;
      indexType              = IndexTypeNone;
      databaseType           = DatabaseTypeNone;
      objectAlignment        = ObjectAlignment::ObjectAlignmentNone;
      pathAdjustmentInterval = -1;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      graphSharedMemorySize  = -1;
      treeSharedMemorySize   = -1;
      objectSharedMemorySize = -1;
#endif
      prefetchOffset                = -1;
      prefetchSize                  = -1;
      accuracyTable                 = "";
      maxMagnitude                  = -1;
      quantizationScale             = -1.0;
      quantizationOffset            = -1.0;
      clippingRate                  = -1.0;
      nOfNeighborsForInsertionOrder = -1;
      epsilonForInsertionOrder      = -1;
      leafNodeSize                  = -1;
      internalChildrenSize          = -1;
    }

    void exportProperty(NGT::PropertySet &p) {
      p.set("Dimension", dimension);
      p.set("ThreadPoolSize", threadPoolSize);
      switch (objectType) {
      case ObjectSpace::ObjectType::Uint8: p.set("ObjectType", "Integer-1"); break;
      case ObjectSpace::ObjectType::Float: p.set("ObjectType", "Float-4"); break;
#ifdef NGT_HALF_FLOAT
      case ObjectSpace::ObjectType::Float16: p.set("ObjectType", "Float-2"); break;
#endif
      case ObjectSpace::ObjectType::Qsuint8: p.set("ObjectType", "QSUInteger-8B"); break;
#ifdef NGT_BFLOAT
      case ObjectSpace::ObjectType::Bfloat16: p.set("ObjectType", "Bfloat-2"); break;
#endif
#ifdef NGT_PQ4
      case ObjectSpace::ObjectType::Qint4: p.set("ObjectType", "QInteger-4B"); break;
#endif
      default: std::cerr << "Fatal error. Invalid object type. " << objectType << std::endl; abort();
      }
#ifdef NGT_REFINEMENT
      switch (refinementObjectType) {
      case ObjectSpace::ObjectType::Uint8: p.set("RefinementObjectType", "Integer-1"); break;
      case ObjectSpace::ObjectType::Float: p.set("RefinementObjectType", "Float-4"); break;
#ifdef NGT_HALF_FLOAT
      case ObjectSpace::ObjectType::Float16: p.set("RefinementObjectType", "Float-2"); break;
#endif
#ifdef NGT_BFLOAT
      case ObjectSpace::ObjectType::Bfloat16: p.set("RefinementObjectType", "Bfloat-2"); break;
#endif
      default:
        std::cerr << "Fatal error. Invalid refinement object type. " << refinementObjectType << std::endl;
        abort();
      }
#endif
      switch (distanceType) {
      case DistanceType::DistanceTypeNone: p.set("DistanceType", "None"); break;
      case DistanceType::DistanceTypeL1: p.set("DistanceType", "L1"); break;
      case DistanceType::DistanceTypeL2: p.set("DistanceType", "L2"); break;
      case DistanceType::DistanceTypeHamming: p.set("DistanceType", "Hamming"); break;
      case DistanceType::DistanceTypeJaccard: p.set("DistanceType", "Jaccard"); break;
      case DistanceType::DistanceTypeSparseJaccard: p.set("DistanceType", "SparseJaccard"); break;
      case DistanceType::DistanceTypeAngle: p.set("DistanceType", "Angle"); break;
      case DistanceType::DistanceTypeCosine: p.set("DistanceType", "Cosine"); break;
      case DistanceType::DistanceTypeNormalizedAngle: p.set("DistanceType", "NormalizedAngle"); break;
      case DistanceType::DistanceTypeNormalizedCosine: p.set("DistanceType", "NormalizedCosine"); break;
      case DistanceType::DistanceTypeNormalizedL2: p.set("DistanceType", "NormalizedL2"); break;
      case DistanceType::DistanceTypeInnerProduct: p.set("DistanceType", "InnerProduct"); break;
      case DistanceType::DistanceTypePoincare: p.set("DistanceType", "Poincare"); break; // added by Nyapicom
      case DistanceType::DistanceTypeLorentz: p.set("DistanceType", "Lorentz"); break;   // added by Nyapicom
      default: std::cerr << "Fatal error. Invalid distance type. " << distanceType << std::endl; abort();
      }
      switch (indexType) {
      case IndexType::GraphAndTree: p.set("IndexType", "GraphAndTree"); break;
      case IndexType::Graph: p.set("IndexType", "Graph"); break;
      default: std::cerr << "Fatal error. Invalid index type. " << indexType << std::endl; abort();
      }
      switch (databaseType) {
      case DatabaseType::Memory: p.set("DatabaseType", "Memory"); break;
      case DatabaseType::MemoryMappedFile: p.set("DatabaseType", "MemoryMappedFile"); break;
      default: std::cerr << "Fatal error. Invalid database type. " << databaseType << std::endl; abort();
      }
      switch (objectAlignment) {
      case ObjectAlignment::ObjectAlignmentNone: p.set("ObjectAlignment", "None"); break;
      case ObjectAlignment::ObjectAlignmentTrue: p.set("ObjectAlignment", "True"); break;
      case ObjectAlignment::ObjectAlignmentFalse: p.set("ObjectAlignment", "False"); break;
      default: std::cerr << "Fatal error. Invalid objectAlignment. " << objectAlignment << std::endl; abort();
      }
      p.set("PathAdjustmentInterval", pathAdjustmentInterval);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      p.set("GraphSharedMemorySize", graphSharedMemorySize);
      p.set("TreeSharedMemorySize", treeSharedMemorySize);
      p.set("ObjectSharedMemorySize", objectSharedMemorySize);
#endif
      p.set("PrefetchOffset", prefetchOffset);
      p.set("PrefetchSize", prefetchSize);
      p.set("AccuracyTable", accuracyTable);
      p.set("MaxMagnitude", maxMagnitude);
      p.set("QuantizationScale", quantizationScale);
      p.set("QuantizationOffset", quantizationOffset);
      p.set("QuantizationClippingRate", clippingRate);
      p.set("NumberOfNeighborsForInsertionOrder", nOfNeighborsForInsertionOrder);
      p.set("EpsilonForInsertionOrder", epsilonForInsertionOrder);
      p.set("LeafNodeSize", leafNodeSize);
      p.set("InternalChildrenSize", internalChildrenSize);
    }

    void importProperty(NGT::PropertySet &p) {
      setDefault();
      dimension                = p.getl("Dimension", dimension);
      threadPoolSize           = p.getl("ThreadPoolSize", threadPoolSize);
      PropertySet::iterator it = p.find("ObjectType");
      if (it != p.end()) {
        if (it->second == "Float-4") {
          objectType = ObjectSpace::ObjectType::Float;
        } else if (it->second == "Integer-1") {
          objectType = ObjectSpace::ObjectType::Uint8;
#ifdef NGT_HALF_FLOAT
        } else if (it->second == "Float-2") {
          objectType = ObjectSpace::ObjectType::Float16;
#endif
        } else if (it->second == "QSUInteger-8B") {
          objectType = ObjectSpace::ObjectType::Qsuint8;
#ifdef NGT_BFLOAT
        } else if (it->second == "Bfloat-2") {
          objectType = ObjectSpace::ObjectType::Bfloat16;
#endif
#ifdef NGT_PQ4
        } else if (it->second == "QInteger-4B") {
          objectType = ObjectSpace::ObjectType::Qint4;
#endif
        } else {
          std::cerr << "Invalid object type in the property. " << it->first << ":" << it->second << std::endl;
        }
      } else {
        std::cerr << "Not found \"ObjectType\"" << std::endl;
      }
#ifdef NGT_REFINEMENT
      {
        PropertySet::iterator it = p.find("RefinementObjectType");
        if (it != p.end()) {
          if (it->second == "Float-4") {
            refinementObjectType = ObjectSpace::ObjectType::Float;
          } else if (it->second == "Integer-1") {
            refinementObjectType = ObjectSpace::ObjectType::Uint8;
#ifdef NGT_HALF_FLOAT
          } else if (it->second == "Float-2") {
            refinementObjectType = ObjectSpace::ObjectType::Float16;
#endif
#ifdef NGT_BFLOAT
          } else if (it->second == "Bfloat-2") {
            refinementObjectType = ObjectSpace::ObjectType::Bfloat16;
#endif
          } else {
            std::cerr << "Invalid object type for refinement in the property. " << it->first << ":"
                      << it->second << std::endl;
          }
        } else {
          std::cerr << "Not found \"RefinementObjectType\"" << std::endl;
        }
      }
#endif
      it = p.find("DistanceType");
      if (it != p.end()) {
        if (it->second == "None") {
          distanceType = DistanceType::DistanceTypeNone;
        } else if (it->second == "L1") {
          distanceType = DistanceType::DistanceTypeL1;
        } else if (it->second == "L2") {
          distanceType = DistanceType::DistanceTypeL2;
        } else if (it->second == "Hamming") {
          distanceType = DistanceType::DistanceTypeHamming;
        } else if (it->second == "Jaccard") {
          distanceType = DistanceType::DistanceTypeJaccard;
        } else if (it->second == "SparseJaccard") {
          distanceType = DistanceType::DistanceTypeSparseJaccard;
        } else if (it->second == "Angle") {
          distanceType = DistanceType::DistanceTypeAngle;
        } else if (it->second == "Cosine") {
          distanceType = DistanceType::DistanceTypeCosine;
        } else if (it->second == "Poincare") { // added by Nyapicom
          distanceType = DistanceType::DistanceTypePoincare;
        } else if (it->second == "Lorentz") { // added by Nyapicom
          distanceType = DistanceType::DistanceTypeLorentz;
        } else if (it->second == "NormalizedAngle") {
          distanceType = DistanceType::DistanceTypeNormalizedAngle;
        } else if (it->second == "NormalizedCosine") {
          distanceType = DistanceType::DistanceTypeNormalizedCosine;
        } else if (it->second == "NormalizedL2") {
          distanceType = DistanceType::DistanceTypeNormalizedL2;
        } else if (it->second == "InnerProduct") {
          distanceType = DistanceType::DistanceTypeInnerProduct;
        } else {
          std::cerr << "Invalid Distance Type in the property. " << it->first << ":" << it->second
                    << std::endl;
        }
      } else {
        std::cerr << "Not found \"DistanceType\"" << std::endl;
      }
      it = p.find("IndexType");
      if (it != p.end()) {
        if (it->second == "GraphAndTree") {
          indexType = IndexType::GraphAndTree;
        } else if (it->second == "Graph") {
          indexType = IndexType::Graph;
        } else {
          std::cerr << "Invalid Index Type in the property. " << it->first << ":" << it->second << std::endl;
        }
      } else {
        std::cerr << "Not found \"IndexType\"" << std::endl;
      }
      it = p.find("DatabaseType");
      if (it != p.end()) {
        if (it->second == "Memory") {
          databaseType = DatabaseType::Memory;
        } else if (it->second == "MemoryMappedFile") {
          databaseType = DatabaseType::MemoryMappedFile;
        } else {
          std::cerr << "Invalid Database Type in the property. " << it->first << ":" << it->second
                    << std::endl;
        }
      } else {
        std::cerr << "Not found \"DatabaseType\"" << std::endl;
      }
      it = p.find("ObjectAlignment");
      if (it != p.end()) {
        if (it->second == "None") {
          objectAlignment = ObjectAlignment::ObjectAlignmentNone;
        } else if (it->second == "True") {
          objectAlignment = ObjectAlignment::ObjectAlignmentTrue;
        } else if (it->second == "False") {
          objectAlignment = ObjectAlignment::ObjectAlignmentFalse;
        } else {
          std::cerr << "Invalid Object Alignment in the property. " << it->first << ":" << it->second
                    << std::endl;
        }
      } else {
        std::cerr << "Not found \"ObjectAlignment\"" << std::endl;
        objectAlignment = ObjectAlignment::ObjectAlignmentFalse;
      }
      pathAdjustmentInterval = p.getl("PathAdjustmentInterval", pathAdjustmentInterval);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      graphSharedMemorySize  = p.getl("GraphSharedMemorySize", graphSharedMemorySize);
      treeSharedMemorySize   = p.getl("TreeSharedMemorySize", treeSharedMemorySize);
      objectSharedMemorySize = p.getl("ObjectSharedMemorySize", objectSharedMemorySize);
#endif
      prefetchOffset = p.getl("PrefetchOffset", prefetchOffset);
      prefetchSize   = p.getl("PrefetchSize", prefetchSize);
      it             = p.find("AccuracyTable");
      if (it != p.end()) {
        accuracyTable = it->second;
      }
      it = p.find("SearchType");
      if (it != p.end()) {
        searchType = it->second;
      }
      maxMagnitude       = p.getf("MaxMagnitude", maxMagnitude);
      quantizationScale  = p.getf("QuantizationScale", quantizationScale);
      quantizationOffset = p.getf("QuantizationOffset", quantizationOffset);
      clippingRate       = p.getf("QuantizationClippingRate", clippingRate);
      nOfNeighborsForInsertionOrder =
          p.getl("NumberOfNeighborsForInsertionOrder", nOfNeighborsForInsertionOrder);
      epsilonForInsertionOrder = p.getf("EpsilonForInsertionOrder", epsilonForInsertionOrder);
      leafNodeSize             = p.getl("LeafNodeSize", leafNodeSize);
      internalChildrenSize     = p.getl("InternalChildrenSize", internalChildrenSize);
    }

    void set(NGT::Property &prop);
    void get(NGT::Property &prop);
    int dimension;
    int threadPoolSize;
    ObjectSpace::ObjectType objectType;
    DistanceType distanceType;
    IndexType indexType;
    DatabaseType databaseType;
    ObjectAlignment objectAlignment;
    int pathAdjustmentInterval;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    int graphSharedMemorySize;
    int treeSharedMemorySize;
    int objectSharedMemorySize;
#endif
    int prefetchOffset;
    int prefetchSize;
    std::string accuracyTable;
    std::string searchType; // test
    float maxMagnitude;
    float quantizationScale;
    float quantizationOffset;
    float clippingRate;
    int nOfNeighborsForInsertionOrder;
    float epsilonForInsertionOrder;
#ifdef NGT_REFINEMENT
    ObjectSpace::ObjectType refinementObjectType;
#endif
    int leafNodeSize;
    int internalChildrenSize;
  };

  class InsertionOrder : public std::vector<uint32_t> {
   public:
    InsertionOrder() : nOfNeighboringNodes(50), epsilon(0.1), nOfThreads(0), indegreeOrder(false) {}
    ObjectID getID(ObjectID id) {
      if (id > size()) {
        std::stringstream msg;
        msg << "InsertionOrder::getID: Invalid ID. " << size() << ":" << id;
        NGTThrowException(msg);
      }
      return at(id - 1);
    }
    size_t nOfNeighboringNodes;
    float epsilon;
    size_t nOfThreads;
    bool indegreeOrder;
  };

  class InsertionResult {
   public:
    InsertionResult() : id(0), identical(false), distance(0.0) {}
    InsertionResult(size_t i, bool tf, Distance d) : id(i), identical(tf), distance(d) {}
    size_t id;
    bool identical;
    Distance distance; // the distance between the centroid and the inserted object.
  };

  class AccuracyTable {
   public:
    AccuracyTable() {};
    AccuracyTable(std::vector<std::pair<float, double>> &t) { set(t); }
    AccuracyTable(std::string str) { set(str); }
    void set(std::vector<std::pair<float, double>> &t) { table = t; }
    void set(std::string str) {
      std::vector<std::string> tokens;
      Common::tokenize(str, tokens, ",");
      if (tokens.size() < 2) {
        return;
      }
      for (auto i = tokens.begin(); i != tokens.end(); ++i) {
        std::vector<std::string> ts;
        Common::tokenize(*i, ts, ":");
        if (ts.size() != 2) {
          std::stringstream msg;
          msg << "AccuracyTable: Invalid accuracy table string " << *i << ":" << str;
          NGTThrowException(msg);
        }
        table.push_back(std::make_pair(Common::strtod(ts[0]), Common::strtod(ts[1])));
      }
    }

    float getEpsilon(double accuracy) {
      if (table.size() <= 2) {
        std::stringstream msg;
        msg << "AccuracyTable: The accuracy table is not set yet. The table size=" << table.size();
        NGTThrowException(msg);
      }
      if (accuracy > 1.0) {
        accuracy = 1.0;
      }
      std::pair<float, double> lower, upper;
      {
        auto i = table.begin();
        for (; i != table.end(); ++i) {
          if ((*i).second >= accuracy) {
            break;
          }
        }
        if (table.end() == i) {
          i -= 2;
        } else if (table.begin() != i) {
          i--;
        }
        lower = *i++;
        upper = *i;
      }
      float e = lower.first +
                (upper.first - lower.first) * (accuracy - lower.second) / (upper.second - lower.second);
      if (e < -0.9) {
        e = -0.9;
      }
      return e;
    }

    std::string getString() {
      std::stringstream str;
      for (auto i = table.begin(); i != table.end(); ++i) {
        str << (*i).first << ":" << (*i).second;
        if (i + 1 != table.end()) {
          str << ",";
        }
      }
      return str.str();
    }
    std::vector<std::pair<float, double>> table;
  };

  Index() : index(0), redirect(false) {
#if defined(NGT_AVX2)
    if (!CpuInfo::isAVX2()) {
      std::stringstream msg;
      msg << "NGT::Index: Fatal Error!. Despite that this NGT library is built with AVX2, this CPU doesn't "
             "support AVX2. This CPU supoorts "
          << CpuInfo::getSupportedSimdTypes();
      NGTThrowException(msg);
    }
#elif defined(NGT_AVX512)
    if (!CpuInfo::isAVX512()) {
      std::stringstream msg;
      msg << "NGT::Index: Fatal Error!. Despite that this NGT library is built with AVX512, this CPU doesn't "
             "support AVX512. This CPU supoorts "
          << CpuInfo::getSupportedSimdTypes();
      NGTThrowException(msg);
    }
#endif
  }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  Index(NGT::Property &prop, const std::string &database);
#else
  Index(NGT::Property &prop);
#endif
  Index(const std::string &database, bool rdOnly = false, Index::OpenType openType = Index::OpenTypeNone)
      : index(0), redirect(false) {
    open(database, rdOnly, openType);
  }
  Index(const std::string &database, NGT::Property &prop) : index(0), redirect(false) {
    open(database, prop);
  }
  virtual ~Index() { close(); }

  void open(const std::string &database, NGT::Property &prop) {
    open(database);
    setProperty(prop);
  }
  void open(const std::string &database, bool rdOnly = false, Index::OpenType openType = OpenTypeNone);

  void close() {
    if (index != 0) {
      delete index;
      index = 0;
    }
    path.clear();
  }
  void save() {
    if (path.empty()) {
      NGTThrowException("NGT::Index::saveIndex: path is empty");
    }
    saveIndex(path);
  }
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
  void save(std::string indexPath) { saveIndex(indexPath); }
#endif
  static void mkdir(const std::string &dir) {
    if (::mkdir(dir.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) != 0) {
      std::stringstream msg;
      msg << "NGT::Index::mkdir: Cannot make the specified directory. " << dir;
      NGTThrowException(msg);
    }
  }
  static void create(const std::string &database, NGT::Property &prop, bool redirect = false) {
    createGraphAndTree(database, prop, redirect);
  }
  static void createGraphAndTree(const std::string &database, NGT::Property &prop,
                                 const std::string &dataFile, size_t dataSize = 0, bool redirect = false);
  static void createGraphAndTree(const std::string &database, NGT::Property &prop, bool redirect = false) {
    createGraphAndTree(database, prop, "", redirect);
  }
  static void create(const std::string &index, const std::string &srcIndex, NGT::Property *updateProp,
                     bool redirect = false);
  static void create(const std::string &index, const std::string &srcIndex, bool redirect = false) {
    create(index, srcIndex, nullptr, redirect);
  }
  static void create(const std::string &index, const std::string &srcIndex, NGT::Property &updateProp,
                     bool redirect = false) {
    create(index, srcIndex, &updateProp, redirect);
  }
  static void createGraph(const std::string &database, NGT::Property &prop, const std::string &dataFile,
                          size_t dataSize = 0, bool redirect = false);
  template <typename T> size_t insert(const std::vector<T> &object);
  template <typename T> size_t insert(ObjectID id, const std::vector<T> &object);
  template <typename T> size_t append(const std::vector<T> &object);
  template <typename T> void update(ObjectID id, const std::vector<T> &object);
#ifdef NGT_REFINEMENT
  template <typename T> size_t appendToRefinement(const std::vector<T> &object);
  template <typename T> size_t insertToRefinement(const std::vector<T> &object);
  template <typename T> void updateToRefinement(ObjectID id, const std::vector<T> &object);
#endif
  static void append(const std::string &index, const std::string &dataFile, size_t threadSize,
                     size_t dataSize);
  static void append(const std::string &index, const float *data, size_t dataSize, size_t threadSize);
  static void appendFromRefinementObjectFile(const std::string &index, size_t threadSize = 0);
  void appendFromRefinementObjectFile();
  void insertFromRefinementObjectFile();
  static void appendFromTextObjectFile(const std::string &index, const std::string &data, size_t dataSize,
                                       bool append = true, bool refinement = false, size_t threadSize = 0);
  void appendFromTextObjectFile(const std::string &data, size_t dataSize, bool append = true,
                                bool refinement = false);
  static void appendFromBinaryObjectFile(const std::string &index, const std::string &data, size_t dataSize,
                                         bool append = true, bool refinement = false, size_t threadSize = 0);
  void appendFromBinaryObjectFile(const std::string &data, size_t dataSize, bool apend = true,
                                  bool refinement = false);
  static void remove(const std::string &database, std::vector<ObjectID> &objects, bool force = false);
  static void exportIndex(const std::string &database, const std::string &file);
  static void importIndex(const std::string &database, const std::string &file);
  virtual void load(const std::string &ifile, size_t dataSize) { getIndex().load(ifile, dataSize); }
  virtual void append(const std::string &ifile, size_t dataSize) { getIndex().append(ifile, dataSize); }
  template <typename T>
  void appendWithPreprocessing(const T *data, size_t dataSize, bool append = true, bool refinement = false);
  template <typename T>
  void append(const T *data, size_t dataSize, bool append = true, bool refinement = false);
  virtual size_t getNumberOfObjects() { return getIndex().getNumberOfObjects(); }
  virtual size_t getNumberOfIndexedObjects() { return getIndex().getNumberOfIndexedObjects(); }
  virtual size_t getObjectRepositorySize() { return getIndex().getObjectRepositorySize(); }
  virtual size_t getGraphRepositorySize() { return getIndex().getGraphRepositorySize(); }
  void createIndex(size_t threadNumber = 0, size_t sizeOfRepository = 0);
  virtual void createIndexWithInsertionOrder(InsertionOrder &insertionOrder, size_t threadNumber = 0,
                                             size_t sizeOfRepository = 0) {
    StdOstreamRedirector redirector(redirect);
    redirector.begin();
    try {
      getIndex().createIndexWithInsertionOrder(insertionOrder, threadNumber, sizeOfRepository);
    } catch (Exception &err) {
      redirector.end();
      throw err;
    }
    redirector.end();
  }
  virtual void saveIndex(const std::string &ofile) { getIndex().saveIndex(ofile); }
  virtual void loadIndex(const std::string &ofile) { getIndex().loadIndex(ofile); }
  virtual void clearIndex() { getIndex().clearIndex(); }
  virtual Object *allocateObject(const std::string &textLine, const std::string &sep) {
    return getIndex().allocateObject(textLine, sep);
  }
  virtual Object *allocateObject(const std::vector<double> &obj) { return getIndex().allocateObject(obj); }
  virtual Object *allocateObject(const std::vector<float> &obj) { return getIndex().allocateObject(obj); }
  virtual Object *allocateObject(const std::vector<uint8_t> &obj) { return getIndex().allocateObject(obj); }
#ifdef NGT_HALF_FLOAT
  virtual Object *allocateObject(const std::vector<float16> &obj) { return getIndex().allocateObject(obj); }
#endif
  virtual Object *allocateObject(const float *obj, size_t size) {
    return getIndex().allocateObject(obj, size);
  }
  virtual size_t getSizeOfElement() { return getIndex().getSizeOfElement(); }
  virtual void setProperty(NGT::Property &prop) { getIndex().setProperty(prop); }
  virtual void getProperty(NGT::Property &prop) { getIndex().getProperty(prop); }
  virtual void deleteObject(Object *po) { getIndex().deleteObject(po); }
  virtual void linearSearch(NGT::SearchContainer &sc) { getIndex().linearSearch(sc); }
  virtual void linearSearch(NGT::SearchQuery &sc) { getIndex().linearSearch(sc); }
  virtual void search(NGT::SearchContainer &sc) { getIndex().search(sc); }
  virtual void search(NGT::SearchQuery &sc) { getIndex().search(sc); }
  virtual void search(NGT::SearchContainer &sc, ObjectDistances &seeds) { getIndex().search(sc, seeds); }
  virtual void getSeeds(NGT::SearchContainer &sc, ObjectDistances &seeds, size_t n) {
    getIndex().getSeeds(sc, seeds, n);
  }
  virtual void remove(ObjectID id, bool force = false) {
    try {
      getRefinementObjectSpace().remove(id);
    } catch (...) {
    }
    getIndex().remove(id, force);
  }
  virtual void exportIndex(const std::string &file) { getIndex().exportIndex(file); }
  virtual void importIndex(const std::string &file) { getIndex().importIndex(file); }
  virtual bool verify(std::vector<uint8_t> &status, bool info = false, char mode = '-') {
    return getIndex().verify(status, info, mode);
  }
  virtual ObjectSpace &getObjectSpace() { return getIndex().getObjectSpace(); }
#ifdef NGT_REFINEMENT
  virtual ObjectSpace &getRefinementObjectSpace() { return getIndex().getRefinementObjectSpace(); }
#endif
  virtual size_t getSharedMemorySize(std::ostream &os, SharedMemoryAllocator::GetMemorySizeType t =
                                                           SharedMemoryAllocator::GetTotalMemorySize) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    size_t osize = getObjectSpace().getRepository().getAllocator().getMemorySize(t);
#else
    size_t osize = 0;
#endif
    os << "object=" << osize << std::endl;
    size_t isize = getIndex().getSharedMemorySize(os, t);
    return osize + isize;
  }
  float getEpsilonFromExpectedAccuracy(double accuracy);
  void searchUsingOnlyGraph(NGT::SearchContainer &sc);
  void searchUsingOnlyGraph(NGT::SearchQuery &searchQuery);
  std::vector<float> makeSparseObject(std::vector<uint32_t> &object);
  Index &getIndex() {
    if (index == 0) {
      NGTThrowException("NGT::Index::getIndex: Index is unavailable.");
    }
    return *index;
  }
  void enableLog() { redirect = false; }
  void disableLog() { redirect = true; }

  void extractInsertionOrder(InsertionOrder &insertionOrder);
  void setQuantizationFromMaxMin(float max, float min);
  void setQuantization(float scale, float offset);
  static void destroy(const std::string &path) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    std::remove(std::string(path + "/grp").c_str());
    std::remove(std::string(path + "/grpc").c_str());
    std::remove(std::string(path + "/trei").c_str());
    std::remove(std::string(path + "/treic").c_str());
    std::remove(std::string(path + "/trel").c_str());
    std::remove(std::string(path + "/trelc").c_str());
    std::remove(std::string(path + "/objpo").c_str());
    std::remove(std::string(path + "/objpoc").c_str());
#else
    std::remove(std::string(path + "/grp").c_str());
    std::remove(std::string(path + "/tre").c_str());
    std::remove(std::string(path + "/obj").c_str());
#endif
    std::remove(std::string(path + "/prf").c_str());
    std::remove(path.c_str());
  }

  static void version(std::ostream &os);
  static std::string getVersion();
  std::string getPath() { return path; }
  size_t getDimension();

 protected:
  Object *allocateQuery(NGT::QueryContainer &queryContainer) {
    auto *vec = queryContainer.getQuery();
    if (vec == 0) {
      std::stringstream msg;
      msg << "NGT::Index::allocateObject: Object is not set. ";
      NGTThrowException(msg);
    }
    Object *object   = 0;
    auto &objectType = queryContainer.getQueryType();
    if (objectType == typeid(float)) {
      object = allocateObject(*static_cast<std::vector<float> *>(vec));
    } else if (objectType == typeid(double)) {
      object = allocateObject(*static_cast<std::vector<double> *>(vec));
    } else if (objectType == typeid(uint8_t)) {
      object = allocateObject(*static_cast<std::vector<uint8_t> *>(vec));
#ifdef NGT_HALF_FLOAT
    } else if (objectType == typeid(float16)) {
      object = allocateObject(*static_cast<std::vector<float16> *>(vec));
#endif
    } else {
      std::stringstream msg;
      msg << "NGT::Index::allocateObject: Unavailable object type.";
      NGTThrowException(msg);
    }
    return object;
  }

  static void loadAndCreateIndex(Index &index, const std::string &database, const std::string &dataFile,
                                 size_t threadSize, size_t dataSize);

  Index *index;
  std::string path;
  bool redirect;
};

class GraphIndex : public Index, public NeighborhoodGraph {
 public:
  class GraphStatistics {
   public:
    size_t getNumberOfObjects() const { return numberOfObjects; }
    size_t getNumberOfIndexedObjects() const { return numberOfIndexedObjects; }
    size_t getSizeOfObjectRepository() const { return sizeOfObjectRepository; }
    size_t getSizeOfRefinementObjectRepository() const { return sizeOfRefinementObjectRepository; }
    size_t getNumberOfRemovedObjects() const { return numberOfRemovedObjects; }
    size_t getNumberOfNodes() const { return numberOfNodes; }
    size_t getNumberOfEdges() const { return numberOfEdges; }
    long double getMeanEdgeLength() const { return meanEdgeLength; }
    double getMeanNumberOfEdgesPerNode() const { return meanNumberOfEdgesPerNode; }
    size_t getNumberOfNodesWithoutEdges() const { return numberOfNodesWithoutEdges; }
    size_t getMaxNumberOfOutdegree() const { return maxNumberOfOutdegree; }
    size_t getMinNumberOfOutdegree() const { return minNumberOfOutdegree; }
    size_t getNumberOfNodesWithoutIndegree() const { return numberOfNodesWithoutIndegree; }
    size_t getMaxNumberOfIndegree() const { return maxNumberOfIndegree; }
    size_t getMinNumberOfIndegree() const { return minNumberOfIndegree; }
    long double getMeanEdgeLengthFor10Edges() const { return meanEdgeLengthFor10Edges; }
    size_t getNodesSkippedFor10Edges() const { return nodesSkippedFor10Edges; }
    long double getMeanIndegreeDistanceFor10Edges() const { return meanIndegreeDistanceFor10Edges; }
    size_t getNodesSkippedForIndegreeDistance() const { return nodesSkippedForIndegreeDistance; }
    double getVarianceOfOutdegree() const { return varianceOfOutdegree; }
    double getVarianceOfIndegree() const { return varianceOfIndegree; }
    int getMedianOutdegree() const { return medianOutdegree; }
    size_t getModeOutdegree() const { return modeOutdegree; }
    double getC95Outdegree() const { return c95Outdegree; }
    double getC99Outdegree() const { return c99Outdegree; }
    int getMedianIndegree() const { return medianIndegree; }
    size_t getModeIndegree() const { return modeIndegree; }
    double getC5Indegree() const { return c5Indegree; }
    double getC1Indegree() const { return c1Indegree; }
    std::vector<int64_t> getIndegreeCount() const { return indegreeCount; }
    std::vector<size_t> getOutdegreeHistogram() const { return outdegreeHistogram; }
    std::vector<size_t> getIndegreeHistogram() const { return indegreeHistogram; }
    bool isValid() const { return valid; }

    std::string toString(NGT::GraphIndex &outGraph, char mode) const {
      std::ostringstream oss;
      oss << "Graph Statistics:" << std::endl;
      oss << "Number of objects:\t" << numberOfObjects << std::endl;
      oss << "Number of indexed objects:\t" << numberOfIndexedObjects << std::endl;
      oss << "Size of object repository (not the number of the objects):\t" << sizeOfObjectRepository
          << std::endl;
#ifdef NGT_REFINEMENT
      oss << "Size of refinement object repository (not the number of the objects):\t"
          << sizeOfRefinementObjectRepository << std::endl;
#endif
      oss << "Number of removed objects:\t" << numberOfRemovedObjects << std::endl;
      oss << "Number of nodes:\t" << numberOfNodes << std::endl;
      oss << "Number of edges:\t" << numberOfEdges << std::endl;
      oss << "Mean edge length:\t" << std::setprecision(10) << meanEdgeLength << std::endl;
      oss << "Mean number of edges per node:\t" << meanNumberOfEdgesPerNode << std::endl;
      oss << "Number of nodes without edges:\t" << numberOfNodesWithoutEdges << std::endl;
      oss << "Maximum outdegree:\t" << maxNumberOfOutdegree << std::endl;
      if (minNumberOfOutdegree == static_cast<size_t>(-1)) {
        oss << "Minimum outdegree:\t-NA-" << std::endl;
      } else {
        oss << "Minimum outdegree:\t" << minNumberOfOutdegree << std::endl;
      }
      oss << "Number of nodes without indegree:\t" << numberOfNodesWithoutIndegree << std::endl;
      oss << "Maximum indegree:\t" << maxNumberOfIndegree << std::endl;
      if (minNumberOfIndegree == static_cast<size_t>(-1)) {
        oss << "Minimum indegree:\t-NA-" << std::endl;
      } else {
        oss << "Minimum indegree:\t" << minNumberOfIndegree << std::endl;
      }
      oss << "Mean edge length for the first 10 edges:\t" << meanEdgeLengthFor10Edges << std::endl;
      oss << "Number of nodes skipped for first 10 edges:\t" << nodesSkippedFor10Edges << std::endl;
      oss << "Mean indegree distance for the first 10 edges:\t" << meanIndegreeDistanceFor10Edges
          << std::endl;
      oss << "Number of nodes skipped for indegree distance:\t" << nodesSkippedForIndegreeDistance
          << std::endl;
      oss << "Variance of outdegree:\t" << varianceOfOutdegree << std::endl;
      oss << "Variance of indegree:\t" << varianceOfIndegree << std::endl;
      oss << "Median outdegree:\t" << medianOutdegree << std::endl;
      oss << "Mode outdegree:\t" << modeOutdegree << std::endl;
      oss << "95th percentile of outdegree:\t" << c95Outdegree << std::endl;
      oss << "99th percentile of outdegree:\t" << c99Outdegree << std::endl;
      oss << "Median indegree:\t" << medianIndegree << std::endl;
      oss << "Mode indegree:\t" << modeIndegree << std::endl;
      oss << "5th percentile of indegree:\t" << c5Indegree << std::endl;
      oss << "1st percentile of indegree:\t" << c1Indegree << std::endl;
      if (mode == 'h') {
        oss << "#\tout\tin" << std::endl;
        for (size_t i = 0; i < outdegreeHistogram.size() || i < indegreeHistogram.size(); i++) {
          size_t out = outdegreeHistogram.size() <= i ? 0 : outdegreeHistogram[i];
          size_t in  = indegreeHistogram.size() <= i ? 0 : indegreeHistogram[i];
          oss << i << "\t" << out << "\t" << in << std::endl;
        }
      } else if (mode == 'p') {
        oss << "ID\toutdegree\tindegree" << std::endl;
        NGT::GraphRepository &graph = outGraph.repository;
        for (size_t id = 1; id < graph.size(); id++) {
          oss << id << "\t" << outGraph.getNode(id)->size() << "\t" << indegreeCount[id] << std::endl;
        }
      }
      oss << "Is valid:\t" << (valid ? "True" : "False") << std::endl;
      return oss.str();
    }

   private:
    void setNumberOfObjects(size_t value) { numberOfObjects = value; }
    void setNumberOfIndexedObjects(size_t value) { numberOfIndexedObjects = value; }
    void setSizeOfObjectRepository(size_t value) { sizeOfObjectRepository = value; }
    void setSizeOfRefinementObjectRepository(size_t value) { sizeOfRefinementObjectRepository = value; }
    void setNumberOfRemovedObjects(size_t value) { numberOfRemovedObjects = value; }
    void setNumberOfNodes(size_t value) { numberOfNodes = value; }
    void setNumberOfEdges(size_t value) { numberOfEdges = value; }
    void setMeanEdgeLength(long double value) { meanEdgeLength = value; }
    void setMeanNumberOfEdgesPerNode(double value) { meanNumberOfEdgesPerNode = value; }
    void setNumberOfNodesWithoutEdges(size_t value) { numberOfNodesWithoutEdges = value; }
    void setMaxNumberOfOutdegree(size_t value) { maxNumberOfOutdegree = value; }
    void setMinNumberOfOutdegree(size_t value) { minNumberOfOutdegree = value; }
    void setNumberOfNodesWithoutIndegree(size_t value) { numberOfNodesWithoutIndegree = value; }
    void setMaxNumberOfIndegree(size_t value) { maxNumberOfIndegree = value; }
    void setMinNumberOfIndegree(size_t value) { minNumberOfIndegree = value; }
    void setMeanEdgeLengthFor10Edges(long double value) { meanEdgeLengthFor10Edges = value; }
    void setNodesSkippedFor10Edges(size_t value) { nodesSkippedFor10Edges = value; }
    void setMeanIndegreeDistanceFor10Edges(long double value) { meanIndegreeDistanceFor10Edges = value; }
    void setNodesSkippedForIndegreeDistance(size_t value) { nodesSkippedForIndegreeDistance = value; }
    void setVarianceOfOutdegree(double value) { varianceOfOutdegree = value; }
    void setVarianceOfIndegree(double value) { varianceOfIndegree = value; }
    void setMedianOutdegree(int value) { medianOutdegree = value; }
    void setModeOutdegree(size_t value) { modeOutdegree = value; }
    void setC95Outdegree(double value) { c95Outdegree = value; }
    void setC99Outdegree(double value) { c99Outdegree = value; }
    void setMedianIndegree(int value) { medianIndegree = value; }
    void setModeIndegree(size_t value) { modeIndegree = value; }
    void setC5Indegree(double value) { c5Indegree = value; }
    void setC1Indegree(double value) { c1Indegree = value; }
    void setIndegreeCount(std::vector<int64_t> &&value) { indegreeCount = std::move(value); }
    void setIndegreeHistogram(std::vector<size_t> &&value) { indegreeHistogram = std::move(value); }
    void setOutdegreeHistogram(std::vector<size_t> &&value) { outdegreeHistogram = std::move(value); }
    void setValid(bool value) { valid = value; }

    size_t numberOfObjects;
    size_t numberOfIndexedObjects;
    size_t sizeOfObjectRepository;
    size_t sizeOfRefinementObjectRepository;
    size_t numberOfRemovedObjects;
    size_t numberOfNodes;
    size_t numberOfEdges;
    long double meanEdgeLength;
    double meanNumberOfEdgesPerNode;
    size_t numberOfNodesWithoutEdges;
    size_t maxNumberOfOutdegree;
    size_t minNumberOfOutdegree;
    size_t numberOfNodesWithoutIndegree;
    size_t maxNumberOfIndegree;
    size_t minNumberOfIndegree;
    long double meanEdgeLengthFor10Edges;
    size_t nodesSkippedFor10Edges;
    long double meanIndegreeDistanceFor10Edges;
    size_t nodesSkippedForIndegreeDistance;
    double varianceOfOutdegree;
    double varianceOfIndegree;
    int medianOutdegree;
    size_t modeOutdegree;
    double c95Outdegree;
    double c99Outdegree;
    int medianIndegree;
    size_t modeIndegree;
    double c5Indegree;
    double c1Indegree;
    std::vector<int64_t> indegreeCount;
    std::vector<size_t> outdegreeHistogram;
    std::vector<size_t> indegreeHistogram;
    bool valid;

    friend class GraphIndex;
  };

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  GraphIndex(const std::string &allocator, bool rdOnly = false);
  GraphIndex(const std::string &allocator, NGT::Property &prop) : readOnly(false) {
    initialize(allocator, prop);
  }
  void initialize(const std::string &allocator, NGT::Property &prop);
#else // NGT_SHARED_MEMORY_ALLOCATOR
  GraphIndex(const std::string &database, bool rdOnly = false,
             Index::OpenType openType = Index::OpenTypeNone);
  GraphIndex(NGT::Property &prop) : readOnly(false) { initialize(prop); }

  void initialize(NGT::Property &prop) {
    constructObjectSpace(prop);
    setProperty(prop);
  }

#endif // NGT_SHARED_MEMORY_ALLOCATOR

  virtual ~GraphIndex() { destructObjectSpace(); }
  void constructObjectSpace(NGT::Property &prop);

  void destructObjectSpace() {
#ifdef NGT_REFINEMENT
    if (refinementObjectSpace != 0) {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
      refinementObjectSpace->deleteAll();
#endif
      delete refinementObjectSpace;
      refinementObjectSpace = 0;
    }
#endif
    if (objectSpace != 0) {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
      objectSpace->deleteAll();
#endif
      delete objectSpace;
      objectSpace = 0;
    }
  }

  virtual void load(const std::string &ifile, size_t dataSize = 0) {
    if (ifile.empty()) {
      return;
    }
    std::istream *is;
    std::ifstream *ifs = 0;
    if (ifile == "-") {
      is = &std::cin;
    } else {
      ifs = new std::ifstream;
      ifs->std::ifstream::open(ifile);
      if (!(*ifs)) {
        std::stringstream msg;
        msg << "Index::load: Cannot open the specified file. " << ifile;
        NGTThrowException(msg);
      }
      is = ifs;
    }
    try {
      objectSpace->readText(*is, dataSize);
    } catch (Exception &err) {
      if (ifile != "-") {
        delete ifs;
      }
      throw(err);
    }
    if (ifile != "-") {
      delete ifs;
    }
  }

  virtual void append(const std::string &ifile, size_t dataSize = 0) {
    if (ifile.empty()) {
      return;
    }
    std::istream *is;
    std::ifstream *ifs = 0;
    if (ifile == "-") {
      is = &std::cin;
    } else {
      ifs = new std::ifstream;
      ifs->std::ifstream::open(ifile);
      if (!(*ifs)) {
        std::stringstream msg;
        msg << "Index::load: Cannot open the specified file. " << ifile;
        NGTThrowException(msg);
      }
      is = ifs;
    }
    try {
      objectSpace->appendText(*is, dataSize);
    } catch (Exception &err) {
      if (ifile != "-") {
        delete ifs;
      }
      throw(err);
    }
    if (ifile != "-") {
      delete ifs;
    }
  }

  virtual void append(const float *data, size_t dataSize) { objectSpace->append(data, dataSize); }
  virtual void append(const double *data, size_t dataSize) { objectSpace->append(data, dataSize); }
  virtual void append(const uint8_t *data, size_t dataSize) { objectSpace->append(data, dataSize); }
#ifdef NGT_HALF_FLOAT
  virtual void append(const float16 *data, size_t dataSize) { objectSpace->append(data, dataSize); }
#endif
#ifdef NGT_REFINEMENT
  virtual void appendToRefinement(const float *data, size_t dataSize) {
    refinementObjectSpace->append(data, dataSize);
  }
  virtual void appendToRefinement(const double *data, size_t dataSize) {
    refinementObjectSpace->append(data, dataSize);
  }
  virtual void appendToRefinement(const uint8_t *data, size_t dataSize) {
    refinementObjectSpace->append(data, dataSize);
  }
#ifdef NGT_HALF_FLOAT
  virtual void appendToRefinement(const float16 *data, size_t dataSize) {
    refinementObjectSpace->append(data, dataSize);
  }
#endif
#endif

  void saveObjectRepository(const std::string &ofile) {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
    try {
      mkdir(ofile);
    } catch (...) {
    }
    if (objectSpace != 0) {
      objectSpace->serialize(ofile + "/obj");
    } else {
      std::cerr << "saveIndex::Warning! ObjectSpace is null. continue saving..." << std::endl;
    }
#ifdef NGT_REFINEMENT
    if (refinementObjectSpace != 0) {
      refinementObjectSpace->serialize(ofile + "/robj");
    }
#endif
#endif
  }

  void saveGraph(const std::string &ofile) {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
    std::string fname = ofile + "/grp";
    std::ofstream osg(fname);
    if (!osg.is_open()) {
      std::stringstream msg;
      msg << "saveIndex:: Cannot open. " << fname;
      NGTThrowException(msg);
    }
    repository.serialize(osg);
#endif
  }

  virtual void saveIndex(const std::string &ofile) {
    saveObjectRepository(ofile);
    saveGraph(ofile);
    saveProperty(ofile);
  }

  void saveProperty(const std::string &file);
  void exportProperty(const std::string &file);

  static void loadGraph(const std::string &ifile, NGT::GraphRepository &graph);
  virtual void loadIndex(const std::string &ifile, bool readOnly, NGT::Index::OpenType openType);

  virtual void clearIndex() {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    NGTThrowException("Not implemented");
#else
    repository.initialize();
#endif
  }

  virtual void exportIndex(const std::string &ofile) {
    try {
      mkdir(ofile);
    } catch (...) {
      std::stringstream msg;
      msg << "exportIndex:: Cannot make the directory. " << ofile;
      NGTThrowException(msg);
    }
    objectSpace->serializeAsText(ofile + "/obj");
    std::ofstream osg(ofile + "/grp");
    repository.serializeAsText(osg);
    exportProperty(ofile);
  }

  virtual void importIndex(const std::string &ifile) {
    objectSpace->deserializeAsText(ifile + "/obj");
    std::string fname = ifile + "/grp";
    std::ifstream isg(fname);
    if (!isg.is_open()) {
      std::stringstream msg;
      msg << "importIndex:: Cannot open. " << fname;
      NGTThrowException(msg);
    }
    repository.deserializeAsText(isg);
  }

  void linearSearch(NGT::SearchContainer &sc) {
    ResultSet results;
    objectSpace->linearSearch(sc.object, sc.radius, sc.size, results);
    ObjectDistances &qresults = sc.getResult();
    qresults.moveFrom(results);
  }

  void linearSearch(NGT::SearchQuery &searchQuery) {
    Object *query = Index::allocateQuery(searchQuery);
    try {
      NGT::SearchQuery sc(searchQuery, *query);
      GraphIndex::linearSearch(static_cast<SearchContainer &>(sc));
      searchQuery.distanceComputationCount = sc.distanceComputationCount;
      searchQuery.visitCount               = sc.visitCount;
    } catch (Exception &err) {
      deleteObject(query);
      throw err;
    }
    deleteObject(query);
  }

  // GraphIndex
  virtual void search(NGT::SearchContainer &sc) {
    sc.distanceComputationCount = 0;
    sc.visitCount               = 0;
    ObjectDistances seeds;
    search(sc, seeds);
  }

  // GraphIndex
  void search(NGT::SearchQuery &searchQuery) {
    NGT::Object *query = 0;
    try {
#ifdef NGT_PQ4
      if (getObjectSpace().pq4IsEnabled() && readOnly) {
        query = getObjectSpace().allocateQuantizedQuery(searchQuery.getQueryVector());
      } else {
        query = Index::allocateQuery(searchQuery);
      }
      NGT::SearchQuery sq(searchQuery, *query);
#else
      query = Index::allocateQuery(searchQuery);
      NGT::SearchContainer sq(searchQuery, *query);
#endif
#ifdef NGT_REFINEMENT
      auto expansion = searchQuery.getRefinementExpansion();
      if (expansion < 1.0 || !refinementObjectSpaceIsAvailable()) {
        GraphIndex::search(static_cast<NGT::SearchContainer &>(sq));
        searchQuery.workingResult = std::move(sq.workingResult);
      } else {
        size_t poffset = 12;
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
        size_t psize = 64;
#endif
        auto size = sq.size;
        sq.size *= expansion;
        try {
          GraphIndex::search(static_cast<NGT::SearchContainer &>(sq));
        } catch (Exception &err) {
          sq.size = size;
          throw err;
        }
        auto &ros            = getRefinementObjectSpace();
        auto &rrepo          = ros.getRepository();
        NGT::Object *robject = 0;
        if (searchQuery.getQueryType() == typeid(float)) {
          auto &v = *static_cast<std::vector<float> *>(searchQuery.getQuery());
          robject = ros.allocateNormalizedObject(v);
        } else if (searchQuery.getQueryType() == typeid(uint8_t)) {
          auto &v = *static_cast<std::vector<uint8_t> *>(searchQuery.getQuery());
          robject = ros.allocateNormalizedObject(v);
#ifdef NGT_HALF_FLOAT
        } else if (searchQuery.getQueryType() == typeid(float16)) {
          auto &v = *static_cast<std::vector<float16> *>(searchQuery.getQuery());
          robject = ros.allocateNormalizedObject(v);
#endif
        } else {
          std::stringstream msg;
          msg << "Invalid query object type.";
          NGTThrowException(msg);
        }
        sq.size          = size;
        auto &comparator = getRefinementObjectSpace().getComparator();
        if (sq.resultIsAvailable()) {
          auto &results = sq.getResult();
          for (auto &r : results) {
            r.distance = comparator(*robject, *rrepo.get(r.id));
          }
          std::sort(results.begin(), results.end());
          results.resize(size);
        } else {
          ObjectDistances rs;
          rs.resize(sq.workingResult.size());
          size_t counter = 0;
          while (!sq.workingResult.empty()) {
            if (counter < poffset) {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
              auto *ptr = rrepo.get(sq.workingResult.top().id)->getPointer();
              MemoryCache::prefetch(static_cast<uint8_t *>(ptr), psize);
#endif
            }
            rs[counter++].id = sq.workingResult.top().id;
            sq.workingResult.pop();
          }
          for (size_t idx = 0; idx < rs.size(); idx++) {
            if (idx + poffset < rs.size()) {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
              auto *ptr = rrepo.get(rs[idx + poffset].id)->getPointer();
              MemoryCache::prefetch(static_cast<uint8_t *>(ptr), psize);
#endif
            }
            auto &r    = rs[idx];
            r.distance = comparator(*robject, *rrepo.get(r.id));
            searchQuery.workingResult.emplace(r);
          }
          while (searchQuery.workingResult.size() > sq.size) {
            searchQuery.workingResult.pop();
          }
        }
        ros.deleteObject(robject);
      }
#else
      GraphIndex::search(static_cast<NGT::SearchContainer &>(sq));
      searchQuery.workingResult = std::move(s.workingResult);
#endif
      searchQuery.distanceComputationCount = sq.distanceComputationCount;
      searchQuery.visitCount               = sq.visitCount;
    } catch (Exception &err) {
#ifdef NGT_PQ4
      if (getObjectSpace().pq4IsEnabled() && readOnly) {
        getObjectSpace().getQuantizer().destructQueryObject(query->getPointer());
        query->detach();
      }
#endif
      deleteObject(query);
      throw err;
    }
#ifdef NGT_PQ4
    if (getObjectSpace().pq4IsEnabled() && readOnly) {
      getObjectSpace().getQuantizer().destructQueryObject(query->getPointer());
      query->detach();
    }
#endif
    deleteObject(query);
  }
  void getSeeds(NGT::SearchContainer &sc, ObjectDistances &seeds, size_t n) {
    getRandomSeeds(repository, seeds, n);
    setupDistances(sc, seeds);
    std::sort(seeds.begin(), seeds.end());
    if (seeds.size() > n) {
      seeds.resize(n);
    }
  }
  // get randomly nodes as seeds.
  template <class REPOSITORY> void getRandomSeeds(REPOSITORY &repo, ObjectDistances &seeds, size_t seedSize) {
    // clear all distances to find the same object as a randomized object.
    for (ObjectDistances::iterator i = seeds.begin(); i != seeds.end(); i++) {
      (*i).distance = 0.0;
    }
    size_t repositorySize = repo.size();
    repositorySize =
        repositorySize == 0 ? 0 : repositorySize - 1; // Because the head of repository is a dummy.
    seedSize = seedSize > repositorySize ? repositorySize : seedSize;
    std::vector<ObjectID> deteted;
    size_t emptyCount = 0;
#ifndef NGT_ENABLE_TIME_SEED_FOR_RANDOM
    if (seeds.size() != 0) {
      srand(seeds[0].id);
    }
#endif
    while (seedSize > seeds.size()) {
      double random = ((double)rand() + 1.0) / ((double)RAND_MAX + 2.0);
      size_t idx    = floor(repositorySize * random) + 1;
      if (repo.isEmpty(idx)) {
        emptyCount++;
        if (emptyCount > repositorySize) {
          break;
        }
        continue;
      }
      ObjectDistance obj(idx, 0.0);
      if (find(seeds.begin(), seeds.end(), obj) != seeds.end()) {
        continue;
      }
      seeds.push_back(obj);
    }
  }

  void remove(const ObjectID id, bool force) {
    if (!NeighborhoodGraph::repository.isEmpty(id)) {
      removeEdgesReliably(id);
    }
    try {
      getObjectRepository().remove(id);
    } catch (Exception &err) {
      std::cerr << "NGT::GraphIndex::remove:: cannot remove from feature. id=" << id << " " << err.what()
                << std::endl;
      throw err;
    }
  }

  virtual void searchForNNGInsertion(Object &po, ObjectDistances &result) {
    NGT::SearchContainer sc(po);
    sc.setResults(&result);
    sc.size                   = NeighborhoodGraph::property.edgeSizeForCreation;
    sc.radius                 = FLT_MAX;
    sc.explorationCoefficient = NeighborhoodGraph::property.insertionRadiusCoefficient;
    sc.insertion              = true;
    try {
      GraphIndex::search(static_cast<NGT::SearchContainer &>(sc));
    } catch (Exception &err) {
      throw err;
    }
    if (static_cast<int>(result.size()) < NeighborhoodGraph::property.edgeSizeForCreation &&
        result.size() < repository.size()) {
      if (sc.edgeSize != 0) {
        sc.edgeSize = 0; // not prune edges.
        try {
          GraphIndex::search(static_cast<NGT::SearchContainer &>(sc));
        } catch (Exception &err) {
          throw err;
        }
      }
    }
  }

  void searchForKNNGInsertion(Object &po, ObjectID id, ObjectDistances &result) {
    double radius = FLT_MAX;
    size_t size   = NeighborhoodGraph::property.edgeSizeForCreation;
    if (id > 0) {
      size = NeighborhoodGraph::property.edgeSizeForCreation + 1;
    }
    ResultSet rs;
    objectSpace->linearSearch(po, radius, size, rs);
    result.moveFrom(rs, id);
    if ((size_t)NeighborhoodGraph::property.edgeSizeForCreation != result.size()) {
      std::cerr << "searchForKNNGInsert::Warning! inconsistency of the sizes. ID=" << id << " "
                << NeighborhoodGraph::property.edgeSizeForCreation << ":" << result.size() << std::endl;
      for (size_t i = 0; i < result.size(); i++) {
        std::cerr << result[i].id << ":" << result[i].distance << " ";
      }
      std::cerr << std::endl;
    }
  }

  virtual void insert(ObjectID id) {
    ObjectRepository &fr = objectSpace->getRepository();
    if (fr[id] == 0) {
      std::cerr << "NGTIndex::insert empty " << id << std::endl;
      return;
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    Object &po = *objectSpace->allocateObject(*fr[id]);
#else
    Object &po = *fr[id];
#endif
    ObjectDistances rs;
    if (NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeANNG ||
        NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeIANNG ||
        NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeRANNG) {
      searchForNNGInsertion(po, rs);
    } else {
      searchForKNNGInsertion(po, id, rs);
    }
    insertNode(id, rs);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    objectSpace->deleteObject(&po);
#endif
  }

  virtual void createIndexWithSingleThread();
  virtual void createIndexWithInsertionOrder(InsertionOrder &insertionOrder, size_t threadNumber = 0,
                                             size_t sizeOfRepository = 0);

  void checkGraph() {
    GraphRepository &repo = repository;
    ObjectRepository &fr  = objectSpace->getRepository();
    for (size_t id = 0; id < fr.size(); id++) {
      if (repo[id] == 0) {
        std::cerr << id << " empty" << std::endl;
        continue;
      }
      if ((id % 10000) == 0) {
        std::cerr << "checkGraph: Processed size=" << id << std::endl;
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      Object &po = *objectSpace->allocateObject(*fr[id]);
#else
      Object &po = *fr[id];
#endif
      GraphNode *objects = getNode(id);

      ObjectDistances rs;
      NeighborhoodGraph::property.edgeSizeForCreation = objects->size() + 1;
      searchForNNGInsertion(po, rs);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      objectSpace->deleteObject(&po);
#endif

      if (rs.size() != objects->size()) {
        std::cerr << "Cannot get the specified number of the results. " << rs.size() << ":" << objects->size()
                  << std::endl;
      }
      size_t count                  = 0;
      ObjectDistances::iterator rsi = rs.begin();
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      for (GraphNode::iterator ri = objects->begin(repo.allocator);
           ri != objects->end(repo.allocator) && rsi != rs.end();) {
#else
      for (GraphNode::iterator ri = objects->begin(); ri != objects->end() && rsi != rs.end();) {
#endif
        if ((*ri).distance == (*rsi).distance && (*ri).id == (*rsi).id) {
          count++;
          ri++;
          rsi++;
        } else if ((*ri).distance < (*rsi).distance) {
          ri++;
        } else {
          rsi++;
        }
      }
      if (count != objects->size()) {
        std::cerr << "id=" << id << " identities=" << count << " " << objects->size() << " " << rs.size()
                  << std::endl;
      }
    }
  }

  virtual bool verify(std::vector<uint8_t> &status, bool info) {
    bool valid = true;
    std::cerr << "Started verifying graph and objects" << std::endl;
    GraphRepository &repo = repository;
    ObjectRepository &fr  = objectSpace->getRepository();
    if (repo.size() != fr.size()) {
      if (info) {
        std::cerr << "Warning! # of nodes is different from # of objects. " << repo.size() << ":" << fr.size()
                  << std::endl;
      }
    }
    status.clear();
    status.resize(fr.size(), 0);
    for (size_t id = 1; id < fr.size(); id++) {
      status[id] |= repo[id] != 0 ? 0x02 : 0x00;
      status[id] |= fr[id] != 0 ? 0x01 : 0x00;
    }
    for (size_t id = 1; id < fr.size(); id++) {
      if (fr[id] == 0) {
        if (id < repo.size() && repo[id] != 0) {
          std::cerr << "Error! The node exists in the graph, but the object does not exist. " << id
                    << std::endl;
          valid = false;
        }
      }
      if (fr[id] != 0 && repo[id] == 0) {
        std::cerr << "Error. No." << id << " is not registerd in the graph." << std::endl;
        valid = false;
      }
      if ((id % 1000000) == 0) {
        std::cerr << "  verified " << id << " entries." << std::endl;
      }
      if (fr[id] != 0) {
        try {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          Object *po = objectSpace->allocateObject(*fr[id]);
#else
          Object *po = fr[id];
#endif
          if (po == 0) {
            std::cerr << "Error! Cannot get the object. " << id << std::endl;
            valid = false;
            continue;
          }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          objectSpace->deleteObject(po);
#endif
        } catch (Exception &err) {
          std::cerr << "Error! Cannot get the object. " << id << " " << err.what() << std::endl;
          valid = false;
          continue;
        }
      }
      if (id >= repo.size()) {
        std::cerr << "Error. No." << id << " is not registerd in the object repository. " << repo.size()
                  << std::endl;
        valid = false;
      }
      if (id < repo.size() && repo[id] != 0) {
        try {
          GraphNode *objects = getNode(id);
          if (objects == 0) {
            std::cerr << "Error! Cannot get the node. " << id << std::endl;
            valid = false;
          }
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          for (GraphNode::iterator ri = objects->begin(repo.allocator); ri != objects->end(repo.allocator);
               ++ri) {
#else
          for (GraphNode::iterator ri = objects->begin(); ri != objects->end(); ++ri) {
#endif
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            for (GraphNode::iterator rj =
                     objects->begin(repo.allocator) + std::distance(objects->begin(repo.allocator), ri);
                 rj != objects->end(repo.allocator); ++rj) {
              if ((*ri).id == (*rj).id && std::distance(objects->begin(repo.allocator), ri) !=
                                              std::distance(objects->begin(repo.allocator), rj)) {
                std::cerr << "Error! More than two identical objects! ID=" << (*rj).id
                          << " idx=" << std::distance(objects->begin(repo.allocator), ri) << ":"
                          << std::distance(objects->begin(repo.allocator), rj)
                          << " disntace=" << (*ri).distance << ":" << (*rj).distance << std::endl;
#else
            for (GraphNode::iterator rj = objects->begin() + std::distance(objects->begin(), ri);
                 rj != objects->end(); ++rj) {
              if ((*ri).id == (*rj).id &&
                  std::distance(objects->begin(), ri) != std::distance(objects->begin(), rj)) {
                std::cerr << "Error! More than two identical objects! ID=" << (*rj).id
                          << " idx=" << std::distance(objects->begin(), ri) << ":"
                          << std::distance(objects->begin(), rj) << " disntace=" << (*ri).distance << ":"
                          << (*rj).distance << std::endl;
#endif
                valid = false;
              }
            }

            if ((*ri).id == 0 || (*ri).id >= repo.size()) {
              std::cerr << "Error! Neighbor's ID of the node is out of range. ID=" << id << std::endl;
              valid = false;
            } else if (repo[(*ri).id] == 0) {
              std::cerr << "Error! The neighbor ID of the node is invalid. ID=" << id
                        << " Invalid ID=" << (*ri).id << std::endl;
              if (fr[(*ri).id] == 0) {
                std::cerr << "The neighbor doesn't exist in the object repository as well. ID=" << (*ri).id
                          << std::endl;
              } else {
                std::cerr << "The neighbor exists in the object repository. ID=" << (*ri).id << std::endl;
              }
              valid = false;
            }
            if ((*ri).distance < 0.0) {
              std::cerr << "Error! Neighbor's distance is munus. ID=" << id << std::endl;
              valid = false;
            }
          }
        } catch (Exception &err) {
          std::cerr << "Error! Cannot get the node. " << id << " " << err.what() << std::endl;
          valid = false;
        }
      }
    }
    return valid;
  }

  void extractSparseness(InsertionOrder &insertionOrder);
  void extractInsertionOrder(InsertionOrder &insertionOrder);

  static bool showStatisticsOfGraph(NGT::GraphIndex &outGraph, char mode = '-', size_t edgeSize = UINT_MAX);
  static NGT::GraphIndex::GraphStatistics getGraphStatistics(NGT::GraphIndex &outGraph, char mode = '-',
                                                             size_t edgeSize = UINT_MAX);

  size_t getNumberOfObjects() { return objectSpace->getRepository().count(); }
  size_t getNumberOfIndexedObjects() {
    ObjectRepository &repo     = objectSpace->getRepository();
    GraphRepository &graphRepo = repository;
    size_t count               = 0;
    for (NGT::ObjectID id = 1; id < repo.size() && id < graphRepo.size(); id++) {
      if (repo[id] != 0 && graphRepo[id] != 0) {
        count++;
      }
    }
    return count;
  }

  size_t getObjectRepositorySize() { return objectSpace->getRepository().size(); }
  size_t getGraphRepositorySize() {
#ifdef NGT_GRAPH_READ_ONLY_GRAPH
    return std::max(repository.size(), searchRepository.size());
#else
    return repository.size();
#endif
  }

  size_t getSizeOfElement() { return objectSpace->getSizeOfElement(); }

  Object *allocateObject(const std::string &textLine, const std::string &sep) {
    return objectSpace->allocateNormalizedObject(textLine, sep);
  }
  Object *allocateObject(const std::vector<double> &obj) {
    return objectSpace->allocateNormalizedObject(obj);
  }
  Object *allocateObject(const std::vector<float> &obj) { return objectSpace->allocateNormalizedObject(obj); }
#ifdef NGT_HALF_FLOAT
  Object *allocateObject(const std::vector<float16> &obj) {
    return objectSpace->allocateNormalizedObject(obj);
  }
#endif
  Object *allocateObject(const std::vector<uint8_t> &obj) {
    return objectSpace->allocateNormalizedObject(obj);
  }
  Object *allocateObject(const float *obj, size_t size) {
    return objectSpace->allocateNormalizedObject(obj, size);
  }

  void deleteObject(Object *po) { return objectSpace->deleteObject(po); }

  ObjectSpace &getObjectSpace() { return *objectSpace; }
#ifdef NGT_REFINEMENT
  ObjectSpace &getRefinementObjectSpace() { return *refinementObjectSpace; }
  bool refinementObjectSpaceIsAvailable() {
    return refinementObjectSpace != 0 && !refinementObjectSpace->getRepository().empty();
  }
#endif
  void setupPrefetch(NGT::Property &prop);

  void setProperty(NGT::Property &prop) {
    setupPrefetch(prop);
    GraphIndex::property.set(prop);
    NeighborhoodGraph::property.set(prop);
    assert(property.dimension != 0);
    accuracyTable.set(property.accuracyTable);
  }

  void getProperty(NGT::Property &prop) {
    GraphIndex::property.get(prop);
    NeighborhoodGraph::property.get(prop);
  }

  NeighborhoodGraph::Property &getGraphProperty() { return NeighborhoodGraph::property; }
  Index::Property &getGraphIndexProperty() { return GraphIndex::property; }

  virtual size_t getSharedMemorySize(std::ostream &os, SharedMemoryAllocator::GetMemorySizeType t) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    size_t size = repository.getAllocator().getMemorySize(t);
#else
    size_t size = 0;
#endif
    os << "graph=" << size << std::endl;
    return size;
  }

  float getEpsilonFromExpectedAccuracy(double accuracy) { return accuracyTable.getEpsilon(accuracy); }
  Index::Property &getProperty() { return property; }
  bool getReadOnly() { return readOnly; }

  template <class REPOSITORY> void getSeedsFromGraph(REPOSITORY &repo, ObjectDistances &seeds) {
    if (repo.size() != 0) {
      size_t seedSize = repo.size() - 1 < (size_t)NeighborhoodGraph::property.seedSize
                            ? repo.size() - 1
                            : (size_t)NeighborhoodGraph::property.seedSize;
      if (NeighborhoodGraph::property.seedType == NeighborhoodGraph::SeedTypeRandomNodes ||
          NeighborhoodGraph::property.seedType == NeighborhoodGraph::SeedTypeNone) {
        getRandomSeeds(repo, seeds, seedSize);
      } else if (NeighborhoodGraph::property.seedType == NeighborhoodGraph::SeedTypeFixedNodes) {
        // To check speed using fixed seeds.
        for (size_t i = 1; i <= seedSize; i++) {
          ObjectDistance obj(i, 0.0);
          seeds.push_back(obj);
        }
      } else if (NeighborhoodGraph::property.seedType == NeighborhoodGraph::SeedTypeFirstNode) {
        ObjectDistance obj(1, 0.0);
        seeds.push_back(obj);
      } else {
        getRandomSeeds(repo, seeds, seedSize);
      }
    }
  }

  // GraphIndex
  virtual void search(NGT::SearchContainer &sc, ObjectDistances &seeds) {
    if (sc.size == 0) {
      while (!sc.workingResult.empty())
        sc.workingResult.pop();
      return;
    }
    if (seeds.size() == 0) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR) || !defined(NGT_GRAPH_READ_ONLY_GRAPH)
      getSeedsFromGraph(repository, seeds);
#else
      if (readOnly) {
        getSeedsFromGraph(searchRepository, seeds);
      } else {
        getSeedsFromGraph(repository, seeds);
      }
#endif
    }
    if (sc.expectedAccuracy > 0.0) {
      sc.setEpsilon(getEpsilonFromExpectedAccuracy(sc.expectedAccuracy));
    }
    try {
      if (readOnly) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR) || !defined(NGT_GRAPH_READ_ONLY_GRAPH)
        NeighborhoodGraph::search(sc, seeds);
#else
        (*searchUnupdatableGraph)(*this, sc, seeds);
#endif
      } else {
        NeighborhoodGraph::search(sc, seeds);
      }
    } catch (Exception &err) {
      Exception e(err);
      throw e;
    }
  }

 public:
  Index::Property property;

 protected:
  bool readOnly;
#ifdef NGT_GRAPH_READ_ONLY_GRAPH
  void (*searchUnupdatableGraph)(NGT::NeighborhoodGraph &, NGT::SearchContainer &, NGT::ObjectDistances &);
#endif

  Index::AccuracyTable accuracyTable;
};

class GraphAndTreeIndex : public GraphIndex, public DVPTree {
 public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  GraphAndTreeIndex(const std::string &allocator, NGT::Property &prop, bool rdOnly)
      : GraphIndex(allocator, rdOnly), DVPTree(prop) {
    initialize(allocator, 0);
  }
  GraphAndTreeIndex(const std::string &allocator, NGT::Property &prop);
  void initialize(const std::string &allocator, size_t sharedMemorySize) {
    DVPTree::objectSpace = GraphIndex::objectSpace;
    DVPTree::open(allocator + "/tre", sharedMemorySize);
  }
#else
  GraphAndTreeIndex(const std::string &database, NGT::Property &prop, bool rdOnly = false)
      : GraphIndex(database, rdOnly), DVPTree(prop) {
    GraphAndTreeIndex::loadIndex(database, rdOnly);
  }

  GraphAndTreeIndex(NGT::Property &prop) : GraphIndex(prop), DVPTree(prop) {
    DVPTree::objectSpace = GraphIndex::objectSpace;
  }
#endif
  virtual ~GraphAndTreeIndex() {}

  void create() {}

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  void alignObjects() {}
#else
  void alignObjects() {
    NGT::ObjectSpace &space     = getObjectSpace();
    NGT::ObjectRepository &repo = space.getRepository();
    Object **object             = repo.getPtr();
    std::vector<bool> exist(repo.size(), false);
    std::vector<NGT::Node::ID> leafNodeIDs;
    DVPTree::getAllLeafNodeIDs(leafNodeIDs);
    size_t objectCount = 0;
    for (size_t i = 0; i < leafNodeIDs.size(); i++) {
      ObjectDistances objects;
      DVPTree::getObjectIDsFromLeaf(leafNodeIDs[i], objects);
      for (size_t j = 0; j < objects.size(); j++) {
        exist[objects[j].id] = true;
        objectCount++;
      }
    }
    std::multimap<uint32_t, uint32_t> notexist;
    if (objectCount != repo.size()) {
      for (size_t id = 1; id < exist.size(); id++) {
        if (!exist[id]) {
          DVPTree::SearchContainer tso(*object[id]);
          tso.mode   = DVPTree::SearchContainer::SearchLeaf;
          tso.radius = 0.0;
          tso.size   = 1;
          try {
            DVPTree::search(tso);
          } catch (Exception &err) {
            std::stringstream msg;
            msg << "GraphAndTreeIndex::getSeeds: Cannot search for tree.:" << err.what();
            NGTThrowException(msg);
          }
          notexist.insert(std::pair<uint32_t, uint32_t>(tso.nodeID.getID(), id));
          objectCount++;
        }
      }
    }
    assert(objectCount == repo.size() - 1);

    objectCount = 1;
    std::vector<std::pair<uint32_t, uint32_t>> order;
    for (size_t i = 0; i < leafNodeIDs.size(); i++) {
      ObjectDistances objects;
      DVPTree::getObjectIDsFromLeaf(leafNodeIDs[i], objects);
      for (size_t j = 0; j < objects.size(); j++) {
        order.push_back(std::pair<uint32_t, uint32_t>(objects[j].id, objectCount));
        objectCount++;
      }
      auto nei = notexist.equal_range(leafNodeIDs[i].getID());
      for (auto ii = nei.first; ii != nei.second; ++ii) {
        order.push_back(std::pair<uint32_t, uint32_t>((*ii).second, objectCount));
        objectCount++;
      }
    }
    assert(objectCount == repo.size());
    Object *tmp = space.allocateObject();
    std::unordered_set<uint32_t> uncopiedObjects;
    for (size_t i = 1; i < repo.size(); i++) {
      uncopiedObjects.insert(i);
    }
    size_t copycount = 0;
    while (!uncopiedObjects.empty()) {
      size_t startID = *uncopiedObjects.begin();
      if (startID == order[startID - 1].first) {
        uncopiedObjects.erase(startID);
        copycount++;
        continue;
      }
      size_t id = startID;
      space.copy(*tmp, *object[id]);
      uncopiedObjects.erase(id);
      do {
        space.copy(*object[id], *object[order[id - 1].first]);
        copycount++;
        id = order[id - 1].first;
        uncopiedObjects.erase(id);
      } while (order[id - 1].first != startID);
      space.copy(*object[id], *tmp);
      copycount++;
    }
    space.deleteObject(tmp);

    assert(copycount == repo.size() - 1);

    sort(order.begin(), order.end());
    uncopiedObjects.clear();
    for (size_t i = 1; i < repo.size(); i++) {
      uncopiedObjects.insert(i);
    }
    copycount = 0;
    Object *tmpPtr;
    while (!uncopiedObjects.empty()) {
      size_t startID = *uncopiedObjects.begin();
      if (startID == order[startID - 1].second) {
        uncopiedObjects.erase(startID);
        copycount++;
        continue;
      }
      size_t id = startID;
      tmpPtr    = object[id];
      uncopiedObjects.erase(id);
      do {
        object[id] = object[order[id - 1].second];
        copycount++;
        id = order[id - 1].second;
        uncopiedObjects.erase(id);
      } while (order[id - 1].second != startID);
      object[id] = tmpPtr;
      copycount++;
    }
    assert(copycount == repo.size() - 1);
  }
#endif // NGT_SHARED_MEMORY_ALLOCATOR

  void load(const std::string &ifile) {
    GraphIndex::load(ifile);
    DVPTree::objectSpace = GraphIndex::objectSpace;
  }

  void saveIndex(const std::string &ofile) {
    GraphIndex::saveIndex(ofile);
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
    std::string fname = ofile + "/tre";
    std::ofstream ost(fname);
    if (!ost.is_open()) {
      std::stringstream msg;
      msg << "saveIndex:: Cannot open. " << fname;
      NGTThrowException(msg);
    }
    DVPTree::serialize(ost);
#endif
  }

  void loadIndex(const std::string &ifile, bool readOnly) {
    DVPTree::objectSpace = GraphIndex::objectSpace;
    std::ifstream ist(ifile + "/tre");
    DVPTree::deserialize(ist);
#ifdef NGT_GRAPH_READ_ONLY_GRAPH
    if (property.objectAlignment == NGT::Index::Property::ObjectAlignmentTrue) {
      alignObjects();
    }
#endif
  }

  void clearIndex() {
    GraphIndex::clearIndex();
    DVPTree::deleteAll();
  }

  void exportIndex(const std::string &ofile) {
    GraphIndex::exportIndex(ofile);
    std::ofstream ost(ofile + "/tre");
    DVPTree::serializeAsText(ost);
  }

  void importIndex(const std::string &ifile) {
    std::string fname = ifile + "/tre";
    std::ifstream ist(fname);
    if (!ist.is_open()) {
      std::stringstream msg;
      msg << "importIndex:: Cannot open. " << fname;
      NGTThrowException(msg);
    }
    DVPTree::deserializeAsText(ist);
    GraphIndex::importIndex(ifile);
  }

  void remove(const ObjectID id, bool force = false) {
    Object *obj = 0;
    try {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      obj = GraphIndex::objectSpace->allocateObject(*GraphIndex::objectSpace->getRepository().get(id));
#else
      obj = GraphIndex::objectSpace->getRepository().get(id);
#endif
    } catch (Exception &err) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      GraphIndex::objectSpace->deleteObject(obj);
#endif
      if (force) {
        try {
          DVPTree::removeNaively(id);
        } catch (...) {
        }
        try {
          GraphIndex::remove(id, force);
        } catch (...) {
        }
        std::stringstream msg;
        msg << err.what()
            << " Even though the object could not be found, the object could be removed from the tree and "
               "graph if it existed in them.";
        NGTThrowException(msg);
      }
      throw err;
    }
    try {
      GraphIndex::getNode(id);
    } catch (Exception &err) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      GraphIndex::objectSpace->deleteObject(obj);
#endif
      if (force) {
        try {
          DVPTree::removeNaively(id);
        } catch (...) {
        }
        try {
          GraphIndex::remove(id, force);
        } catch (...) {
        }
        std::stringstream msg;
        msg << err.what()
            << " Even though the node could not be found, the object could be removed from the indices "
               "if it existed in them.";
        NGTThrowException(msg);
      }
      throw err;
    }
    if (NeighborhoodGraph::repository.isEmpty(id)) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      GraphIndex::objectSpace->deleteObject(obj);
#endif
      if (force) {
        try {
          DVPTree::removeNaively(id);
        } catch (...) {
        }
      }
      GraphIndex::remove(id, force);
      return;
    }
    NGT::SearchContainer so(*obj);
    ObjectDistances results;
    so.setResults(&results);
    so.id                     = 0;
    so.size                   = 2;
    so.radius                 = 0.0;
    so.explorationCoefficient = 1.1;
    so.insertion              = true;
    ObjectDistances seeds;
    seeds.push_back(ObjectDistance(id, 0.0));
    GraphIndex::search(so, seeds);
    if (results.size() == 0) {
      if (!GraphIndex::objectSpace->isNormalizedDistance() && !GraphIndex::objectSpace->isQintObjectType()) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        GraphIndex::objectSpace->deleteObject(obj);
#endif
        std::stringstream msg;
        msg << "Not found the specified id. (1) ID=" << id;
        NGTThrowException(msg);
      }
      for (size_t it = 0, rsize = 10; it < 3 && results.empty(); it++, rsize *= 5) {
        so.radius = FLT_MAX;
        so.size   = rsize;
        results.clear();
        GraphIndex::search(so, seeds);
        for (size_t i = 0; i < results.size(); i++) {
          try {
            auto *robj          = GraphIndex::objectSpace->getRepository().get(results[i].id);
            results[i].distance = GraphIndex::objectSpace->compareWithL1(*obj, *robj);
          } catch (Exception &err) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
            GraphIndex::objectSpace->deleteObject(obj);
#endif
            std::stringstream msg;
            msg << "remove: Fatal Inner Error! Cannot get an object. ID=" << id;
            NGTThrowException(msg);
          }
        }
        std::sort(results.begin(), results.end());
        results.resize(2);
        for (auto i = results.begin(); i != results.end(); ++i) {
          if ((*i).distance != 0.0) {
            results.resize(distance(results.begin(), i));
            break;
          }
        }
        if (results.size() == 0) {
          std::cerr << "remove: Warning! Cannot find any same object. " << id << ":" << it << std::endl;
        }
      }
      if (results.size() == 0) {
        std::cerr << "remove: Warning! Cannot find the specified node through search. "
                     "Since this situation is weird, the index might become inconsistent. "
                     "Anyway, the removal process continues."
                  << std::endl;
        results.emplace_back(NGT::ObjectDistance(id, 0.0));
      }
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    GraphIndex::objectSpace->deleteObject(obj);
#endif
    if (results.size() == 1) {
      try {
        DVPTree::remove(id);
      } catch (Exception &err) {
        std::stringstream msg;
        msg << "remove:: cannot remove from tree. id=" << id << " " << err.what();
        NGTThrowException(msg);
      }
    } else {
      ObjectID replaceID = id == results[0].id ? results[1].id : results[0].id;
      try {
        DVPTree::replace(id, replaceID);
      } catch (Exception &err) {
      }
    }
    GraphIndex::remove(id, force);
  }

  void searchForNNGInsertion(Object &po, ObjectDistances &result) {
    NGT::SearchContainer sc(po);
    sc.setResults(&result);
    sc.size                   = NeighborhoodGraph::property.edgeSizeForCreation;
    sc.radius                 = FLT_MAX;
    sc.explorationCoefficient = NeighborhoodGraph::property.insertionRadiusCoefficient;
    sc.useAllNodesInLeaf      = true;
    sc.insertion              = true;
    try {
      GraphAndTreeIndex::search(static_cast<NGT::SearchContainer &>(sc));
    } catch (Exception &err) {
      throw err;
    }
    if (static_cast<int>(result.size()) < NeighborhoodGraph::property.edgeSizeForCreation &&
        result.size() < repository.size()) {
      if (sc.edgeSize != 0) {
        try {
          GraphAndTreeIndex::search(static_cast<NGT::SearchContainer &>(sc));
        } catch (Exception &err) {
          throw err;
        }
      }
    }
  }

  void insert(ObjectID id) {
    ObjectRepository &fr = GraphIndex::objectSpace->getRepository();
    if (fr[id] == 0) {
      std::cerr << "GraphAndTreeIndex::insert empty " << id << std::endl;
      return;
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    Object &po = *GraphIndex::objectSpace->allocateObject(*fr[id]);
#else
    Object &po = *fr[id];
#endif
    ObjectDistances rs;
    if (NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeANNG ||
        NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeIANNG ||
        NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeRANNG) {
      searchForNNGInsertion(po, rs);
    } else {
      searchForKNNGInsertion(po, id, rs);
    }

    GraphIndex::insertNode(id, rs);

    if (((rs.size() > 0) && (rs[0].distance != 0.0)) || rs.size() == 0) {
      DVPTree::InsertContainer tiobj(po, id);
      try {
        DVPTree::insert(tiobj);
      } catch (Exception &err) {
        std::cerr << "GraphAndTreeIndex::insert: Fatal error" << std::endl;
        std::cerr << err.what() << std::endl;
        return;
      }
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    GraphIndex::objectSpace->deleteObject(&po);
#endif
  }

  void createIndexWithInsertionOrder(InsertionOrder &insertionOrder, size_t threadNumber = 0,
                                     size_t sizeOfRepository = 0);

  void createIndex(const std::vector<std::pair<NGT::Object *, size_t>> &objects,
                   std::vector<InsertionResult> &ids, float range, size_t threadNumber);

  void createTreeIndex();

  void getSeeds(NGT::SearchContainer &sc, ObjectDistances &seeds, size_t n) {
    DVPTree::SearchContainer tso(sc.object);
    tso.mode                     = DVPTree::SearchContainer::SearchLeaf;
    tso.radius                   = 0.0;
    tso.size                     = 1;
    tso.distanceComputationCount = 0;
    tso.visitCount               = 0;
    try {
      DVPTree::search(tso);
    } catch (Exception &err) {
      std::stringstream msg;
      msg << "GraphAndTreeIndex::getSeeds: Cannot search for tree.:" << err.what();
      NGTThrowException(msg);
    }
    try {
      DVPTree::getObjectIDsFromLeaf(tso.nodeID, seeds);
    } catch (Exception &err) {
      std::stringstream msg;
      msg << "GraphAndTreeIndex::getSeeds: Cannot get a leaf.:" << err.what();
      NGTThrowException(msg);
    }
    sc.distanceComputationCount += tso.distanceComputationCount;
    sc.visitCount += tso.visitCount;
    if (seeds.size() < n) {
      GraphIndex::getRandomSeeds(repository, seeds, n);
    }
    GraphIndex::setupDistances(sc, seeds);
    std::sort(seeds.begin(), seeds.end());
    if (seeds.size() > n) {
      seeds.resize(n);
    }
  }

  // GraphAndTreeIndex
  void getSeedsFromTree(NGT::SearchContainer &sc, ObjectDistances &seeds) {
    DVPTree::SearchContainer tso(sc.object);
    tso.mode                     = DVPTree::SearchContainer::SearchLeaf;
    tso.radius                   = 0.0;
    tso.size                     = 1;
    tso.distanceComputationCount = 0;
    tso.visitCount               = 0;
    try {
      DVPTree::search(tso);
    } catch (Exception &err) {
      std::stringstream msg;
      msg << "GraphAndTreeIndex::getSeeds: Cannot search for tree.:" << err.what();
      NGTThrowException(msg);
    }
    try {
      DVPTree::getObjectIDsFromLeaf(tso.nodeID, seeds);
    } catch (Exception &err) {
      std::stringstream msg;
      msg << "GraphAndTreeIndex::getSeeds: Cannot get a leaf.:" << err.what();
      NGTThrowException(msg);
    }
    sc.distanceComputationCount += tso.distanceComputationCount;
    sc.visitCount += tso.visitCount;
    if (sc.useAllNodesInLeaf ||
        NeighborhoodGraph::property.seedType == NeighborhoodGraph::SeedTypeAllLeafNodes) {
      return;
    }
    // if seedSize is zero, the result size of the query is used as seedSize.
    size_t seedSize =
        NeighborhoodGraph::property.seedSize == 0 ? sc.size : NeighborhoodGraph::property.seedSize;
    seedSize = seedSize > sc.size ? sc.size : seedSize;
    if (seeds.size() > seedSize) {
#ifndef NGT_ENABLE_TIME_SEED_FOR_RANDOM
      srand(tso.nodeID.getID());
#endif
      // to accelerate thinning data.
      for (size_t i = seeds.size(); i > seedSize; i--) {
        double random = ((double)rand() + 1.0) / ((double)RAND_MAX + 2.0);
        size_t idx    = floor(i * random);
        seeds[idx]    = seeds[i - 1];
      }
      seeds.resize(seedSize);
    } else if (seeds.size() < seedSize) {
      // A lack of the seeds is compansated by random seeds.
      //getRandomSeeds(seeds, seedSize);
    }
  }

  // GraphAndTreeIndex
  void search(NGT::SearchContainer &sc) {
    sc.distanceComputationCount = 0;
    sc.visitCount               = 0;
    ObjectDistances seeds;
    getSeedsFromTree(sc, seeds);
    sc.visitCount = sc.distanceComputationCount;
    GraphIndex::search(sc, seeds);
  }

  // GraphAndTreeIndex
  void search(NGT::SearchQuery &searchQuery) {
    NGT::Object *query = 0;
    try {
#ifdef NGT_PQ4
      if (getObjectSpace().pq4IsEnabled() && readOnly) {
        query = getObjectSpace().allocateQuantizedQuery(searchQuery.getQueryVector());
      } else {
        query = Index::allocateQuery(searchQuery);
      }
      NGT::SearchQuery sq(searchQuery, *query);
#else
      query = Index::allocateQuery(searchQuery);
      NGT::SearchContainer sq(searchQuery, *query);
#endif
#ifdef NGT_REFINEMENT
      auto expansion = searchQuery.getRefinementExpansion();
      if (expansion < 1.0 || !refinementObjectSpaceIsAvailable()) {
        GraphAndTreeIndex::search(static_cast<NGT::SearchContainer &>(sq));
        searchQuery.workingResult = std::move(sq.workingResult);
      } else {
        size_t poffset = 12;
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
        size_t psize = 64;
#endif
        auto size = sq.size;
        sq.size *= expansion;
        try {
          GraphAndTreeIndex::search(static_cast<NGT::SearchContainer &>(sq));
        } catch (Exception &err) {
          sq.size = size;
          throw err;
        }
        auto &ros            = getRefinementObjectSpace();
        auto &rrepo          = ros.getRepository();
        NGT::Object *robject = 0;
        if (searchQuery.getQueryType() == typeid(float)) {
          auto &v = *static_cast<std::vector<float> *>(searchQuery.getQuery());
          robject = ros.allocateNormalizedObject(v);
        } else if (searchQuery.getQueryType() == typeid(uint8_t)) {
          auto &v = *static_cast<std::vector<uint8_t> *>(searchQuery.getQuery());
          robject = ros.allocateNormalizedObject(v);
#ifdef NGT_HALF_FLOAT
        } else if (searchQuery.getQueryType() == typeid(float16)) {
          auto &v = *static_cast<std::vector<float16> *>(searchQuery.getQuery());
          robject = ros.allocateNormalizedObject(v);
#endif
        } else {
          std::stringstream msg;
          msg << "Invalid query object type.";
          NGTThrowException(msg);
        }
        sq.size          = size;
        auto &comparator = getRefinementObjectSpace().getComparator();
        if (sq.resultIsAvailable()) {
          auto &results = sq.getResult();
          for (auto &r : results) {
            r.distance = comparator(*robject, *rrepo.get(r.id));
          }
          std::sort(results.begin(), results.end());
          results.resize(size);
        } else {
          ObjectDistances rs;
          rs.resize(sq.workingResult.size());
          size_t counter = 0;
          while (!sq.workingResult.empty()) {
            if (counter < poffset) {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
              auto *ptr = rrepo.get(sq.workingResult.top().id)->getPointer();
              MemoryCache::prefetch(static_cast<uint8_t *>(ptr), psize);
#endif
            }
            rs[counter++].id = sq.workingResult.top().id;
            sq.workingResult.pop();
          }
          for (size_t idx = 0; idx < rs.size(); idx++) {
            if (idx + poffset < rs.size()) {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
              auto *ptr = rrepo.get(rs[idx + poffset].id)->getPointer();
              MemoryCache::prefetch(static_cast<uint8_t *>(ptr), psize);
#endif
            }
            auto &r    = rs[idx];
            r.distance = comparator(*robject, *rrepo.get(r.id));
            searchQuery.workingResult.emplace(r);
          }
          while (searchQuery.workingResult.size() > sq.size) {
            searchQuery.workingResult.pop();
          }
        }
        ros.deleteObject(robject);
      }
#else
      GraphAndTreeIndex::search(sq);
      searchQuery.workingResult = std::move(sq.workingResult);
#endif
      searchQuery.distanceComputationCount = sq.distanceComputationCount;
      searchQuery.visitCount               = sq.visitCount;
    } catch (Exception &err) {
      if (query != 0) {
#ifdef NGT_PQ4
        if (getObjectSpace().pq4IsEnabled() & readOnly) {
          getObjectSpace().getQuantizer().destructQueryObject(query->getPointer());
          query->detach();
        }
#endif
        deleteObject(query);
      }
      throw err;
    }
#ifdef NGT_PQ4
    if (getObjectSpace().pq4IsEnabled() & readOnly) {
      getObjectSpace().getQuantizer().destructQueryObject(query->getPointer());
      query->detach();
    }
#endif
    deleteObject(query);
  }

  size_t getSharedMemorySize(std::ostream &os, SharedMemoryAllocator::GetMemorySizeType t) {
    return GraphIndex::getSharedMemorySize(os, t) + DVPTree::getSharedMemorySize(os, t);
  }

  bool verify(std::vector<uint8_t> &status, bool info, char mode);
};

class Property : public Index::Property, public NeighborhoodGraph::Property {
 public:
  void setDefault() {
    Index::Property::setDefault();
    NeighborhoodGraph::Property::setDefault();
  }
  void clear() {
    Index::Property::clear();
    NeighborhoodGraph::Property::clear();
  }
  void set(NGT::Property &p) {
    Index::Property::set(p);
    NeighborhoodGraph::Property::set(p);
  }
  void load(const std::string &file) {
    NGT::PropertySet prop;
    prop.load(file + "/prf");
    Index::Property::importProperty(prop);
    NeighborhoodGraph::Property::importProperty(prop);
  }

  void save(const std::string &file) {
    NGT::PropertySet prop;
    Index::Property::exportProperty(prop);
    NeighborhoodGraph::Property::exportProperty(prop);
    prop.save(file + "/prf");
  }

  static void save(GraphIndex &graphIndex, const std::string &file) {
    NGT::PropertySet prop;
    graphIndex.getGraphIndexProperty().exportProperty(prop);
    graphIndex.getGraphProperty().exportProperty(prop);
    prop.save(file + "/prf");
  }

  void importProperty(const std::string &file) {
    NGT::PropertySet prop;
    prop.load(file + "/prf");
    Index::Property::importProperty(prop);
    NeighborhoodGraph::Property::importProperty(prop);
  }

  static void exportProperty(GraphIndex &graphIndex, const std::string &file) {
    NGT::PropertySet prop;
    graphIndex.getGraphIndexProperty().exportProperty(prop);
    graphIndex.getGraphProperty().exportProperty(prop);
    prop.save(file + "/prf");
  }
};

} // namespace NGT

template <typename T> size_t NGT::Index::append(const std::vector<T> &object) {
  auto &os   = getObjectSpace();
  auto &repo = os.getRepository();
  if (repo.size() == 0) {
    repo.initialize();
  }
  auto *o = repo.allocateNormalizedPersistentObject(object);
  repo.push_back(dynamic_cast<PersistentObject *>(o));
  size_t oid = repo.size() - 1;
  return oid;
}

template <typename T> size_t NGT::Index::insert(const std::vector<T> &object) {
  auto &os   = getObjectSpace();
  auto &repo = os.getRepository();
  if (repo.size() == 0) {
    repo.initialize();
  }

  auto *o    = repo.allocateNormalizedPersistentObject(object);
  size_t oid = repo.insert(dynamic_cast<PersistentObject *>(o));
  return oid;
}

template <typename T> size_t NGT::Index::insert(ObjectID id, const std::vector<T> &object) {
  auto &os   = getObjectSpace();
  auto &repo = os.getRepository();
  if (repo.size() == 0) {
    repo.initialize();
  }

  auto *o    = repo.allocateNormalizedPersistentObject(object);
  size_t oid = repo.insert(id, dynamic_cast<PersistentObject *>(o));
  return oid;
}

template <typename T> void NGT::Index::update(ObjectID id, const std::vector<T> &object) {
  auto &os   = getObjectSpace();
  auto &repo = os.getRepository();

  Object *obj = 0;
  try {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    obj = os.allocateObject(*repo.get(id));
#else
    obj = repo.get(id);
#endif
  } catch (Exception &err) {
    std::stringstream msg;
    msg << "Invalid ID. " << id;
    NGTThrowException(msg);
  }
  repo.setObject(*obj, object);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  os.deleteObject(obj);
#endif
  return;
}

#ifdef NGT_REFINEMENT
template <typename T> size_t NGT::Index::appendToRefinement(const std::vector<T> &object) {
  auto &os   = getRefinementObjectSpace();
  auto &repo = os.getRepository();
  if (repo.size() == 0) {
    repo.initialize();
  }

  auto *o = repo.allocateNormalizedPersistentObject(object);
  repo.push_back(dynamic_cast<PersistentObject *>(o));
  size_t oid = repo.size() - 1;
  return oid;
}

template <typename T> size_t NGT::Index::insertToRefinement(const std::vector<T> &object) {
  auto &os   = getRefinementObjectSpace();
  auto &repo = os.getRepository();
  if (repo.size() == 0) {
    repo.initialize();
  }

  auto *o    = repo.allocateNormalizedPersistentObject(object);
  size_t oid = repo.insert(dynamic_cast<PersistentObject *>(o));
  return oid;
}

template <typename T> void NGT::Index::updateToRefinement(ObjectID id, const std::vector<T> &object) {
  auto &os   = getRefinementObjectSpace();
  auto &repo = os.getRepository();

  Object *obj = 0;
  try {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    obj = os.allocateObject(*repo.get(id));
#else
    obj = repo.get(id);
#endif
  } catch (Exception &err) {
    std::stringstream msg;
    msg << "Invalid ID. " << id;
    NGTThrowException(msg);
  }
  repo.setObject(*obj, object);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  os.deleteObject(obj);
#endif
  return;
}
#endif

template <typename T>
void NGT::Index::appendWithPreprocessing(const T *data, size_t dataSize, bool append, bool refinement) {
  if (dataSize == 0) {
    return;
  }
  NGT::Property prop;
  getProperty(prop);
  float maxMag    = prop.maxMagnitude;
  bool maxMagSkip = false;
  if (maxMag > 0.0) maxMagSkip = true;
  std::vector<float> addedElement;
  auto *obj  = data;
  size_t dim = prop.dimension;
  if (append && prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
    NGT::Timer timer;
    timer.start();
    size_t counter = 0;
    for (size_t idx = 0; idx < dataSize; idx++, obj += dim) {
      std::vector<float> object;
      object.reserve(dim);
      for (size_t dataidx = 0; dataidx < dim; dataidx++) {
        object.push_back(obj[dataidx]);
      }
      double mag = 0.0;
      if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
        for (auto &v : object) {
          mag += static_cast<float>(v) * v;
        }
        if (!maxMagSkip && mag > maxMag) {
          maxMag = mag;
        }
        addedElement.emplace_back(mag);
      }
      counter++;
      if (counter % 2000000 == 0) {
        timer.stop();
        std::cerr << "processed " << static_cast<float>(counter) / 1000000.0 << "M objects."
                  << " maxMag=" << maxMag << " time=" << timer << std::endl;
        timer.restart();
      }
    }
    timer.stop();
    std::cerr << "time=" << timer << std::endl;
    std::cerr << "maxMag=" << maxMag << std::endl;
    std::cerr << "dataSize=" << dataSize << std::endl;
    if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
      if (static_cast<NGT::GraphIndex &>(getIndex()).property.maxMagnitude <= 0.0 && maxMag > 0.0) {
        static_cast<NGT::GraphIndex &>(getIndex()).property.maxMagnitude = maxMag;
      }
    }
  }

  if (append && getObjectSpace().isQintObjectType() && prop.clippingRate >= 0.0) {
    std::priority_queue<float> min;
    std::priority_queue<float, std::vector<float>, std::greater<float>> max;
    {
      NGT::Timer timer;
      timer.start();
      auto clippingSize = static_cast<float>(dataSize * dim) * prop.clippingRate;
      clippingSize      = clippingSize == 0 ? 1 : clippingSize;
      std::string line;
      size_t counter = 0;
      obj            = data;
      for (size_t idx = 0; idx < dataSize; idx++, obj += dim) {
        std::vector<float> object;
        object.reserve(dim);
        for (size_t dataidx = 0; dataidx < dim; dataidx++) {
          object.push_back(obj[dataidx]);
        }
        if (getObjectSpace().isNormalizedDistance()) {
          ObjectSpace::normalize(object);
        }
        if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
          float v = maxMag - addedElement[counter];
          object.emplace_back(sqrt(v >= 0.0 ? v : 0.0));
        }
        for (auto &v : object) {
          if (max.size() < clippingSize) {
            max.push(v);
          } else if (max.top() <= v) {
            max.push(v);
            max.pop();
          }
          if (min.size() < clippingSize) {
            min.push(v);
          } else if (min.top() >= v) {
            min.push(v);
            min.pop();
          }
        }
        counter++;
      }
      timer.stop();
      std::cerr << "time=" << timer << std::endl;
      if (counter != 0) {
        std::cerr << "max:min=" << max.top() << ":" << min.top() << std::endl;
        setQuantizationFromMaxMin(max.top(), min.top());
      }
    }
  }
  if (append || refinement) {

    size_t counter = 0;
    obj            = data;
    for (size_t idx = 0; idx < dataSize; idx++, obj += dim) {
      std::vector<float> object;
      object.reserve(dim);
      for (size_t dataidx = 0; dataidx < dim; dataidx++) {
        object.push_back(obj[dataidx]);
      }
#ifdef NGT_REFINEMENT
      if (refinement) {
        appendToRefinement(object);
      }
#endif
      if (append) {
        if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
          object.emplace_back(sqrt(maxMag - addedElement[counter]));
        }
        NGT::Index::append(object);
      }
      counter++;
    }
  }
  std::cerr << "End of append" << std::endl;
}

template <typename T> void NGT::Index::append(const T *data, size_t dataSize, bool append, bool refinement) {
  StdOstreamRedirector redirector(redirect);
  redirector.begin();
  try {
    NGT::Property prop;
    getProperty(prop);
    if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct ||
        getObjectSpace().isQintObjectType()) {
      appendWithPreprocessing(data, dataSize, append, refinement);
    } else {
      auto &index = static_cast<GraphIndex &>(getIndex());
      if (append) {
        index.append(data, dataSize);
      }
#ifdef NGT_REFINEMENT
      if (refinement) {
        index.appendToRefinement(data, dataSize);
      }
#endif
    }
  } catch (Exception &err) {
    redirector.end();
    throw err;
  }
  redirector.end();
}
