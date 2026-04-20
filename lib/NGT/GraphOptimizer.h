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

#include "GraphReconstructor.h"
#include "Optimizer.h"

namespace NGT {
class GraphOptimizer {
 public:
  class ANNGEdgeOptimizationParameter {
   public:
    ANNGEdgeOptimizationParameter() { initialize(); }
    void initialize() {
      noOfQueries       = 200;
      noOfResults       = 50;
      noOfThreads       = 16;
      targetAccuracy    = 0.9; // when epsilon is 0.0 and all of the edges are used
      targetNoOfObjects = 0;
      noOfSampleObjects = 100000;
      maxNoOfEdges      = 100;
    }
    size_t noOfQueries;
    size_t noOfResults;
    size_t noOfThreads;
    float targetAccuracy;
    size_t targetNoOfObjects;
    size_t noOfSampleObjects;
    size_t maxNoOfEdges;
  };

  GraphOptimizer(bool unlog = false) {
    init();
    logDisabled = unlog;
  }

  GraphOptimizer(int outgoing, int incoming, int nofqs, int nofrs, float baseAccuracyFrom,
                 float baseAccuracyTo, float rateAccuracyFrom, float rateAccuracyTo, double gte, double m,
                 bool unlog // stderr log is disabled.
  ) {
    init();
    set(outgoing, incoming, nofqs, nofrs, baseAccuracyFrom, baseAccuracyTo, rateAccuracyFrom, rateAccuracyTo,
        gte, m);
    logDisabled = unlog;
  }

  void init() {
    numOfOutgoingEdges              = 10;
    numOfIncomingEdges              = 120;
    minNumOfEdges                   = 0;
    maxNumOfEdges                   = std::numeric_limits<int64_t>::max();
    numOfQueries                    = 100;
    numOfResults                    = 20;
    baseAccuracyRange               = std::pair<float, float>(0.30, 0.50);
    rateAccuracyRange               = std::pair<float, float>(0.80, 0.90);
    gtEpsilon                       = 0.1;
    margin                          = 0.2;
    logDisabled                     = false;
    shortcutReduction               = true;
    searchParameterOptimization     = true;
    prefetchParameterOptimization   = true;
    accuracyTableGeneration         = true;
    shortcutReductionWithLessMemory = false;
    undirectedGraphConversion       = false;
    numOfThreads                    = 0;
  }

  void adjustSearchCoefficients(const std::string indexPath) {
    NGT::Index index(indexPath);
    NGT::GraphIndex &graph = static_cast<NGT::GraphIndex &>(index.getIndex());
    NGT::Optimizer optimizer(index);
    if (logDisabled) {
      optimizer.disableLog();
    } else {
      optimizer.enableLog();
    }
    try {
      auto coefficients = optimizer.adjustSearchEdgeSize(baseAccuracyRange, rateAccuracyRange, numOfQueries,
                                                         gtEpsilon, margin);
      NGT::NeighborhoodGraph::Property &prop = graph.getGraphProperty();
      prop.dynamicEdgeSizeBase               = coefficients.first;
      prop.dynamicEdgeSizeRate               = coefficients.second;
      prop.edgeSizeForSearch                 = -2;
    } catch (NGT::Exception &err) {
      std::stringstream msg;
      msg << "Optimizer::adjustSearchCoefficients: Cannot adjust the search coefficients. " << err.what();
      NGTThrowException(msg);
    }
    graph.saveIndex(indexPath);
  }

  static std::pair<double, float> measureQueryTime(NGT::Index &index, size_t start, float epsilon) {
    NGT::ObjectSpace &objectSpace           = index.getObjectSpace();
    NGT::ObjectRepository &objectRepository = objectSpace.getRepository();
    size_t nQueries                         = 200;
    if (objectRepository.size() == 0) {
      std::stringstream msg;
      msg << "The object repository is empty. " << objectRepository.size();
      NGTThrowException(msg);
    }
    nQueries = objectRepository.size() - 1 < nQueries ? objectRepository.size() - 1 : nQueries;
    if (nQueries == 0) {
      std::stringstream msg;
      msg << "# of the queries is unexpected zero value. " << nQueries << ":" << objectRepository.size();
      NGTThrowException(msg);
    }
    size_t step = objectRepository.size() / nQueries;
    std::vector<size_t> ids;
    for (size_t startID = start; startID < step; startID++) {
      for (size_t id = startID; id < objectRepository.size(); id += step) {
        if (!objectRepository.isEmpty(id)) {
          ids.push_back(id);
        }
      }
      if (ids.size() >= nQueries) {
        ids.resize(nQueries);
        break;
      }
    }
    if (nQueries > ids.size()) {
      std::cerr << "# of Queries is not enough." << std::endl;
      return std::pair<double, float>(DBL_MAX, 0.1);
    }
    if (epsilon < 0.0) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      NGT::Object *obj = objectSpace.allocateObject(*objectRepository.get(ids[0]));
      NGT::SearchContainer searchContainer(*obj);
#else
      NGT::SearchContainer searchContainer(*objectRepository.get(ids[0]));
#endif
      for (size_t i = 0; i < 100; i++, epsilon /= 2.0) {
        Timer timer;
        NGT::ObjectDistances objects;
        searchContainer.setResults(&objects);
        searchContainer.setSize(10);
        searchContainer.setEpsilon(epsilon);
        timer.start();
        index.search(searchContainer);
        timer.stop();
        std::cerr << timer << std::endl;
        std::cerr << timer.ntime << std::endl;
        std::cerr << epsilon << std::endl;
        if (timer.ntime < 1000000.0) {
          break;
        }
      }
    }

    NGT::Timer timer;
    timer.reset();
    for (auto id = ids.begin(); id != ids.end(); id++) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      NGT::Object *obj = objectSpace.allocateObject(*objectRepository.get(*id));
      NGT::SearchContainer searchContainer(*obj);
#else
      NGT::SearchContainer searchContainer(*objectRepository.get(*id));
#endif
      NGT::ObjectDistances objects;
      searchContainer.setResults(&objects);
      searchContainer.setSize(10);
      searchContainer.setEpsilon(epsilon);
      timer.restart();
      index.search(searchContainer);
      timer.stop();
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      objectSpace.deleteObject(obj);
#endif
    }
    return std::pair<double, float>(timer.time * 1000.0, epsilon);
  }

  static std::pair<size_t, double> searchMinimumQueryTime(NGT::Index &index, size_t prefetchOffset,
                                                          int maxPrefetchSize, size_t seedID, float epsilon) {
    NGT::ObjectSpace &objectSpace = index.getObjectSpace();
    int step                      = 256;
    int prevPrefetchSize          = 64;
    size_t minPrefetchSize        = 0;
    double minTime                = DBL_MAX;
    for (step = 256; step != 32; step /= 2) {
      double prevTime = DBL_MAX;
      for (int prefetchSize = prevPrefetchSize - step < 64 ? 64 : prevPrefetchSize - step;
           prefetchSize <= maxPrefetchSize; prefetchSize += step) {
        objectSpace.setPrefetchOffset(prefetchOffset);
        objectSpace.setPrefetchSize(prefetchSize);
        auto time = measureQueryTime(index, seedID, epsilon);
        if (prevTime < time.first) {
          break;
        }
        prevTime         = time.first;
        prevPrefetchSize = prefetchSize;
      }
      if (minTime > prevTime) {
        minTime         = prevTime;
        minPrefetchSize = prevPrefetchSize;
      }
    }
    return std::make_pair(minPrefetchSize, minTime);
  }

  static std::pair<size_t, size_t> adjustPrefetchParameters(NGT::Index &index) {

    bool gridSearch = false;
    float epsilon   = -1.0;
    {
      auto time = measureQueryTime(index, 1, epsilon);
      epsilon   = time.second;
      if (time.first < 500.0) {
        gridSearch = true;
      }
    }

    size_t prefetchOffset = 0;
    size_t prefetchSize   = 0;
    std::vector<std::pair<size_t, size_t>> mins;
    NGT::ObjectSpace &objectSpace = index.getObjectSpace();
    int maxSize                   = objectSpace.getByteSizeOfObject() * 4;
    maxSize                       = maxSize < 64 * 28 ? maxSize : 64 * 28;
    for (int trial = 0; trial < 10; trial++) {
      size_t minps = 0;
      size_t minpo = 0;
      if (gridSearch) {
        double minTime = DBL_MAX;
        for (size_t po = 1; po <= 10; po++) {
          auto min = searchMinimumQueryTime(index, po, maxSize, trial + 1, epsilon);
          if (minTime > min.second) {
            minTime = min.second;
            minps   = min.first;
            minpo   = po;
          }
        }
      } else {
        double prevTime = DBL_MAX;
        for (size_t po = 1; po <= 10; po++) {
          auto min = searchMinimumQueryTime(index, po, maxSize, trial + 1, epsilon);
          if (prevTime < min.second) {
            break;
          }
          prevTime = min.second;
          minps    = min.first;
          minpo    = po;
        }
      }
      if (std::find(mins.begin(), mins.end(), std::make_pair(minpo, minps)) != mins.end()) {
        prefetchOffset = minpo;
        prefetchSize   = minps;
        mins.push_back(std::make_pair(minpo, minps));
        break;
      }
      mins.push_back(std::make_pair(minpo, minps));
    }
    return std::make_pair(prefetchOffset, prefetchSize);
  }

  void execute(const std::string inIndexPath, const std::string outIndexPath) {
    if (access(outIndexPath.c_str(), 0) == 0) {
      std::stringstream msg;
      msg << "Optimizer::execute: The specified index exists. " << outIndexPath;
      NGTThrowException(msg);
    }

    const std::string com = "cp -r " + inIndexPath + " " + outIndexPath;
    int stat              = system(com.c_str());
    if (stat != 0) {
      std::stringstream msg;
      msg << "Optimizer::execute: Cannot create the specified index. " << outIndexPath;
      NGTThrowException(msg);
    }

    {
      NGT::StdOstreamRedirector redirector(logDisabled);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      auto *graphIndex = new NGT::GraphIndex(outIndexPath, false);
#else
      auto *graphIndex = new NGT::GraphIndex(outIndexPath, false, NGT::Index::OpenTypeObjectDisabled);
#endif
      std::cerr << "GraphOptimizer::execute:  vm size=" << NGT::Common::getProcessVmSizeStr() << ":"
                << NGT::Common::getProcessVmPeakStr() << std::endl;
      std::cerr << " delete all of objects" << std::endl;
      try {
        graphIndex->destructObjectSpace();
      } catch (NGT::Exception &err) {
        delete graphIndex;
        throw(err);
      }
      std::cerr << " vm size=" << NGT::Common::getProcessVmSizeStr() << ":"
                << NGT::Common::getProcessVmPeakStr() << std::endl;
      numOfOutgoingEdges =
          (numOfOutgoingEdges < 0 && graphIndex->NeighborhoodGraph::property.outgoingEdge >= 0)
              ? graphIndex->NeighborhoodGraph::property.outgoingEdge
              : numOfOutgoingEdges;
      numOfIncomingEdges =
          (numOfIncomingEdges < 0 && graphIndex->NeighborhoodGraph::property.incomingEdge >= 0)
              ? graphIndex->NeighborhoodGraph::property.incomingEdge
              : numOfIncomingEdges;
      if (numOfOutgoingEdges > 0 || numOfIncomingEdges > 0) {
        if (!logDisabled) {
          std::cerr << "GraphOptimizer: adjusting outgoing and incoming edges..." << std::endl;
        }
        redirector.begin();
        NGT::Timer timer;
        timer.start();
        std::vector<NGT::ObjectDistances> graph;
        try {
          std::cerr << "Optimizer::execute: Extract the graph data." << std::endl;
          // extract only edges from the index to reduce the memory usage.
          NGT::GraphReconstructor::extractGraph(graph, *graphIndex);
          NeighborhoodGraph::Property &prop = graphIndex->getGraphProperty();
          if (undirectedGraphConversion) {
            NGT::GraphReconstructor::convertToANNG(graph);
          }
          NGT::GraphReconstructor::reconstructGraph(graph, *graphIndex, numOfOutgoingEdges,
                                                    numOfIncomingEdges, maxNumOfEdges);
          timer.stop();
          std::cerr << "Optimizer::execute: Graph reconstruction time=" << timer.time << " (sec) "
                    << std::endl;
          graphIndex->saveGraph(outIndexPath);
          prop.graphType = NGT::NeighborhoodGraph::GraphTypeONNG;
          graphIndex->saveProperty(outIndexPath);
        } catch (NGT::Exception &err) {
          delete graphIndex;
          redirector.end();
          throw(err);
        }
      }

      if (shortcutReduction) {
        if (!logDisabled) {
          std::cerr << "GraphOptimizer: redusing shortcut edges..." << std::endl;
        }
        try {
          NGT::Timer timer;
          timer.start();
          if (shortcutReductionWithLessMemory) {
            NGT::GraphReconstructor::removeShortcutEdges(*graphIndex, outIndexPath, shortcutReductionRange,
                                                         numOfThreads, minNumOfEdges);
          } else {
            NGT::GraphReconstructor::adjustPathsEffectively(*graphIndex, minNumOfEdges);
          }
          timer.stop();
          std::cerr << "Optimizer::execute: Path adjustment time=" << timer.time << " (sec) " << std::endl;
        } catch (NGT::Exception &err) {
          delete graphIndex;
          redirector.end();
          throw(err);
        }
      }
      graphIndex->saveGraph(outIndexPath);
      delete graphIndex;
      redirector.end();
    }

    optimizeSearchParameters(outIndexPath);
  }

  void optimizeSearchParameters(const std::string outIndexPath) {

    if (searchParameterOptimization) {
      if (!logDisabled) {
        std::cerr << "GraphOptimizer: optimizing search parameters..." << std::endl;
      }
      NGT::Index outIndex(outIndexPath);
      NGT::GraphIndex &outGraph = static_cast<NGT::GraphIndex &>(outIndex.getIndex());
      NGT::Optimizer optimizer(outIndex);
      if (logDisabled) {
        optimizer.disableLog();
      } else {
        optimizer.enableLog();
      }
      try {
        auto coefficients = optimizer.adjustSearchEdgeSize(baseAccuracyRange, rateAccuracyRange, numOfQueries,
                                                           gtEpsilon, margin);
        NGT::NeighborhoodGraph::Property &prop = outGraph.getGraphProperty();
        prop.dynamicEdgeSizeBase               = coefficients.first;
        prop.dynamicEdgeSizeRate               = coefficients.second;
        prop.edgeSizeForSearch                 = -2;
        outGraph.saveProperty(outIndexPath);
      } catch (NGT::Exception &err) {
        std::stringstream msg;
        msg << "Optimizer::execute: Cannot adjust the search coefficients. " << err.what();
        NGTThrowException(msg);
      }
    }

    if (searchParameterOptimization || prefetchParameterOptimization || accuracyTableGeneration) {
      NGT::StdOstreamRedirector redirector(logDisabled);
      redirector.begin();
      NGT::Index outIndex(outIndexPath, true);
      NGT::GraphIndex &outGraph = static_cast<NGT::GraphIndex &>(outIndex.getIndex());
      if (prefetchParameterOptimization) {
        if (!logDisabled) {
          std::cerr << "GraphOptimizer: optimizing prefetch parameters..." << std::endl;
        }
        try {
          auto prefetch = adjustPrefetchParameters(outIndex);
          NGT::Property prop;
          outIndex.getProperty(prop);
          prop.prefetchOffset = prefetch.first;
          prop.prefetchSize   = prefetch.second;
          outIndex.setProperty(prop);
          outGraph.saveProperty(outIndexPath);
        } catch (NGT::Exception &err) {
          redirector.end();
          std::stringstream msg;
          msg << "Optimizer::execute: Cannot adjust prefetch parameters. " << err.what();
          NGTThrowException(msg);
        }
      }
      if (accuracyTableGeneration) {
        if (!logDisabled) {
          std::cerr << "GraphOptimizer: generating the accuracy table..." << std::endl;
        }
        try {
          auto table = NGT::Optimizer::generateAccuracyTable(outIndex, numOfResults, numOfQueries);
          NGT::Index::AccuracyTable accuracyTable(table);
          NGT::Property prop;
          outIndex.getProperty(prop);
          prop.accuracyTable = accuracyTable.getString();
          outIndex.setProperty(prop);
        } catch (NGT::Exception &err) {
          redirector.end();
          std::stringstream msg;
          msg << "Optimizer::execute: Cannot generate the accuracy table. " << err.what();
          NGTThrowException(msg);
        }
      }
      try {
        outGraph.saveProperty(outIndexPath);
        redirector.end();
      } catch (NGT::Exception &err) {
        redirector.end();
        std::stringstream msg;
        msg << "Optimizer::execute: Cannot save the index. " << outIndexPath << err.what();
        NGTThrowException(msg);
      }
    }
  }

  static std::tuple<size_t, double, double> // optimized # of edges, accuracy, accuracy gain per edge
  optimizeNumberOfEdgesForANNG(NGT::Optimizer &optimizer, std::vector<std::vector<float>> &queries,
                               size_t nOfResults, float targetAccuracy, size_t maxNoOfEdges) {

    NGT::Index &index = optimizer.index;
    std::stringstream queryStream;
    std::stringstream gtStream;
    float maxEpsilon = 0.0;

    optimizer.generatePseudoGroundTruth(queries, maxEpsilon, queryStream, gtStream);

    size_t nOfEdges     = 0;
    double accuracy     = 0.0;
    size_t prevEdge     = 0;
    double prevAccuracy = 0.0;
    double gain         = 0.0;
    {
      std::vector<NGT::ObjectDistances> graph;
      NGT::GraphReconstructor::extractGraph(graph, static_cast<NGT::GraphIndex &>(index.getIndex()));
      float epsilon = 0.0;
      for (size_t edgeSize = 5; edgeSize <= maxNoOfEdges; edgeSize += (edgeSize >= 10 ? 10 : 5)) {
        NGT::GraphReconstructor::reconstructANNGFromANNG(graph, index, edgeSize);
        NGT::Command::SearchParameters searchParameters;
        searchParameters.size           = nOfResults;
        searchParameters.outputMode     = 'e';
        searchParameters.edgeSize       = 0;
        searchParameters.beginOfEpsilon = searchParameters.endOfEpsilon = epsilon;
        queryStream.clear();
        queryStream.seekg(0, std::ios_base::beg);
        std::vector<NGT::Optimizer::MeasuredValue> acc;
        NGT::Optimizer::search(index, queryStream, gtStream, searchParameters, acc);
        if (acc.size() == 0) {
          NGTThrowException("Fatal error! Cannot get any accuracy value.");
        }
        accuracy = acc[0].meanAccuracy;
        nOfEdges = edgeSize;
        if (prevEdge != 0) {
          gain = (accuracy - prevAccuracy) / (edgeSize - prevEdge);
        }
        if (accuracy >= targetAccuracy) {
          break;
        }
        prevEdge     = edgeSize;
        prevAccuracy = accuracy;
      }
    }
    return std::make_tuple(nOfEdges, accuracy, gain);
  }

  static std::pair<size_t, float> optimizeNumberOfEdgesForANNG(NGT::Index &index,
                                                               ANNGEdgeOptimizationParameter &parameter) {
    if (parameter.targetNoOfObjects == 0) {
      parameter.targetNoOfObjects = index.getObjectRepositorySize();
    }

    NGT::Optimizer optimizer(index, parameter.noOfResults);

    NGT::ObjectRepository &objectRepository = index.getObjectSpace().getRepository();
    NGT::GraphIndex &graphIndex             = static_cast<NGT::GraphIndex &>(index.getIndex());
    NGT::GraphAndTreeIndex &treeIndex       = static_cast<NGT::GraphAndTreeIndex &>(index.getIndex());
    NGT::GraphRepository &graphRepository   = graphIndex.NeighborhoodGraph::repository;

    std::vector<std::vector<float>> queries;
    optimizer.extractAndRemoveRandomQueries(parameter.noOfQueries, queries);
    {
      graphRepository.deleteAll();
      treeIndex.DVPTree::deleteAll();
      treeIndex.DVPTree::insertNode(treeIndex.DVPTree::allocateLeafNode());
    }

    NGT::NeighborhoodGraph::Property &prop = graphIndex.getGraphProperty();
    prop.edgeSizeForCreation               = parameter.maxNoOfEdges;
    std::vector<std::pair<size_t, std::tuple<size_t, double, double>>> transition;
    size_t targetNo = 12500;
    for (; targetNo <= objectRepository.size() && targetNo <= parameter.noOfSampleObjects; targetNo *= 2) {
      ObjectID id        = 0;
      size_t noOfObjects = 0;
      for (id = 1; id < objectRepository.size(); ++id) {
        if (!objectRepository.isEmpty(id)) {
          noOfObjects++;
        }
        if (noOfObjects >= targetNo) {
          break;
        }
      }
      id++;
      index.createIndex(parameter.noOfThreads, id);
      auto edge = NGT::GraphOptimizer::optimizeNumberOfEdgesForANNG(
          optimizer, queries, parameter.noOfResults, parameter.targetAccuracy, parameter.maxNoOfEdges);
      transition.push_back(make_pair(noOfObjects, edge));
    }
    if (transition.size() < 2) {
      std::stringstream msg;
      msg << "Optimizer::optimizeNumberOfEdgesForANNG: Cannot optimize the number of edges. Too small object "
             "set. # of objects="
          << objectRepository.size() << " target No.=" << targetNo;
      NGTThrowException(msg);
    }
    double edgeRate     = 0.0;
    double accuracyRate = 0.0;
    for (auto i = transition.begin(); i != transition.end() - 1; ++i) {
      edgeRate += std::get<0>((*(i + 1)).second) - std::get<0>((*i).second);
      accuracyRate += std::get<1>((*(i + 1)).second) - std::get<1>((*i).second);
    }
    edgeRate /= (transition.size() - 1);
    accuracyRate /= (transition.size() - 1);
    size_t estimatedEdge    = std::get<0>(transition[0].second) +
                              edgeRate * (log2(parameter.targetNoOfObjects) - log2(transition[0].first));
    float estimatedAccuracy = std::get<1>(transition[0].second) +
                              accuracyRate * (log2(parameter.targetNoOfObjects) - log2(transition[0].first));
    if (estimatedAccuracy < parameter.targetAccuracy) {
      estimatedEdge += (parameter.targetAccuracy - estimatedAccuracy) / std::get<2>(transition.back().second);
      estimatedAccuracy = parameter.targetAccuracy;
    }
    if (estimatedEdge == 0) {
      std::stringstream msg;
      msg << "Optimizer::optimizeNumberOfEdgesForANNG: Cannot optimize the number of edges. " << estimatedEdge
          << ":" << estimatedAccuracy << " # of objects=" << objectRepository.size();
      NGTThrowException(msg);
    }

    return std::make_pair(estimatedEdge, estimatedAccuracy);
  }

  std::pair<size_t, float>
  optimizeNumberOfEdgesForANNG(const std::string indexPath,
                               GraphOptimizer::ANNGEdgeOptimizationParameter &parameter) {

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    NGTThrowException("Not implemented for NGT with the shared memory option.");
#endif

    NGT::StdOstreamRedirector redirector(logDisabled);
    redirector.begin();

    try {
      NGT::Index index(indexPath, false);

      auto optimizedEdge = NGT::GraphOptimizer::optimizeNumberOfEdgesForANNG(index, parameter);

      NGT::GraphIndex &graph = static_cast<NGT::GraphIndex &>(index.getIndex());
      size_t noOfEdges       = (optimizedEdge.first + 10) / 5 * 5;
      if (noOfEdges > parameter.maxNoOfEdges) {
        noOfEdges = parameter.maxNoOfEdges;
      }

      NGT::NeighborhoodGraph::Property &prop = graph.getGraphProperty();
      prop.edgeSizeForCreation               = noOfEdges;
      static_cast<NGT::GraphIndex &>(index.getIndex()).saveProperty(indexPath);
      optimizedEdge.first = noOfEdges;
      redirector.end();
      return optimizedEdge;
    } catch (NGT::Exception &err) {
      redirector.end();
      throw(err);
    }
  }

  void set(int outgoing, int incoming, int nofqs, int nofrs, float baseAccuracyFrom, float baseAccuracyTo,
           float rateAccuracyFrom, float rateAccuracyTo, double gte, double m) {
    set(outgoing, incoming, nofqs, nofrs);
    setExtension(baseAccuracyFrom, baseAccuracyTo, rateAccuracyFrom, rateAccuracyTo, gte, m);
  }

  void set(int outgoing, int incoming, int nofqs, int nofrs) {
    numOfOutgoingEdges = outgoing;
    numOfIncomingEdges = incoming;
    if (nofqs > 0) {
      numOfQueries = nofqs;
    }
    if (nofrs > 0) {
      numOfResults = nofrs;
    }
  }

  void setExtension(float baseAccuracyFrom, float baseAccuracyTo, float rateAccuracyFrom,
                    float rateAccuracyTo, double gte, double m) {
    if (baseAccuracyFrom > 0.0) {
      baseAccuracyRange.first = baseAccuracyFrom;
    }
    if (baseAccuracyTo > 0.0) {
      baseAccuracyRange.second = baseAccuracyTo;
    }
    if (rateAccuracyFrom > 0.0) {
      rateAccuracyRange.first = rateAccuracyFrom;
    }
    if (rateAccuracyTo > 0.0) {
      rateAccuracyRange.second = rateAccuracyTo;
    }
    if (gte >= -1.0) {
      gtEpsilon = gte;
    }
    if (m > 0.0) {
      margin = m;
    }
  }

  // obsolete because of a lack of a parameter
  void set(int outgoing, int incoming, int nofqs, float baseAccuracyFrom, float baseAccuracyTo,
           float rateAccuracyFrom, float rateAccuracyTo, double gte, double m) {
    if (outgoing >= 0) {
      numOfOutgoingEdges = outgoing;
    }
    if (incoming >= 0) {
      numOfIncomingEdges = incoming;
    }
    if (nofqs > 0) {
      numOfQueries = nofqs;
    }
    if (baseAccuracyFrom > 0.0) {
      baseAccuracyRange.first = baseAccuracyFrom;
    }
    if (baseAccuracyTo > 0.0) {
      baseAccuracyRange.second = baseAccuracyTo;
    }
    if (rateAccuracyFrom > 0.0) {
      rateAccuracyRange.first = rateAccuracyFrom;
    }
    if (rateAccuracyTo > 0.0) {
      rateAccuracyRange.second = rateAccuracyTo;
    }
    if (gte >= -1.0) {
      gtEpsilon = gte;
    }
    if (m > 0.0) {
      margin = m;
    }
  }

  void setProcessingModes(bool shortcut = true, bool searchParameter = true, bool prefetchParameter = true,
                          bool accuracyTable = true) {
    shortcutReduction             = shortcut;
    searchParameterOptimization   = searchParameter;
    prefetchParameterOptimization = prefetchParameter;
    accuracyTableGeneration       = accuracyTable;
  }

  int64_t numOfOutgoingEdges;
  int64_t numOfIncomingEdges;
  int64_t minNumOfEdges;
  int64_t maxNumOfEdges;
  std::pair<float, float> baseAccuracyRange;
  std::pair<float, float> rateAccuracyRange;
  size_t numOfQueries;
  size_t numOfResults;
  double gtEpsilon;
  double margin;
  bool logDisabled;
  bool shortcutReduction;
  bool searchParameterOptimization;
  bool prefetchParameterOptimization;
  bool accuracyTableGeneration;
  bool shortcutReductionWithLessMemory;
  bool undirectedGraphConversion;
  float shortcutReductionRange;
  size_t numOfThreads;
};

}; // namespace NGT
