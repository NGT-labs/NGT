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

#include "Quantizer.h"

#if defined(NGTQ_QBG) && !defined(NGTQ_SHARED_INVERTED_INDEX)

#include "NGT/NGTQ/QbgCli.h"
#include "NGT/NGTQ/Optimizer.h"
#include "NGT/NGTQ/HierarchicalKmeans.h"

typedef NGTQ::Quantizer::ObjectList QBGObjectList;

class QbgCliBuildParameters : public QBG::BuildParameters {
 public:
  QbgCliBuildParameters(NGT::Args &a) : args(a) { args.parse("ZUv"); }

  void getBuildParameters() {
    getHierarchicalClustringParameters();
    getOptimizationParameters();
  }

  void getCreationParameters() {
    creation.numOfObjects                   = args.getl("n", 0);
    creation.threadSize                     = args.getl("p", 24);
    creation.dimension                      = args.getl("d", 0);
    auto clusterDataType                    = args.getString("C", "-");
    creation.scalarQuantizationClippingRate = args.getf("r", 0.0);
    creation.scalarQuantizationNoOfSamples  = args.getl("V", 0);
#ifdef NGTQ_QBG
    creation.numOfLocalClusters = args.getl("c", 16);
#else
    creation.numOfLocalClusters = args.getl("c", 65000);
#endif
    creation.numOfSubvectors                  = args.getl("N", 0);
    creation.batchSize                        = args.getl("b", 1000);
    creation.localClusteringSampleCoefficient = args.getl("s", 10);
    {
      char localCentroidType       = args.getChar("T", 'f');
      creation.singleLocalCodebook = localCentroidType == 't' ? true : false;
    }
    {
      char centroidCreationMode = args.getChar("M", 'l');
      switch (centroidCreationMode) {
      case 'd': creation.centroidCreationMode = NGTQ::CentroidCreationModeDynamic; break;
      case 's': creation.centroidCreationMode = NGTQ::CentroidCreationModeStatic; break;
      case 'l': creation.centroidCreationMode = NGTQ::CentroidCreationModeStaticLayer; break;
      default:
        std::stringstream msg;
        msg << "Command::CreateParameters: Error: Invalid centroid creation mode. " << centroidCreationMode;
        NGTThrowException(msg);
      }
    }
    {
      char localCentroidCreationMode = args.getChar("L", 's');
      switch (localCentroidCreationMode) {
      case 'd': creation.localCentroidCreationMode = NGTQ::CentroidCreationModeDynamic; break;
      case 's': creation.localCentroidCreationMode = NGTQ::CentroidCreationModeStatic; break;
      case 'k': creation.localCentroidCreationMode = NGTQ::CentroidCreationModeDynamicKmeans; break;
      default:
        std::stringstream msg;
        msg << "Command::CreateParameters: Error: Invalid centroid creation mode. "
            << localCentroidCreationMode;
        NGTThrowException(msg);
      }
    }
#ifdef NGTQ_QBG
    creation.localIDByteSize = args.getl("B", 1);
#endif

    creation.globalEdgeSizeForCreation = args.getl("E", 100);
    creation.globalEdgeSizeForSearch   = args.getl("S", 40);
    {
      char indexType           = args.getChar("i", 't');
      creation.globalIndexType = indexType == 't' ? NGT::Property::GraphAndTree : NGT::Property::Graph;
      creation.localIndexType  = creation.globalIndexType;
    }
    creation.globalInsertionRadiusCoefficient = args.getf("e", 0.1) + 1.0;
    creation.localInsertionRadiusCoefficient  = creation.globalInsertionRadiusCoefficient;

    transform(clusterDataType.begin(), clusterDataType.end(), clusterDataType.begin(), ::tolower);
    if (clusterDataType == "-" || clusterDataType == "pq4") {
      creation.localClusterDataType = NGTQ::ClusterDataTypePQ4;
    } else if (clusterDataType == "sqsu8" || clusterDataType == "sqs8" || clusterDataType == "sq8") {
      creation.localClusterDataType = NGTQ::ClusterDataTypeSQSU8;
    } else if (clusterDataType == "nq") {
      creation.localClusterDataType = NGTQ::ClusterDataTypeNQ;
    }

    char objectType = args.getChar("o", 'f');
    {
      switch (objectType) {
      case 'f': creation.dataType = NGTQ::DataTypeFloat; break;
#ifdef NGT_HALF_FLOAT
      case 'h': creation.dataType = NGTQ::DataTypeFloat16; break;
#endif
      case 'c': creation.dataType = NGTQ::DataTypeUint8; break;
      default:
        std::stringstream msg;
        msg << "Command::CreateParameters: Error: Invalid object type. " << objectType;
        NGTThrowException(msg);
      }
    }
    {
      std::string globalObjectType = args.getString("K", "-");
      std::string objectType;
      if (globalObjectType == "-") {
        if (clusterDataType == "-" || clusterDataType == "pq4") {
          objectType = "f";
        } else {
          objectType = clusterDataType;
        }
      } else {
        objectType = globalObjectType;
      }
      if (objectType == "f") {
        creation.globalObjectType = NGT::ObjectSpace::ObjectType::Float;
      } else if (objectType == "h") {
        creation.globalObjectType = NGT::ObjectSpace::ObjectType::Float16;
      } else if (clusterDataType == "sqsu8" || clusterDataType == "sqs8" || clusterDataType == "sq8") {
        creation.globalObjectType = NGT::ObjectSpace::ObjectType::Qsuint8;
      } else {
        std::stringstream msg;
        msg << "Command::CreateParameters: Error: Invalid global object type. " << objectType;
        NGTThrowException(msg);
      }
    }

    {
      char distanceType = args.getChar("D", '2');
      switch (distanceType) {
      case '2': creation.distanceType = NGTQ::DistanceType::DistanceTypeL2; break;
      case '1': creation.distanceType = NGTQ::DistanceType::DistanceTypeL1; break;
      case 'a': creation.distanceType = NGTQ::DistanceType::DistanceTypeAngle; break;
      case 'C': creation.distanceType = NGTQ::DistanceType::DistanceTypeNormalizedCosine; break;
      case 'E': creation.distanceType = NGTQ::DistanceType::DistanceTypeL2; break;
      case 'i': creation.distanceType = NGTQ::DistanceType::DistanceTypeInnerProduct; break;
      default:
        std::stringstream msg;
        msg << "Command::CreateParameters: Error: Invalid distance type. " << distanceType;
        NGTThrowException(msg);
      }
    }
#ifdef NGTQ_QBG
    creation.genuineDimension = creation.dimension;
    creation.dimension        = args.getl("P", 0);
    if (creation.dimension == 0) {
      creation.dimension = ((creation.genuineDimension + 15) / 16) * 16;
    }
    creation.dimensionOfSubvector = args.getl("Q", 0);
    if (creation.numOfSubvectors == 0 && (creation.localClusterDataType == NGTQ::ClusterDataTypeSQSU8)) {
      creation.numOfSubvectors = creation.dimension;
    }
    {
      char oType = args.getChar("O", objectType);
      switch (oType) {
      case 'f': creation.genuineDataType = ObjectFile::DataTypeFloat; break;
#ifdef NGT_HALF_FLOAT
      case 'h': creation.genuineDataType = ObjectFile::DataTypeFloat16; break;
#endif
      case 'c': creation.genuineDataType = ObjectFile::DataTypeUint8; break;
      default:
        std::stringstream msg;
        msg << "Command::CreateParameters: Error: Invalid genuine data type. " << objectType;
        NGTThrowException(msg);
      }
    }
#endif
    {
      auto refinementDataType = args.getString("R", "-");
      std::vector<std::string> tokens;
      NGT::Common::tokenize(refinementDataType, tokens, ":,");
      creation.refinementDataType = NGTQ::DataTypeNone;
      if (tokens.size() >= 1 && tokens[0].size() >= 1) {
        switch (tokens[0][0]) {
        case 'f': creation.refinementDataType = NGTQ::DataTypeFloat; break;
#ifdef NGT_HALF_FLOAT
        case 'h': creation.refinementDataType = NGTQ::DataTypeFloat16; break;
#endif
        case '-': creation.refinementDataType = NGTQ::DataTypeNone; break;
        default:
          std::stringstream msg;
          msg << "Command::CreateParameters: Error: Invalid refinement data type. " << refinementDataType;
          NGTThrowException(msg);
        }
      }
      if (tokens.size() >= 2 && tokens[1].size() >= 1) {
        switch (tokens[1][0]) {
        case 'O':
        case 'o': creation.orderedSearchRanking = true; break;
        default: creation.orderedSearchRanking = false; break;
        }
      }
    }
#ifdef NGTQ_QUANTIZED_TREE
    creation.maxObjectsPerNode = args.getl("k", -1);
#endif
  }

  void getHierarchicalClustringParameters() {
    hierarchicalClustering.maxSize       = args.getl("r", 1000);
    hierarchicalClustering.numOfObjects  = args.getl("O", 0);
    hierarchicalClustering.numOfClusters = args.getl("E", 2);
    try {
      hierarchicalClustering.numOfTotalClusters = args.getl("C", 0);
    } catch (...) {
      hierarchicalClustering.numOfTotalClusters = 0;
    }
    hierarchicalClustering.numOfTotalBlobs = args.getl("b", 0);
    hierarchicalClustering.clusterID       = args.getl("c", -1);
    hierarchicalClustering.verbose         = args.getBool("v");

    char iMode                      = args.getChar("i", '-');
    hierarchicalClustering.initMode = NGT::Clustering::InitializationModeKmeansPlusPlus;
    switch (iMode) {
    case 'l':
    case 'h': hierarchicalClustering.initMode = NGT::Clustering::InitializationModeHead; break;
    case 'r': hierarchicalClustering.initMode = NGT::Clustering::InitializationModeRandom; break;
    case 'R': hierarchicalClustering.initMode = NGT::Clustering::InitializationModeRandomFixedSeed; break;
    case 'P':
      hierarchicalClustering.initMode = NGT::Clustering::InitializationModeKmeansPlusPlusFixedSeed;
      break;
    default:
    case '-':
    case 'p': hierarchicalClustering.initMode = NGT::Clustering::InitializationModeKmeansPlusPlus; break;
    }

    hierarchicalClustering.numOfRandomObjects = args.getl("r", 0);
    char rmode                                = args.getChar("R", '-');
    if (rmode == 'c') {
      hierarchicalClustering.extractCentroid = true;
    } else {
      hierarchicalClustering.extractCentroid = false;
    }

    hierarchicalClustering.expectedRecall = args.getf("A", 0.98);

    hierarchicalClustering.numOfFirstObjects   = 0;
    hierarchicalClustering.numOfFirstClusters  = 0;
    hierarchicalClustering.numOfSecondObjects  = 0;
    hierarchicalClustering.numOfSecondClusters = 0;
    hierarchicalClustering.numOfThirdClusters  = 0;
    hierarchicalClustering.numOfThirdObjects   = 0;

    std::string blob = args.getString("B", "-");

    if (blob != "-") {
      std::vector<std::string> tokens;
      NGT::Common::tokenize(blob, tokens, ",");
      size_t idx = 0;
      if (tokens.size() > 3) {
        if (tokens[idx] == "3") {
          hierarchicalClustering.clusteringType = QBG::HierarchicalKmeans::ClusteringTypeThreeLayer;
        } else if (tokens[idx] == "21") {
          hierarchicalClustering.clusteringType = QBG::HierarchicalKmeans::ClusteringTypeTwoPlusOneLayer;
        } else if (tokens[idx] == "21N") {
          hierarchicalClustering.clusteringType =
              QBG::HierarchicalKmeans::ClusteringTypeTwoPlusOneLayerWithNGT;
        } else if (tokens[idx] == "22") {
          hierarchicalClustering.clusteringType = QBG::HierarchicalKmeans::ClusteringTypeTwoPlusTwoLayer;
        } else {
          std::stringstream msg;
          msg << "invalid clustering type. " << tokens[idx];
          NGTThrowException(msg);
        }
        idx++;
      } else {
        hierarchicalClustering.clusteringType = QBG::HierarchicalKmeans::ClusteringTypeThreeLayer;
      }
      if (tokens.size() > idx) {
        std::vector<std::string> ftokens;
        NGT::Common::tokenize(tokens[idx], ftokens, ":");
        if (ftokens.size() >= 1) {
          hierarchicalClustering.numOfFirstObjects = NGT::Common::strtof(ftokens[0]);
        }
        if (ftokens.size() >= 2) {
          hierarchicalClustering.numOfFirstClusters = NGT::Common::strtof(ftokens[1]);
        }
        idx++;
      }
      if (tokens.size() > idx) {
        std::vector<std::string> ftokens;
        NGT::Common::tokenize(tokens[idx], ftokens, ":");
        if (ftokens.size() >= 1) {
          hierarchicalClustering.numOfSecondObjects = NGT::Common::strtof(ftokens[0]);
        }
        if (ftokens.size() >= 2) {
          hierarchicalClustering.numOfSecondClusters = NGT::Common::strtof(ftokens[1]);
        }
        idx++;
      }
      if (tokens.size() > idx) {
        std::vector<std::string> ftokens;
        NGT::Common::tokenize(tokens[idx], ftokens, ":");
        if (ftokens.size() >= 1) {
          if (ftokens[0] == "" || ftokens[0] == "-") {
            hierarchicalClustering.numOfThirdObjects = 0;
          } else {
            hierarchicalClustering.numOfThirdObjects = NGT::Common::strtof(ftokens[0]);
          }
        }
        if (ftokens.size() >= 2) {
          hierarchicalClustering.numOfThirdClusters = NGT::Common::strtof(ftokens[1]);
        }
      }
    }
  }

  void getOptimizationParameters() {
    optimization.numOfObjects    = args.getl("o", 1000);
    optimization.numOfClusters   = args.getl("n", 0);
    optimization.numOfSubvectors = args.getl("m", 0);

    optimization.randomizedObjectExtraction = true;

#ifdef NGT_CLUSTERING
    string cType;
    try {
      cType = args.getString("C", "k");
    } catch (...) {
    }

    optimization.clusteringType = NGT::Clustering::ClusteringTypeKmeansWithNGT;
    if (cType == "k") {
      optimization.clusteringType = NGT::Clustering::ClusteringTypeKmeansWithoutNGT;
    } else if (cType == "KS") {
      optimization.clusteringType = NGT::Clustering::ClusteringTypeKmeansWithNGT;
    } else if (cType == "i") {
      optimization.clusteringType = NGT::Clustering::ClusteringTypeKmeansWithIteration;
    } else {
      std::stringstream msg;
      msg << "invalid clustering type. " << cType;
      NGTThrowException(msg);
    }
#else
    char clusteringType;
    try {
      clusteringType = args.getChar("C", 'k');
    } catch (...) {
    }
#endif

#ifdef NGT_CLUSTERING
    char iMode            = args.getChar("i", '-');
    optimization.initMode = NGT::Clustering::InitializationModeKmeansPlusPlus;
    switch (iMode) {
    case 'h': optimization.initMode = NGT::Clustering::InitializationModeHead; break;
    case 'r': optimization.initMode = NGT::Clustering::InitializationModeRandom; break;
    case 'p': optimization.initMode = NGT::Clustering::InitializationModeKmeansPlusPlus; break;
    case 'R': optimization.initMode = NGT::Clustering::InitializationModeRandomFixedSeed; break;
    case 'P': optimization.initMode = NGT::Clustering::InitializationModeKmeansPlusPlusFixedSeed; break;
    default:
    case '-':
    case 'b': optimization.initMode = NGT::Clustering::InitializationModeBest; break;
    }
#else
    optimization.initMode = args.getChar("i", '-');
#endif

    optimization.convergenceLimitTimes = args.getl("c", 5);
    optimization.iteration             = args.getl("t", 100);
    optimization.clusterIteration      = args.getl("I", 100);

    optimization.clusterSizeConstraint = false;
    if (args.getChar("s", 'f') == 't') {
      optimization.clusterSizeConstraintCoefficient = 5.0;
      optimization.clusterSizeConstraint            = true;
    } else if (args.getChar("s", 'f') == 'f') {
      optimization.clusterSizeConstraint = false;
    } else {
      optimization.clusterSizeConstraint            = true;
      optimization.clusterSizeConstraintCoefficient = args.getf("s", 5.0);
    }

    optimization.numOfMatrices     = args.getl("M", 2);
    optimization.seedNumberOfSteps = args.getf("S", 2);
    optimization.seedStep          = args.getl("X", 10);
    optimization.reject            = args.getf("R", 0.9);
    optimization.timelimit         = args.getf("L", 24 * 1);
    optimization.timelimit *= 60.0 * 60.0;
    optimization.showClusterInfo = args.getBool("Z");
    optimization.unifiedPQ       = args.getBool("U");
    optimization.verbose         = args.getBool("v");

#ifdef NGTQG_NO_ROTATION
    char positionMode = args.getChar("P", 'n');
#else
    char positionMode = args.getChar("P", 'r');
#endif
    switch (positionMode) {
    case 'r':
      optimization.rotation      = true;
      optimization.repositioning = false;
      break;
    case 'R':
      optimization.rotation      = true;
      optimization.repositioning = true;
      break;
    case 'p':
      optimization.rotation      = false;
      optimization.repositioning = true;
      break;
    case 'n':
    default: optimization.rotation = false; optimization.repositioning = false;
    }
    char globalType = args.getChar("G", '-');
    switch (globalType) {
    case 'z': optimization.globalType = QBG::Optimizer::GlobalTypeZero; break;
    case 'm': optimization.globalType = QBG::Optimizer::GlobalTypeMean; break;
    default:
    case 'n':
      optimization.globalType = QBG::Optimizer::GlobalTypeNone;
      break;
      break;
    }
  }

 protected:
  NGT::Args &args;
};

class SearchParameters : public NGT::Command::SearchParameters {
 public:
  SearchParameters(NGT::Args &args) : NGT::Command::SearchParameters(args, "0.02") {
    stepOfResultExpansion       = 2;
    std::string resultExpansion = args.getString("p", "3.0");
    std::vector<std::string> tokens;
    NGT::Common::tokenize(resultExpansion, tokens, ":");
    if (tokens.size() >= 1) {
      beginOfResultExpansion = endOfResultExpansion = NGT::Common::strtod(tokens[0]);
    }
    if (tokens.size() >= 2) {
      endOfResultExpansion = NGT::Common::strtod(tokens[1]);
    }
    if (tokens.size() >= 3) {
      stepOfResultExpansion = NGT::Common::strtod(tokens[2]);
    }
  }
  float beginOfResultExpansion;
  float endOfResultExpansion;
  float stepOfResultExpansion;
};

void QBG::CLI::buildQG(NGT::Args &args) {
  const std::string usage = "Usage: qbg build-qg [-Q dimension-of-subvector] [-E max-number-of-edges] index";

  QbgCliBuildParameters buildParameters(args);
  buildParameters.getBuildParameters();

  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  size_t phase         = args.getl("p", 0);
  size_t maxNumOfEdges = args.getl("E", 128);

  const std::string qgPath = indexPath + "/qg";

  if (phase == 0 || phase == 1) {
    QBG::Optimizer optimizer(buildParameters);
    optimizer.globalType = QBG::Optimizer::GlobalTypeZero;

#ifdef NGTQG_NO_ROTATION
    if (optimizer.rotation || optimizer.repositioning) {
      std::cerr << "build-qg: Warning! Although rotation or repositioning is specified, turn off rotation "
                   "and repositioning because of unavailable options."
                << std::endl;
      optimizer.rotation      = false;
      optimizer.repositioning = false;
    }
#endif

    std::cerr << "optimizing..." << std::endl;
    optimizer.optimize(qgPath);
  }
  auto verbose = buildParameters.optimization.verbose;
  if (phase == 0 || phase == 2) {
    std::cerr << "building the inverted index..." << std::endl;
    QBG::Index::buildNGTQ(qgPath, verbose);
  }
  if (phase == 0 || phase == 3) {
    std::cerr << "building the quantized graph... " << std::endl;
    NGTQG::Index::realign(indexPath, maxNumOfEdges, verbose);
  }
}

void searchQG(NGTQG::Index &index, SearchParameters &searchParameters, ostream &stream) {

  std::ifstream is(searchParameters.query);
  if (!is) {
    std::cerr << "Cannot open the specified file. " << searchParameters.query << std::endl;
    return;
  }

  if (searchParameters.outputMode[0] == 'e') {
    stream << "# Beginning of Evaluation" << endl;
  }

  string line;
  double totalTime  = 0;
  size_t queryCount = 0;

  while (getline(is, line)) {
    if (searchParameters.querySize > 0 && queryCount >= searchParameters.querySize) {
      break;
    }
    vector<float> query;
    stringstream linestream(line);
    while (!linestream.eof()) {
      float value;
      linestream >> value;
      if (linestream.fail()) {
        NGTThrowException("NGTQG: invalid stream.");
      }
      query.push_back(value);
    }
    queryCount++;
    float beginOfParam          = 0;
    float endOfParam            = 0;
    float stepOfParam           = FLT_MAX;
    bool resultExpansionEnabled = true;
    if (searchParameters.beginOfResultExpansion != searchParameters.endOfResultExpansion) {
      beginOfParam           = searchParameters.beginOfResultExpansion;
      endOfParam             = searchParameters.endOfResultExpansion;
      stepOfParam            = searchParameters.stepOfResultExpansion;
      resultExpansionEnabled = true;
    } else if (searchParameters.beginOfEpsilon != searchParameters.endOfEpsilon) {
      beginOfParam           = searchParameters.beginOfEpsilon;
      endOfParam             = searchParameters.endOfEpsilon;
      stepOfParam            = searchParameters.stepOfEpsilon;
      resultExpansionEnabled = false;
    }
    for (float param = beginOfParam; param <= endOfParam; param += stepOfParam) {
      NGTQG::SearchQuery searchQuery(query);
      NGT::ObjectDistances objects;
      searchQuery.setResults(&objects);
      searchQuery.setSize(searchParameters.size);
      searchQuery.setRadius(searchParameters.radius);
      float epsilon, resultExpansion;
      if (stepOfParam == FLT_MAX) {
        resultExpansion = searchParameters.beginOfResultExpansion;
        epsilon         = searchParameters.beginOfEpsilon;
      } else if (resultExpansionEnabled) {
        resultExpansion = param;
        epsilon         = searchParameters.beginOfEpsilon;
      } else {
        resultExpansion = searchParameters.beginOfResultExpansion;
        epsilon         = param;
      }
      searchQuery.setResultExpansion(resultExpansion);
      searchQuery.setEpsilon(epsilon);
      searchQuery.setEdgeSize(searchParameters.edgeSize);
      NGT::Timer timer;
      switch (searchParameters.indexType) {
      case 't':
        timer.start();
        index.NGTQG::Index::search(searchQuery);
        timer.stop();
        break;
      case 'Q':
        timer.start();
        index.NGTQG::Index::searchUsingDistanceSorter(searchQuery);
        timer.stop();
        break;
      case 's':
        timer.start();
        index.linearSearch(searchQuery);
        timer.stop();
        break;
      default:
        timer.start();
        index.NGTQG::Index::searchClassic(searchQuery);
        timer.stop();
        break;
      }
      totalTime += timer.time;
      if (searchParameters.outputMode[0] == 'e') {
        stream << "# Query No.=" << queryCount << endl;
        stream << "# Query=" << line.substr(0, 20) + " ..." << endl;
        stream << "# Index Type=" << searchParameters.indexType << endl;
        stream << "# Size=" << searchParameters.size << endl;
        stream << "# Radius=" << searchParameters.radius << endl;
        stream << "# Epsilon*=" << epsilon << endl;
        stream << "# Result Expansion*=" << resultExpansion << endl;
        stream << "# Factor=" << param << endl;
        stream << "# Query Time (msec)=" << timer.time * 1000.0 << endl;
        stream << "# Distance Computation=" << searchQuery.distanceComputationCount << endl;
        stream << "# VM Peak=" << NGT::Common::getProcessVmPeakStr() << endl;
        stream << "# Visit Count=" << searchQuery.visitCount << endl;
      } else {
        stream << "Query No." << queryCount << endl;
        stream << "Rank\tID\tDistance" << endl;
      }
      for (size_t i = 0; i < objects.size(); i++) {
        stream << i + 1 << "\t" << objects[i].id << "\t";
        stream << objects[i].distance << endl;
      }
      if (searchParameters.outputMode[0] == 'e') {
        stream << "# End of Search" << endl;
      } else {
        stream << "Query Time= " << timer.time << " (sec), " << timer.time * 1000.0 << " (msec)" << endl;
      }
    }
    if (searchParameters.outputMode[0] == 'e') {
      stream << "# End of Query" << endl;
    }
  }
  if (searchParameters.outputMode[0] == 'e') {
    stream << "# Average Query Time (msec)=" << totalTime * 1000.0 / (double)queryCount << endl;
    stream << "# Number of queries=" << queryCount << endl;
    stream << "# End of Evaluation" << endl;
  } else {
    stream << "Average Query Time= " << totalTime / (double)queryCount << " (sec), "
           << totalTime * 1000.0 / (double)queryCount << " (msec), (" << totalTime << "/" << queryCount << ")"
           << endl;
  }
}

void QBG::CLI::searchQG(NGT::Args &args) {
  const string usage =
      "Usage: qbg search-qg [-i index-type(g|t|s)] [-n result-size] [-e epsilon] [-E edge-size] "
      "[-o output-mode] [-p result-expansion] index(input) query.tsv(input)";

  args.parse("v");

  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    ;
    msg << usage << endl;
    NGTThrowException(msg);
  }

  SearchParameters searchParameters(args);

  bool readOnly = true;
  NGTQG::Index index(indexPath, 128, readOnly);

  if (debugLevel >= 1) {
    std::cerr << "indexType=" << searchParameters.indexType << std::endl;
    std::cerr << "size=" << searchParameters.size << std::endl;
    std::cerr << "edgeSize=" << searchParameters.edgeSize << std::endl;
    std::cerr << "epsilon=" << searchParameters.beginOfEpsilon << "<->" << searchParameters.endOfEpsilon
              << "," << searchParameters.stepOfEpsilon << std::endl;
    std::cerr << "VM size=" << NGT::Common::getProcessVmSizeStr() << std::endl;
    std::cerr << "VM peak=" << NGT::Common::getProcessVmPeakStr() << std::endl;
  }

  try {
    ::searchQG(index, searchParameters, std::cout);
  } catch (NGT::Exception &err) {
    std::stringstream msg;
    msg << "qbg: Error " << err.what() << std::endl;
    msg << usage << endl;
    NGTThrowException(msg);
  } catch (std::exception &err) {
    std::stringstream msg;
    msg << "qbg: Error " << err.what() << std::endl;
    msg << usage << endl;
    NGTThrowException(msg);
  } catch (...) {
    std::stringstream msg;
    msg << "qbg: Error ";
    msg << usage << endl;
    NGTThrowException(msg);
  }
}

#ifdef NGTQ_QUANTIZED_TREE
void quantizeTree(const std::string &indexPath, size_t maxObjectsPerNode, bool verbose) {
  NGT::Index index(indexPath);

  NGT::GraphAndTreeIndex &graphIndex = static_cast<NGT::GraphAndTreeIndex &>(index.getIndex());

  // Check if already quantized or not ready
  size_t objectCount    = index.getObjectRepositorySize();
  size_t graphNodeCount = graphIndex.getGraphRepositorySize();
  if (objectCount < graphNodeCount) {
    std::cerr << "quantizeTree: already quantized. objectCount=" << objectCount
              << " graphNodeCount=" << graphNodeCount << std::endl;
    std::string quantizedIndexPath = indexPath + "/" + NGTQG::Index::getQGDirectoryName();
    NGTQ::Index quantizedIndex(quantizedIndexPath);
    quantizedIndex.getQuantizer().property.treeQuantization = true;
    quantizedIndex.save();
    return;
  }
  if (objectCount > graphNodeCount) {
    std::cerr << "quantizeTree: graph index is not yet generated. objectCount=" << objectCount
              << " graphNodeCount=" << graphNodeCount << std::endl;
    return;
  }

  auto &leafNodes = graphIndex.DVPTree::leafNodes;

  std::cerr << "# of leaf nodes = " << leafNodes.size() << std::endl;

  size_t graphSize = graphIndex.getGraphRepositorySize() - 1;
  std::cerr << "Current graph size = " << graphSize << std::endl;

  for (size_t i = 1; i < leafNodes.size(); i++) {
    if (leafNodes[i] != 0) {
      NGT::LeafNode *leaf          = leafNodes[i];
      NGT::ObjectDistance *objects = leaf->getObjectIDs();
      int objectSize               = leaf->objectSize;

      int limitedSize = (maxObjectsPerNode > 0 && objectSize > (int)maxObjectsPerNode)
                            ? (int)maxObjectsPerNode
                            : objectSize;
      NGT::ObjectDistances neighbors;
      neighbors.reserve(limitedSize);
      for (int j = 0; j < limitedSize; j++) {
        neighbors.push_back(NGT::ObjectDistance(objects[j].id, objects[j].distance));
      }

      NGT::ObjectID newNodeId = graphSize + i;
      static_cast<NGT::NeighborhoodGraph &>(graphIndex).repository.insert(newNodeId, neighbors);

      if (verbose && i < 5) {
        std::cerr << "  Leaf[" << i << "] objectSize=" << objectSize << "(limited to " << limitedSize << ")"
                  << " -> GraphNode[" << newNodeId << "]" << std::endl;
      }
    }
  }

  std::cerr << "New graph size = " << graphIndex.getGraphRepositorySize() << std::endl;

  index.save();
  index.close();

  {
    std::string quantizedIndexPath = indexPath + "/" + NGTQG::Index::getQGDirectoryName();
    NGTQ::Index quantizedIndex(quantizedIndexPath);
    quantizedIndex.getQuantizer().property.treeQuantization = true;
    quantizedIndex.save();
  }
}
#endif

void QBG::CLI::createQG(NGT::Args &args) {
  const std::string usage =
      "Usage: qbg create-qg [-Q dimension-of-subvector] [-k max-objects-per-node] index";

  QbgCliBuildParameters buildParameters(args);
  buildParameters.getCreationParameters();

  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }

  bool verbose = args.getBool("v");

  std::cerr << "creating..." << std::endl;
  NGTQG::Index::create(indexPath, buildParameters);
  std::cerr << "appending..." << std::endl;
  NGTQG::Index::append(indexPath, buildParameters);

#ifdef NGTQ_QUANTIZED_TREE
  if (buildParameters.creation.maxObjectsPerNode >= 0) {
    std::cerr << "quantizing tree..." << std::endl;
    ::quantizeTree(indexPath, static_cast<size_t>(buildParameters.creation.maxObjectsPerNode), verbose);
  }
#endif
}

void QBG::CLI::appendQG(NGT::Args &args) {
  const std::string usage = "Usage: qbg append-qbg ngt-index";
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }
  QBG::Index::appendFromObjectRepository(indexPath, indexPath + "/" + NGTQG::Index::getQGDirectoryName(),
                                         false);
}

#ifdef NGTQ_QUANTIZED_TREE
void QBG::CLI::quantizeTree(NGT::Args &args) {
  const std::string usage = "Usage: qbg quantize-tree [-k max-objects-per-node] index";

  args.parse("v");
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }

  bool verbose             = args.getBool("v");
  size_t maxObjectsPerNode = args.getl("k", 32);

  try {
    ::quantizeTree(indexPath, maxObjectsPerNode, verbose);
  } catch (NGT::Exception &err) {
    std::stringstream msg;
    msg << "Error. " << err.what() << std::endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }
}
#endif

#ifdef NGTQ_OBGRAPH
void QBG::CLI::setBlobID(NGT::Args &args) {
  const string usage = "Usage: qbg set-blob-id index";

  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  string ngtPath;
  try {
    ngtPath = args.get("#2");
  } catch (...) {
  }

  float resultExpansion             = args.getf("p", 0.0);
  size_t nOfProbes                  = args.getl("P", 10);
  float epsilon                     = args.getf("e", 0.1);
  float blobEpsilon                 = args.getf("B", 0.05);
  size_t outEdge                    = args.getl("o", 5);
  size_t inEdge                     = args.getl("i", 0);
  size_t inRank                     = args.getl("I", 0);
  size_t explorationSize            = args.getf("N", 256);
  float threshold                   = args.getf("T", 0.5);
  char mode                         = args.getChar("m", '-');
  NGTQ::DataType refinementDataType = NGTQ::DataTypeAny;
  {
    char refinement = args.getChar("R", '-');
    switch (refinement) {
    case 'f': refinementDataType = NGTQ::DataTypeFloat; break;
#ifdef NGT_HALF_FLOAT
    case 'h': refinementDataType = NGTQ::DataTypeFloat16; break;
#endif
    case 'x': refinementDataType = NGTQ::DataTypeNone; break;
    case '-': refinementDataType = NGTQ::DataTypeAny; break;
    default:
      std::stringstream msg;
      msg << "Command::CreateParameters: Error: Invalid refinement data type. " << refinement;
      NGTThrowException(msg);
    }
  }

  if (!ngtPath.empty()) {
    QBG::Index::setBlobIDsFromGraph(indexPath, ngtPath, refinementDataType);
  } else {
    QBG::SearchContainer searchContainer;
    searchContainer.setRefinementExpansion(resultExpansion);
    searchContainer.setNumOfProbes(nOfProbes);
    searchContainer.setEpsilon(epsilon);
    searchContainer.setBlobEpsilon(blobEpsilon);
    searchContainer.setGraphExplorationSize(explorationSize);
    if (mode == '-') {
      QBG::Index::setBlobIDsFromONNGBySearch(indexPath, searchContainer, outEdge, inEdge, inRank,
                                             refinementDataType);
    } else {
      QBG::Index::setBlobIDsFromExtGraphBySearch(indexPath, searchContainer, outEdge, inEdge, threshold,
                                                 refinementDataType);
    }
  }
}
#endif

void QBG::CLI::info(NGT::Args &args) {
  const string usage = "Usage: qbg index";

  std::string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }

  try {
    bool readOnly = true;
    try {
      QBG::Index index(indexPath, readOnly);
    } catch (NGT::Exception &err) {
      readOnly = false;
    }
    QBG::Index index(indexPath, readOnly);
    auto &quantizer = index.getQuantizer();
    std::cout << "The index type: QBG" << std::endl;
    std::cout << "# of the dimensions: " << quantizer.globalCodebookIndex.getObjectSpace().getDimension()
              << std::endl;
    std::cout << "# of the padded dimensions: "
              << quantizer.globalCodebookIndex.getObjectSpace().getPaddedDimension() << std::endl;
    std::cout << "# of the stored objects: "
              << (quantizer.objectList.size() == 0 ? 0 : quantizer.objectList.size() - 1) << std::endl;
  } catch (NGT::Exception &err) {
    bool readOnly = true;
    try {
      NGTQG::Index index(indexPath, 128, readOnly);
      std::cout << "The index type: QG" << std::endl;
    } catch (...) {
      std::stringstream msg;
      msg << "qbg: The specified index is neither QBG nor QG." << std::endl;
      msg << usage << endl;
      NGTThrowException(msg);
    }
  }
}

void QBG::CLI::create(NGT::Args &args) {
  const string usage = "Usage: qbg create "
                       " -d dimension [-o object-type (object-list-data-type)] "
                       "[-O genuine-data-type (=object-type)] [-C cluster-data-type] [-K graph-data-type] "
                       "[-D distance-function] [-n data-size] "
                       "[-p #-of-thread] [-R global-codebook-range] [-r local-codebook-range] "
                       "[-c local-codebook-size-limit] [-N local-division-no] "
                       "[-T single-local-centroid (t|f)] [-e epsilon] [-i index-type (t:Tree|g:Graph)] "
                       "[-M global-centroid-creation-mode (d|s)] [-L local-centroid-creation-mode (d|k|s)] "
                       "[-s local-sample-coefficient] "
                       "index(OUT) data.tsv(IN) rotation(IN)";

  try {
    cerr << "qbg: Create" << endl;
    QbgCliBuildParameters buildParameters(args);
    buildParameters.getCreationParameters();

    std::vector<float> r;
    auto *rotation = &r;
    {
      try {
        std::string rotationPath = args.get("#3");
        cerr << "rotation is " << rotationPath << "." << endl;
        std::ifstream stream(rotationPath);
        if (!stream) {
          std::stringstream msg;
          msg << "Cannot open the rotation. " << rotationPath << std::endl;
          msg << usage << std::endl;
          NGTThrowException(msg);
        }
        std::string line;
        while (getline(stream, line)) {
          std::vector<std::string> tokens;
          NGT::Common::tokenize(line, tokens, " \t");
          for (auto &token : tokens) {
            r.push_back(NGT::Common::strtof(token));
          }
        }
      } catch (...) {
        rotation = 0;
      }
      std::cerr << "rotation matrix size=" << r.size() << std::endl;
    }
    std::string indexPath = args.get("#1");
    std::string objectPath;
    try {
      objectPath = args.get("#2");
    } catch (...) {
    }

    QBG::Index::create(indexPath, buildParameters, rotation, objectPath);
  } catch (NGT::Exception &err) {
    std::stringstream msg;
    msg << err.what() << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }
}

void QBG::CLI::load(NGT::Args &args) {
  const string usage = "Usage: qbg load ";

  int threadSize = args.getl("p", 50);

  std::string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  std::cerr << "qbg: loading the specified blobs..." << std::endl;
  std::string blobs;
  try {
    blobs = args.get("#2");
  } catch (...) {
  }

  std::string localCodebooks;
  try {
    localCodebooks = args.get("#3");
  } catch (...) {
  }

  std::string quantizerCodebooks;
  try {
    quantizerCodebooks = args.get("#4");
  } catch (...) {
  }

  std::string rotationPath;
  try {
    rotationPath = args.get("#5");
  } catch (...) {
  }
  cerr << "rotation is " << rotationPath << "." << endl;

  QBG::Index::load(indexPath, blobs, localCodebooks, quantizerCodebooks, rotationPath, threadSize);
}

void QBG::CLI::search(NGT::Args &args) {
  const string usage = "Usage: qbg search [-n result-size] [-e epsilon] [-m mode(r|l|c|a)] "
                       "[-B blob-epsilon] "
                       "[-E edge-size] [-o output-mode] [-b result expansion(begin:end:[x]step)] "
                       "index(input) query.tsv(input)";
  args.parse("v");
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  string query;
  try {
    query = args.get("#2");
  } catch (...) {
    std::stringstream msg;
    msg << "Query is not specified" << endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  bool verbose                      = args.getBool("v");
  size_t size                       = args.getl("n", 20);
  char outputMode                   = args.getChar("o", '-');
  char searchMode                   = args.getChar("M", 'n');
  NGTQ::DataType refinementDataType = NGTQ::DataTypeAny;
  {
    char refinement = args.getChar("R", '-');
    switch (refinement) {
    case 'f': refinementDataType = NGTQ::DataTypeFloat; break;
#ifdef NGT_HALF_FLOAT
    case 'h': refinementDataType = NGTQ::DataTypeFloat16; break;
#endif
    case 'x': refinementDataType = NGTQ::DataTypeNone; break;
    case '-': refinementDataType = NGTQ::DataTypeAny; break;
    default:
      std::stringstream msg;
      msg << "Command::CreateParameters: Error: Invalid refinement data type. " << refinement;
      NGTThrowException(msg);
    }
  }
  float blobEpsilon      = args.getf("B", 0.05);
  size_t edgeSize        = args.getl("E", 0);
  float cutback          = args.getf("C", 0.0);
  size_t explorationSize = args.getf("N", 256);
  size_t nOfProbes       = 0;
  float resultExpansion  = -1;
  float epsilon          = -1;
  size_t nOfTrials       = args.getl("T", 1);
  size_t dynamicOption   = 0;
  if (nOfTrials != 1) {
    std::cerr << "# of trials=" << nOfTrials << std::endl;
  }
  std::vector<double> queryTimes;

  float beginOfParameter, endOfParameter, stepOfParameter;
  bool mulStep = false;
  {
    beginOfParameter = 0.0;
    endOfParameter   = 0.0;
    stepOfParameter  = 1;
    vector<string> tokens;
    size_t vcounter = 0;
    if (args.getString("p", "-").find_first_of(':') == std::string::npos) {
      resultExpansion = args.getf("p", 0.0);
    } else {
      vcounter++;
      dynamicOption = 1;
      NGT::Common::tokenize(args.getString("p", "-"), tokens, ":");
    }
    if (args.getString("P", "-").find_first_of(':') == std::string::npos) {
      nOfProbes = args.getl("P", 10);
    } else {
      vcounter++;
      dynamicOption = 2;
      NGT::Common::tokenize(args.getString("P", "-"), tokens, ":");
    }
    if (args.getString("e", "-").find_first_of(':') == std::string::npos) {
      if (args.getString("e", "none") == "-") {
        epsilon = FLT_MAX;
      } else {
        epsilon = args.getf("e", 0.1);
      }
    } else {
      vcounter++;
      dynamicOption = 3;
      NGT::Common::tokenize(args.getString("e", "-"), tokens, ":");
    }

    if (vcounter >= 2) {
      NGTThrowException("Error: Cannot specify both -p and -P as fluctuating values.");
    }
    if (tokens.size() >= 2) {
      beginOfParameter = NGT::Common::strtod(tokens[0]);
      endOfParameter   = beginOfParameter;
      if (tokens.size() >= 2) {
        endOfParameter = NGT::Common::strtod(tokens[1]);
      }
      if (tokens.size() >= 3) {
        if (tokens[2][0] == 'x') {
          mulStep         = true;
          stepOfParameter = NGT::Common::strtod(tokens[2].substr(1));
        } else {
          stepOfParameter = NGT::Common::strtod(tokens[2]);
        }
      }
    }
  }
  if (debugLevel >= 1) {
    cerr << "size=" << size << endl;
    cerr << "parameter=" << beginOfParameter << "->" << endOfParameter << "," << stepOfParameter << endl;
  }

  auto prebuilt = true;
  QBG::Index index(indexPath, prebuilt, verbose, refinementDataType);
  std::cerr << "qbg::The index is open." << std::endl;
  std::cerr << "  vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
  std::cerr << "  peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
  if (outputMode == 'e') {
    std::cout << "# Beginning of Evaluation" << endl;
  }
  try {
    for (size_t trial = 0; trial < nOfTrials; trial++) {
      ifstream is(query);
      if (!is) {
        cerr << "Cannot open the specified file. " << query << endl;
        return;
      }
      string line;
      double totalTime = 0;
      int queryCount   = 0;
      while (getline(is, line)) {
        vector<float> queryVector;
        stringstream linestream(line);
        while (!linestream.eof()) {
          float value;
          linestream >> value;
          queryVector.push_back(value);
        }
        queryCount++;
        for (auto parameter = beginOfParameter; parameter <= endOfParameter;
             parameter      = mulStep ? parameter * stepOfParameter : parameter + stepOfParameter) {
          NGT::ObjectDistances objects;
          QBG::SearchContainer searchContainer;
          auto query = queryVector;
          searchContainer.setObjectVector(query);
          searchContainer.setResults(&objects);
          auto re = dynamicOption == 1 ? parameter : resultExpansion;
          auto np = dynamicOption == 2 ? parameter : nOfProbes;
          auto e  = dynamicOption == 3 ? parameter : epsilon;
          searchContainer.setRefinementExpansion(re);
          searchContainer.setNumOfProbes(np);
          searchContainer.setEpsilon(e);
          searchContainer.setSize(size);
          searchContainer.setBlobEpsilon(blobEpsilon);
          searchContainer.setEdgeSize(edgeSize);
          searchContainer.setCutback(cutback);
          searchContainer.setGraphExplorationSize(explorationSize);
          NGT::Timer timer;
          timer.start();
          switch (searchMode) {
          case 'n': index.searchInTwoSteps(searchContainer); break;
#ifdef NGTQ_OBGRAPH
          case 'o': index.searchOBGraph(searchContainer); break;
#endif
          case 'g':
          default: index.searchInOneStep(searchContainer); break;
          }
          if (objects.size() > size) {
            objects.resize(size);
          }
          timer.stop();
          totalTime += timer.time;
          if (outputMode == 'e' || outputMode == 'E') {
            cout << "# Query No.=" << queryCount << endl;
            cout << "# Query=" << line.substr(0, 20) + " ..." << endl;
            cout << "# Index Type="
                 << "----" << endl;
            cout << "# Size=" << size << endl;
            cout << "# Epsilon=" << e << endl;
            cout << "# Refinement expansion=" << re << endl;
            cout << "# # of probes=" << np << endl;
            cout << "# Factor=" << parameter << endl;
            cout << "# Distance Computation=" << index.getQuantizer().distanceComputationCount << endl;
            cout << "# Query Time (msec)=" << timer.time * 1000.0 << endl;
          } else if (outputMode == 't' || outputMode == 'T') {
            cout << queryCount << "\t";
          } else {
            cout << "Query No." << queryCount << endl;
            cout << "Rank\tID\tDistance" << endl;
          }

          if (outputMode == 't' || outputMode == 'T') {
            for (size_t i = 0; i < objects.size(); i++) {
              cout << objects[i].id;
              if (outputMode == 'T') {
                cout << "\t" << objects[i].distance;
              }
              if (i + 1 != objects.size()) {
                std::cout << "\t";
              }
            }
            std::cout << std::endl;
          } else {
            for (size_t i = 0; i < objects.size(); i++) {
              cout << i + 1 << "\t" << objects[i].id << "\t";
              cout << objects[i].distance << endl;
            }
          }

          if (outputMode == 'e' || outputMode == 'E') {
            cout << "# End of Search" << endl;
          } else if (!(outputMode == 't' || outputMode == 'T')) {
            cout << "Query Time= " << timer.time << " (sec), " << timer.time * 1000.0 << " (msec)" << endl;
          }
        }
        if (outputMode == 'e' || outputMode == 'E') {
          cout << "# End of Query" << endl;
        }
      }
      queryTimes.push_back(totalTime * 1000.0 / static_cast<double>(queryCount));
      if (outputMode == 'e' || outputMode == 'E') {
        cout << "# Average Query Time (msec)=" << queryTimes.back() << endl;
        cout << "# Number of queries=" << queryCount << endl;
        cout << "# End of Evaluation" << endl;
      } else if (!(outputMode == 't' || outputMode == 'T')) {
        cout << "Average Query Time= " << totalTime / (double)queryCount << " (sec), "
             << totalTime * 1000.0 / (double)queryCount << " (msec), (" << totalTime << "/" << queryCount
             << ")" << endl;
      }
    }
  } catch (NGT::Exception &err) {
    std::stringstream msg;
    msg << "Error " << err.what() << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  } catch (...) {
    std::stringstream msg;
    msg << "Error" << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }
  if (outputMode == 'e' || outputMode == 'E') {
    if (nOfTrials >= 1) {
      std::cout << "# Total minimum query time (msec)="
                << *std::min_element(queryTimes.begin(), queryTimes.end()) << "/" << nOfTrials << " (msec)"
                << std::endl;
    }
    std::cout << "# qbg: the end of search" << std::endl;
    std::cout << "#   vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
    std::cout << "#   peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
  }
  index.close();
}
void QBG::CLI::append(NGT::Args &args) {
  const string usage = "Usage: qbg append [-n data-size] [-m b|e] [-v] index(output) data.tsv(input)";
  args.parse("v");
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }
  string data;
  try {
    data = args.get("#2");
  } catch (...) {
    std::cerr << "Data is not specified." << std::endl;
  }

  size_t dataSize  = args.getl("n", 0);
  std::string mode = args.getString("m", "");
  bool verbose     = args.getBool("v");

  if (mode.find_first_of('e') != std::string::npos) {
    QBG::Index index(indexPath, false);
    std::cerr << "size=" << index.getQuantizer().objectList.size() << std::endl;
    if (index.getQuantizer().objectList.size() > 1) {
      if (verbose) {
        std::cerr << "QBG: Error. The index is not empty." << std::endl;
        cerr << usage << endl;
      }
      return;
    }
  }

  std::cerr << "qbg: appending..." << std::endl;
  NGT::Timer timer;
  timer.start();
  if (mode.find_first_of('b') != std::string::npos) {
    QBG::Index::appendBinary(indexPath, data, dataSize, verbose);
  } else {
    QBG::Index::append(indexPath, data, dataSize, verbose);
  }
  timer.stop();
  std::cerr << "qbg: appending time=" << timer << std::endl;
}

void QBG::CLI::insert(NGT::Args &args) {
  const string usage = "Usage: qbg insert [-n data-size] [-m b|e] [-v] index(output) data.tsv(input)";
  args.parse("v");
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }
  string data;
  try {
    data = args.get("#2");
  } catch (...) {
    std::cerr << "Data is not specified." << std::endl;
  }

  std::ifstream stream(data);
  if (!stream) {
    std::stringstream msg;
    msg << "Cannot open the data file. " << data << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }
  bool verbose = args.getBool("v");

  QBG::Index qbg(indexPath);
  std::string line;
  std::vector<std::vector<float>> objects;

  while (getline(stream, line)) {
    std::vector<float> object;
    stringstream linestream(line);
    while (!linestream.eof()) {
      float value;
      linestream >> value;
      object.emplace_back(value);
    }
    objects.emplace_back(object);
  }
  std::vector<NGT::ObjectID> ids;
  qbg.insert(objects, ids);
  if (verbose) {
    for (auto &id : ids) {
      std::cout << id << " ";
    }
    std::cout << std::endl;
  }

  qbg.save();
}

void QBG::CLI::remove(NGT::Args &args) {
  const string usage = "Usage: qbg remove index removed-id";
  args.parse("v");
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }
  uint32_t ids;
  try {
    ids = args.getl("#2", 0);
  } catch (...) {
    std::stringstream msg;
    msg << "Data is not specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  auto verbose = args.getBool("v");
  if (verbose) {
    std::cout << "Removed ID=" << ids << std::endl;
  }
  std::vector<uint32_t> removedIds;
  removedIds.push_back(ids);

  QBG::Index qbg(indexPath);
  qbg.remove(removedIds);
  qbg.save();
}

void QBG::CLI::expandBlob(NGT::Args &args) {
  const string usage = "Usage: qbg expand-blob index [centroids-file]";
  args.parse("v");
  std::string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }
  std::string clusterCentroidsPath;
  try {
    clusterCentroidsPath = args.get("#2");
  } catch (...) {
  }

  NGTQ::DataType refinementDataType = NGTQ::DataTypeAny;
  {
    char dataType = args.getChar("R", '-');
    switch (dataType) {
    case 'f': refinementDataType = NGTQ::DataTypeFloat; break;
#ifdef NGT_HALF_FLOAT
    case 'h': refinementDataType = NGTQ::DataTypeFloat16; break;
#endif
    case 'x': refinementDataType = NGTQ::DataTypeNone; break;
    case '-': refinementDataType = NGTQ::DataTypeAny; break;
    default:
      std::stringstream msg;
      msg << "Command::CreateParameters: Error: Invalid refinement data type. " << dataType;
      NGTThrowException(msg);
    }
  }

  auto verbose = args.getBool("v");

  NGT::SearchContainer ngtSearchContainer;
  ngtSearchContainer.setSize(50);
  QBG::SearchContainer qbgSearchContainer;
  qbgSearchContainer.setSize(args.getl("n", 10));
  qbgSearchContainer.setBlobEpsilon(args.getl("b", 0.1));
  qbgSearchContainer.setNumOfProbes(args.getl("P", 10));
  qbgSearchContainer.setRefinementExpansion(args.getf("p", 3.0));
  float rate = args.getf("r", -1.0);

  QBG::Index::expandBlob(indexPath, clusterCentroidsPath, ngtSearchContainer, qbgSearchContainer, rate,
                         refinementDataType, verbose);
}

void QBG::CLI::buildIndex(NGT::Args &args) {
  const std::string usage =
      "Usage: qbg build-index  [-Q dimension-of-subvector] [-E max-number-of-edges] index";
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }
  char mode = args.getChar("m", '-');

  size_t beginID = args.getl("s", 1);
  size_t size    = args.getl("n", 0);
  size_t endID   = beginID + size - 1;

  std::vector<std::vector<float>> quantizerCodebook;
  std::vector<uint32_t> codebookIndex;
  std::vector<std::vector<uint32_t>> objectIndex;

  if (mode == 'q' || mode == '-') {
    {
      try {
        std::string codebookPath;
        try {
          codebookPath = args.get("#2");
        } catch (...) {
          codebookPath = indexPath + "/ws/kmeans-cluster_qcentroid.tsv";
        }
        std::ifstream stream(codebookPath);
        if (!stream) {
          std::stringstream msg;
          msg << "Cannot open the codebook. " << codebookPath << std::endl;
          msg << usage << std::endl;
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
                << object.size() << ":" << quantizerCodebook.size() << ":" << line << std::endl;
            msg << usage << std::endl;
            NGTThrowException(msg);
          }
          if (!object.empty()) {
            quantizerCodebook.push_back(object);
          }
        }
        std::cerr << "The size of quantizerCodebook is " << quantizerCodebook.size() << std::endl;
      } catch (...) {
      }
    }

    {
      try {
        std::string codebookIndexPath;
        try {
          codebookIndexPath = args.get("#3");
        } catch (...) {
          codebookIndexPath = indexPath + "/ws/kmeans-cluster_bqindex.tsv";
        }
        cerr << "codebook index is " << codebookIndexPath << "." << endl;
        std::ifstream stream(codebookIndexPath);
        if (!stream) {
          std::stringstream msg;
          msg << "Cannot open the codebook index. " << codebookIndexPath << std::endl;
          msg << usage << std::endl;
          NGTThrowException(msg);
        }
        std::string line;
        while (getline(stream, line)) {
          std::vector<std::string> tokens;
          NGT::Common::tokenize(line, tokens, " \t");
          if (tokens.size() != 1) {
            std::stringstream msg;
            msg << "The specified codebook index is invalid. " << line << std::endl;
            msg << usage << std::endl;
            NGTThrowException(msg);
          }
          codebookIndex.push_back(NGT::Common::strtol(tokens[0]));
        }

      } catch (...) {
      }
    }

    {
      try {
        std::string objectIndexPath;
        try {
          objectIndexPath = args.get("#4");
        } catch (...) {
          objectIndexPath = indexPath + "/ws/kmeans-cluster_index.tsv";
        }
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
            msg << "Cannot open the codebook index. " << objectIndexPath << std::endl;
            msg << usage << std::endl;
            NGTThrowException(msg);
          }
          {
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
      } catch (...) {
      }
    }

    std::cerr << "quantizer codebook size=" << quantizerCodebook.size() << std::endl;
    std::cerr << "codebook index size=" << codebookIndex.size() << std::endl;
    std::cerr << "object index size=" << objectIndex.size() << std::endl;

    if (mode == 'q') {
      QBG::Index::buildNGTQ(indexPath, quantizerCodebook, codebookIndex, objectIndex, beginID, endID);
      return;
    }
  }

  if (mode == 'g') {
    QBG::Index::buildQBG(indexPath);
    return;
  }

  QBG::Index::build(indexPath, quantizerCodebook, codebookIndex, objectIndex, beginID, endID);
}

void QBG::CLI::build(NGT::Args &args) {
  const std::string usage = "Usage: qbg build [-Q dimension-of-subvector] [-E max-number-of-edges] index";

  QbgCliBuildParameters buildParameters(args);
  buildParameters.getBuildParameters();

  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  std::string phaseString = args.getString("p", "1-3");
  if (phaseString.substr(0, 10) == "assign-all") {
    std::vector<std::string> tokens;
    NGT::Common::tokenize(phaseString, tokens, ":");
    int64_t lowerBoundOfObjects = 0;
    size_t noOfNearestNeighbors = 1;
    if (tokens.size() >= 2) {
      lowerBoundOfObjects = NGT::Common::strtod(tokens[1]);
    }
    if (tokens.size() >= 3) {
      noOfNearestNeighbors = NGT::Common::strtod(tokens[2]);
    }
    std::cerr << "qbg: assign-all" << std::endl;
    HierarchicalKmeans hierarchicalKmeans(buildParameters);
    hierarchicalKmeans.assignAll(indexPath, lowerBoundOfObjects, noOfNearestNeighbors);
    return;
  }
  bool phase[3];
  if (phaseString.empty()) {
    phase[0] = phase[1] = phase[2] = true;
  } else {
    vector<string> tokens;
    NGT::Common::tokenize(phaseString, tokens, "-");
    int beginOfPhase, endOfPhase;
    if (tokens.size() >= 1) {
      if (tokens[0].empty()) {
        beginOfPhase = endOfPhase = 0;
      } else {
        beginOfPhase = endOfPhase = NGT::Common::strtod(tokens[0]) - 1;
      }
    }
    if (tokens.size() >= 2) {
      endOfPhase = NGT::Common::strtod(tokens[1]) - 1;
    }
    if (tokens.size() >= 3 || tokens.size() == 0) {
      std::stringstream msg;
      msg << "The specified phases are invalid! " << phaseString << std::endl;
      msg << usage << std::endl;
      NGTThrowException(msg);
    }
    phase[0] = phase[1] = phase[2] = false;
    for (int p = beginOfPhase; p <= endOfPhase; p++) {
      phase[p] = true;
    }
  }

  HierarchicalKmeans hierarchicalKmeans(buildParameters);

  if (phase[0]) {
    std::cerr << "qbg: hierarchical clustering..." << std::endl;
    NGT::Timer timer;
    timer.start();
    hierarchicalKmeans.clustering(indexPath);
    timer.stop();
    if (buildParameters.verbose) {
      std::cerr << "qbg: hierarchical clustering successfully completed." << std::endl;
      ;
      std::cerr << "  ph0 time=" << timer << std::endl;
      std::cerr << "  ph0 vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
      std::cerr << "  ph0 peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
    }
  }

  QBG::Optimizer optimizer(buildParameters);

  if (phase[1]) {
    std::cerr << "qbg: optimizing..." << std::endl;
    NGT::Timer timer;
    timer.start();
    optimizer.optimize(indexPath);
    timer.stop();
    if (buildParameters.verbose) {
      std::cerr << "qbg: optimization successfully completed." << std::endl;
      ;
      std::cerr << "  ph1 time=" << timer << std::endl;
      std::cerr << "  ph1 vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
      std::cerr << "  ph1 peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
    }
  }

  if (phase[2]) {
    std::cerr << "qbg: building..." << std::endl;
    NGT::Timer timer;
    timer.start();
    QBG::Index::build(indexPath, optimizer.verbose);
    timer.stop();
    if (buildParameters.verbose) {
      std::cerr << "qbg: index build successfully completed." << std::endl;
      ;
      std::cerr << "  ph2 time=" << timer << std::endl;
      std::cerr << "  ph2 vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
      std::cerr << "  ph2 peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
    }
  }
}

void QBG::CLI::rebuild(NGT::Args &args) {
  const std::string usage = "Usage: qbg rebuild index";
  args.parse("v");
  bool verbose = args.getBool("v");
  if (verbose) {
    std::cerr << "rebuild" << std::endl;
  }
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }
  auto start = args.getl("s", 0);
  if (start == 0) {
    std::stringstream msg;
    msg << "Start ID(-s) should be set. The ID is the smallest ID of the objects that are appended but not "
           "indexed."
        << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  NGT::Timer timer;
  timer.start();
  NGTQ::Index ngtq(indexPath);
  ngtq.createIndex(start);
  ngtq.save();
  QBG::Index::buildQBG(indexPath, verbose);
  timer.stop();
  if (verbose) {
    std::cerr << "qbg: index build successfully completed." << std::endl;
    ;
    std::cerr << "  ph2 time=" << timer << std::endl;
    std::cerr << "  ph2 vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
    std::cerr << "  ph2 peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
  }
}

void QBG::CLI::preprocessForNGT(NGT::Args &args) {
  const std::string usage = "Usage: qbg prep index object-list-file";
  args.parse("v");
  std::string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }
  std::string objectPath;
  try {
    objectPath = args.get("#2");
  } catch (...) {
  }
  bool verbose = args.getBool("v");
  QBG::Index::preprocessingForNGT(indexPath, objectPath, verbose);
}

void QBG::CLI::hierarchicalKmeans(NGT::Args &args) {
  const std::string usage = "qbg kmeans -O #-of-objects -B x1:y1,x2,y2,x3 index [prefix] [object-ID-file]";
  std::string indexPath;

  QbgCliBuildParameters buildParameters(args);

  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  std::string prefix;
  try {
    prefix = args.get("#2");
    std::cerr << "prefix=" << prefix << std::endl;
  } catch (...) {
    std::cerr << "Prefix is not specified." << std::endl;
  }

  std::string objectIDsFile;
  try {
    objectIDsFile = args.get("#3");
    std::cerr << "object IDs=" << objectIDsFile << std::endl;
  } catch (...) {
    cerr << "Object ID file is not specified" << endl;
  }

  HierarchicalKmeans hierarchicalKmeans(buildParameters);

  hierarchicalKmeans.clustering(indexPath, prefix, objectIDsFile);

  if (buildParameters.verbose) {
    std::cerr << "qbg: the end of clustering" << std::endl;
    std::cerr << "  vmsize=" << NGT::Common::getProcessVmSizeStr() << std::endl;
    std::cerr << "  peak vmsize=" << NGT::Common::getProcessVmPeakStr() << std::endl;
  }
}

void QBG::CLI::assign(NGT::Args &args) {
  const std::string usage = "qbg assign";
  std::string indexPath;

  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  std::string queryPath;

  try {
    queryPath = args.get("#2");
  } catch (...) {
    std::stringstream msg;
    msg << "No query is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  auto epsilon      = args.getf("e", 0.1);
  auto numOfObjects = args.getl("n", 10);
  auto mode         = args.getChar("m", '-');

  try {
    NGT::Index index(indexPath);
    NGT::Property property;
    index.getProperty(property);
    ifstream is(queryPath);
    if (!is) {
      std::cerr << "Cannot open the query file. " << queryPath << std::endl;
      return;
    }
    string line;
    while (getline(is, line)) {
      vector<float> query;
      {
        stringstream linestream(line);
        while (!linestream.eof()) {
          float value;
          linestream >> value;
          query.push_back(value);
        }
        if (static_cast<size_t>(property.dimension) != query.size()) {
          std::cerr << "Dimension is invallid. " << property.dimension << ":" << query.size() << std::endl;
          return;
        }
      }
      NGT::SearchQuery sc(query);
      NGT::ObjectDistances objects;
      sc.setResults(&objects);
      sc.setSize(numOfObjects);
      sc.setEpsilon(epsilon);

      index.search(sc);
      if (objects.size() == 0) {
        std::cerr << "The result is empty. Something wrong." << std::endl;
        return;
      }
      if (mode == '-') {
        std::cout << objects[0].id - 1 << std::endl;
      } else {
        std::cout << objects[0].id << std::endl;
      }
    }
  } catch (NGT::Exception &err) {
    cerr << "Error " << err.what() << endl;
    return;
  } catch (...) {
    cerr << "Error" << endl;
    return;
  }
}

void QBG::CLI::extract(NGT::Args &args) {

  const string usage = "Usage: qbg extract binary-file|index [output-file]";

  std::string objectPath;
  try {
    objectPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Object file is not specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  std::ostream *os;
  std::ofstream ofs;
  std::string outputFile;

  size_t n         = args.getl("n", 100);
  size_t dim       = args.getl("d", 0);
  std::string type = args.getString("t", "float32");
  char mode        = args.getChar("m", 'r');

  try {
    QBG::Index index(objectPath, true);

    try {
      outputFile = args.get("#2");
      if (outputFile == "-") {
        os = &std::cout;
      } else {
        ofs.open(outputFile);
        os = &ofs;
      }
    } catch (...) {
      ofs.open(objectPath + "/ws/base.10000.u8.tsv");
      os = &ofs;
    }
    index.extract(*os, n, mode == 'r');
  } catch (NGT::Exception &err) {
    try {
      outputFile = args.get("#2");
      ofs.open(outputFile);
      os = &ofs;
    } catch (...) {
      os = &std::cout;
    }
    try {
      StaticObjectFileLoader loader(objectPath, type);
      if (mode == 'r') {
        struct timeval randTime;
        gettimeofday(&randTime, 0);
        srand(randTime.tv_usec);
        for (size_t cnt = 0; cnt < n; cnt++) {
          double random = ((double)rand() + 1.0) / ((double)RAND_MAX + 2.0);
          size_t id     = 0;
          do {
            id = floor(loader.noOfObjects * random);
          } while (id >= loader.noOfObjects);
          loader.seek(id);
          auto object = loader.getObject();
          if (cnt + 1 % 100000 == 0) {
            std::cerr << "loaded " << static_cast<float>(cnt + 1) / 1000000.0 << "M objects." << std::endl;
          }
          if (dim != 0) {
            object.resize(dim, 0.0);
          }
          for (auto v = object.begin(); v != object.end(); ++v) {
            if (v + 1 != object.end()) {
              *os << *v << "\t";
            } else {
              *os << *v << std::endl;
              ;
            }
          }
        }
      } else {
        size_t cnt = 0;
        while (!loader.isEmpty()) {
          auto object = loader.getObject();
          cnt++;
          if (cnt % 100000 == 0) {
            std::cerr << "loaded " << static_cast<float>(cnt) / 1000000.0 << "M objects." << std::endl;
          }
          if (dim != 0) {
            object.resize(dim, 0.0);
          }
          for (auto v = object.begin(); v != object.end(); ++v) {
            if (v + 1 != object.end()) {
              *os << *v << "\t";
            } else {
              *os << *v << std::endl;
              ;
            }
          }
          if (n > 0 && cnt >= n) {
            break;
          }
        }
      }
    } catch (...) {
      cerr << "Error" << endl;
      return;
    }
  }
  return;
}

void QBG::CLI::gt(NGT::Args &args) {
  string path;

  try {
    path = args.get("#1");
  } catch (...) {
    std::cerr << "An object file is not specified." << std::endl;
    return;
  }

  std::ifstream stream;
  stream.open(path, std::ios::in | std::ios::binary);
  if (!stream) {
    std::cerr << "Error!" << std::endl;
    return;
  }
  uint32_t numQueries;
  uint32_t k;

  stream.read(reinterpret_cast<char *>(&numQueries), sizeof(numQueries));
  stream.read(reinterpret_cast<char *>(&k), sizeof(k));
  std::cerr << "# of queries=" << numQueries << std::endl;
  std::cerr << "k=" << k << std::endl;

  {
    std::ofstream idf;
    idf.open(path + "_gt.tsv");
    for (uint32_t qidx = 0; qidx < numQueries; qidx++) {
      for (uint32_t rank = 0; rank < k; rank++) {
        uint32_t id;
        stream.read(reinterpret_cast<char *>(&id), sizeof(id));
        idf << id;
        if (rank + 1 == k) {
          idf << std::endl;
        } else {
          idf << "\t";
        }
      }
    }
  }

  {
    std::ofstream df;
    df.open(path + "_gtdist.tsv");
    for (uint32_t qidx = 0; qidx < numQueries; qidx++) {
      for (uint32_t rank = 0; rank < k; rank++) {
        float distance;
        stream.read(reinterpret_cast<char *>(&distance), sizeof(distance));
        df << distance;
        if (rank + 1 == k) {
          df << std::endl;
        } else {
          df << "\t";
        }
      }
    }
  }
}

void QBG::CLI::gtRange(NGT::Args &args) {
  string path;

  try {
    path = args.get("#1");
  } catch (...) {
    std::cerr << "An object file is not specified." << std::endl;
    return;
  }

  std::ifstream stream;
  stream.open(path, std::ios::in | std::ios::binary);
  if (!stream) {
    std::cerr << "Error!" << std::endl;
    return;
  }
  uint32_t numQueries;
  uint32_t totalRes;

  stream.read(reinterpret_cast<char *>(&numQueries), sizeof(numQueries));
  stream.read(reinterpret_cast<char *>(&totalRes), sizeof(totalRes));
  std::cerr << "# of queries=" << numQueries << std::endl;
  std::cerr << "totalRes=" << totalRes << std::endl;

  std::vector<uint32_t> numResultsPerQuery(numQueries);
  for (size_t qidx = 0; qidx < numQueries; qidx++) {
    uint32_t v;
    stream.read(reinterpret_cast<char *>(&v), sizeof(v));
    numResultsPerQuery[qidx] = v;
  }
  {
    std::ofstream idf;
    idf.open(path + "_gt.tsv");
    std::ofstream df;
    df.open(path + "_gtdist.tsv");
    size_t count = 0;
    for (size_t qidx = 0; qidx < numQueries; qidx++) {
      if (numResultsPerQuery[qidx] == 0) {
        idf << std::endl;
        df << std::endl;
        continue;
      }
      for (size_t rank = 0; rank < numResultsPerQuery[qidx]; rank++) {
        uint32_t v;
        stream.read(reinterpret_cast<char *>(&v), sizeof(v));
        idf << v;
        df << 0.0;
        count++;
        if (rank + 1 == numResultsPerQuery[qidx]) {
          idf << std::endl;
          df << std::endl;
        } else {
          idf << "\t";
          df << "\t";
        }
      }
    }
    if (count != totalRes) {
      std::cerr << "Fatal error. " << count << ":" << totalRes << std::endl;
    }
  }
}

void QBG::CLI::optimize(NGT::Args &args) {

  string usage = "Usage: qbg optimize -n number-of-clusters -m number-of subspaces [-O t|f] [-s t|f] [-I "
                 "cluster-iteration] [-t R-max-iteration] [-c convergence-limit-times] vector-file "
                 "[output-file-prefix]\n"
                 "       qbg optimize -e E -n number-of-clusters -m number-of index [subspaces] "
                 "[vector-file] [local-centroid-file] [global-centroid-file]";

  QbgCliBuildParameters buildParameters(args);

  std::string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }

  string invector;
  try {
    invector = args.get("#2");
  } catch (...) {
  }

  string ofile;
  try {
    ofile = args.get("#3");
  } catch (...) {
  }

  string global;
  try {
    global = args.get("#4");
  } catch (...) {
  }

  QBG::Optimizer optimizer(buildParameters);

  if (invector.empty() || ofile.empty() || global.empty()) {
    optimizer.optimize(indexPath);
  } else {
    optimizer.optimize(invector, ofile, global);
  }

  return;
}

#endif
