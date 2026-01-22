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

#include "NGT/Command.h"
#include "NGT/GraphReconstructor.h"
#include "NGT/Optimizer.h"
#include "NGT/GraphOptimizer.h"
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
#include "NGT/MmapManagerException.h"
#endif

using namespace std;

#define NGT_APPENDING_BINARY
#ifdef NGT_APPENDING_BINARY
#include "NGT/ArrayFile.h"
#include "NGT/ObjectSpace.h"
#include "NGTQ/ObjectFile.h"
#endif
#ifdef NGT_FOREST
#include <filesystem>
#endif

NGT::Command::CreateParameters::CreateParameters(Args &args) {
  args.parse("v");
  try {
    index = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Command::CreateParameter: Error: An index is not specified.";
    NGTThrowException(msg);
  }

  try {
    objectPath = args.get("#2");
  } catch (...) {
  }

  verbose                             = !args.getBool("v");
  property.edgeSizeForCreation        = args.getl("E", 10);
  property.edgeSizeForSearch          = args.getl("S", 40);
  property.batchSizeForCreation       = args.getl("b", 200);
  property.insertionRadiusCoefficient = args.getf("e", 0.1) + 1.0;
  property.truncationThreshold        = args.getl("t", 0);
  property.dimension                  = args.getl("d", 0);
  property.threadPoolSize             = args.getl("p", 24);
  property.pathAdjustmentInterval     = args.getl("P", 0);
  property.dynamicEdgeSizeBase        = args.getl("B", 30);
  property.buildTimeLimit             = args.getf("T", 0.0);

  {
    property.leafNodeSize         = NGT::LeafNode::LeafObjectsSizeMax;
    property.internalChildrenSize = NGT::InternalNode::InternalChildrenSizeMax;
    std::string str               = args.getString("L", "-");
    if (str != "-") {
      vector<string> tokens;
      NGT::Common::tokenize(str, tokens, ":");
      if (tokens.size() != 1 && tokens.size() != 2) {
        std::stringstream msg;
        msg << "Command::CreateParameter: Error: leaf node size/internal children size specification is "
               "invalid. "
               "(leafNodeSize):(internalChildrenSize) "
            << str;
        NGTThrowException(msg);
      }
      property.leafNodeSize = NGT::Common::strtol(tokens[0]);
      if (tokens.size() == 2) {
        property.internalChildrenSize = NGT::Common::strtol(tokens[1]);
      }
    }
  }

  if (property.dimension <= 0) {
    std::stringstream msg;
    msg << "Command::CreateParameter: Error: Specify greater than 0 for # of your data dimension by a "
           "parameter -d.";
    NGTThrowException(msg);
  }

  property.objectAlignment = args.getChar("A", 'f') == 't' ? NGT::Property::ObjectAlignmentTrue
                                                           : NGT::Property::ObjectAlignmentFalse;

  char graphType = args.getChar("g", 'a');
  switch (graphType) {
  case 'a': property.graphType = NGT::Property::GraphType::GraphTypeANNG; break;
  case 'k': property.graphType = NGT::Property::GraphType::GraphTypeKNNG; break;
  case 'b': property.graphType = NGT::Property::GraphType::GraphTypeBKNNG; break;
  case 'd': property.graphType = NGT::Property::GraphType::GraphTypeDNNG; break;
  case 'o': property.graphType = NGT::Property::GraphType::GraphTypeONNG; break;
  case 'i': property.graphType = NGT::Property::GraphType::GraphTypeIANNG; break;
  case 'r': property.graphType = NGT::Property::GraphType::GraphTypeRANNG; break;
  case 'R': property.graphType = NGT::Property::GraphType::GraphTypeRIANNG; break;
  default:
    std::stringstream msg;
    msg << "Command::CreateParameter: Error: Invalid graph type. " << graphType;
    NGTThrowException(msg);
  }

  if (property.graphType == NGT::Property::GraphType::GraphTypeANNG ||
      property.graphType == NGT::Property::GraphType::GraphTypeONNG ||
      property.graphType == NGT::Property::GraphType::GraphTypeIANNG ||
      property.graphType == NGT::Property::GraphType::GraphTypeRANNG ||
      property.graphType == NGT::Property::GraphType::GraphTypeRIANNG) {
    property.outgoingEdge = 10;
    property.incomingEdge = 100;
    string str            = args.getString("O", "-");
    if (str != "-") {
      vector<string> tokens;
      NGT::Common::tokenize(str, tokens, "x");
      if (str != "-" && tokens.size() != 2) {
        std::stringstream msg;
        msg << "Command::CreateParameter: Error: outgoing/incoming edge size specification is invalid. "
               "(out)x(in) "
            << str;
        NGTThrowException(msg);
      }
      property.outgoingEdge = NGT::Common::strtod(tokens[0]);
      property.incomingEdge = NGT::Common::strtod(tokens[1]);
    }
  }

  char seedType = args.getChar("s", '-');
  switch (seedType) {
  case 'f': property.seedType = NGT::Property::SeedType::SeedTypeFixedNodes; break;
  case '1': property.seedType = NGT::Property::SeedType::SeedTypeFirstNode; break;
  case 'r': property.seedType = NGT::Property::SeedType::SeedTypeRandomNodes; break;
  case 'l': property.seedType = NGT::Property::SeedType::SeedTypeAllLeafNodes; break;
  default:
  case '-': property.seedType = NGT::Property::SeedType::SeedTypeNone; break;
  }

  auto objectType   = args.getString("o", "f");
  char distanceType = args.getChar("D", '2');
#ifdef NGT_REFINEMENT
  char refinementObjectType = args.getChar("R", 'f');
#endif

  numOfObjects = args.getl("n", 0);
  indexType    = args.getChar("i", 't');

  if (objectType == "f") {
    property.objectType = NGT::Index::Property::ObjectType::Float;
  } else if (objectType == "c") {
    property.objectType = NGT::Index::Property::ObjectType::Uint8;
#ifdef NGT_HALF_FLOAT
  } else if (objectType == "h") {
    property.objectType = NGT::Index::Property::ObjectType::Float16;
#endif
  } else if (objectType == "qsu8" || objectType == "qs8" || objectType == "q") {
    property.objectType = NGT::Index::Property::ObjectType::Qsuint8;
#ifdef NGT_BFLOAT
  } else if (objectType == "H") {
    property.objectType = NGT::Index::Property::ObjectType::Bfloat16;
#endif
#ifdef NGT_PQ4
  } else if (objectType == "pq4") {
    property.objectType = NGT::Index::Property::ObjectType::Qint4;
#endif
  } else {
    std::stringstream msg;
    msg << "Command::CreateParameter: Error: Invalid object type. " << objectType;
    NGTThrowException(msg);
  }

#ifdef NGT_REFINEMENT
  switch (refinementObjectType) {
  case 'f': property.refinementObjectType = NGT::Index::Property::ObjectType::Float; break;
  case 'c': property.refinementObjectType = NGT::Index::Property::ObjectType::Uint8; break;
#ifdef NGT_HALF_FLOAT
  case 'h': property.refinementObjectType = NGT::Index::Property::ObjectType::Float16; break;
#endif
#ifdef NGT_BFLOAT
  case 'H': property.refinementObjectType = NGT::Index::Property::ObjectType::Bfloat16; break;
#endif
  default:
    std::stringstream msg;
    msg << "Command::CreateParameter: Error: Invalid refinement object type. " << objectType;
    NGTThrowException(msg);
  }
#endif

  switch (distanceType) {
  case '1': property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeL1; break;
  case '2':
  case 'e': property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeL2; break;
  case 'a': property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeAngle; break;
  case 'A': property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeNormalizedAngle; break;
  case 'h': property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeHamming; break;
  case 'j': property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeJaccard; break;
  case 'J': property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeSparseJaccard; break;
  case 'c': property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeCosine; break;
  case 'C': property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeNormalizedCosine; break;
  case 'E': property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeNormalizedL2; break;
  case 'i': property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeInnerProduct; break;
  case 'p': // added by Nyapicom
    property.distanceType = NGT::Index::Property::DistanceType::DistanceTypePoincare;
    break;
  case 'l': // added by Nyapicom
    property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeLorentz;
    break;
  default:
    std::stringstream msg;
    msg << "Command::CreateParameter: Error: Invalid distance type. " << distanceType;
    NGTThrowException(msg);
  }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  size_t maxNoOfObjects = args.getl("N", 0);
  if (maxNoOfObjects > 0) {
    property.graphSharedMemorySize = property.treeSharedMemorySize = property.objectSharedMemorySize =
        512 * ceil(maxNoOfObjects / 50000000);
  }
#endif

  property.clippingRate = args.getf("c", 0.0);

  {
    string str = args.getString("l", "-");
    if (str != "-") {
      vector<string> tokens;
      NGT::Common::tokenize(str, tokens, ":");
      if (tokens.size() == 1) {
        property.nOfNeighborsForInsertionOrder = NGT::Common::strtol(tokens[0]);
      } else if (tokens.size() == 2) {
        property.nOfNeighborsForInsertionOrder = NGT::Common::strtol(tokens[0]);
        property.epsilonForInsertionOrder      = NGT::Common::strtof(tokens[1]);
      } else {
        std::stringstream msg;
        msg << "Command::CreateParameter: Error: Invalid insertion order parameters. " << str << endl;
        NGTThrowException(msg);
      }
    }
  }

  char epsilonType = args.getChar("M", '-');
  switch (epsilonType) {
  case 'q': property.epsilonType = NGT::Property::EpsilonType::EpsilonTypeByQuery; break;
  case 'n':
  case '-': property.epsilonType = NGT::Property::EpsilonType::EpsilonTypeNone; break;
  default:
    std::stringstream msg;
    msg << "Command::CreateParameter: Error: Invalid epsilon type. " << epsilonType;
    NGTThrowException(msg);
  }
}

void NGT::Command::create(Args &args) {
  const string usage =
      "Usage: ngt create "
      "-d dimension [-p #-of-thread] [-i index-type(t|g)] [-g graph-type(a|k|b|o|i)] "
      "[-t truncation-edge-limit] [-E edge-size] [-S edge-size-for-search] [-L edge-size-limit] "
      "[-e epsilon] "
#ifdef NGT_HALF_FLOAT
      "[-o object-type(f|h|c|q)] "
#else
      "[-o object-type(f|c|q)] "
#endif
      "[-D distance-function(1|2|a|A|h|j|c|C|E|p|l)] [-n #-of-inserted-objects] " // added by Nyapicom
      "[-P path-adjustment-interval] [-B dynamic-edge-size-base] [-A object-alignment(t|f)] "
      "[-T build-time-limit] [-O outgoing x incoming] "
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      "[-N maximum-#-of-inserted-objects] "
#endif
      "[-l #-of-neighbors-for-insertion-order[:epsilon-for-insertion-order]] "
      "[-c scalar-quantization-clipping-rate] "
      "[-R refinement-object-type[f|h]] "
      "index(output) [data.tsv(input)]";

  try {
    CreateParameters createParameters(args);

    if (debugLevel >= 1) {
      cerr << "edgeSizeForCreation=" << createParameters.property.edgeSizeForCreation << endl;
      cerr << "edgeSizeForSearch=" << createParameters.property.edgeSizeForSearch << endl;
      cerr << "edgeSizeLimit=" << createParameters.property.edgeSizeLimitForCreation << endl;
      cerr << "batch size=" << createParameters.property.batchSizeForCreation << endl;
      cerr << "graphType=" << createParameters.property.graphType << endl;
      cerr << "epsilon=" << createParameters.property.insertionRadiusCoefficient - 1.0 << endl;
      cerr << "thread size=" << createParameters.property.threadPoolSize << endl;
      cerr << "dimension=" << createParameters.property.dimension << endl;
      cerr << "indexType=" << createParameters.indexType << endl;
    }

    switch (createParameters.indexType) {
    case 't':
      NGT::Index::createGraphAndTree(createParameters.index, createParameters.property,
                                     createParameters.objectPath, createParameters.numOfObjects,
                                     createParameters.verbose);
      break;
    case 'g':
      NGT::Index::createGraph(createParameters.index, createParameters.property, createParameters.objectPath,
                              createParameters.numOfObjects, createParameters.verbose);
      break;
    }
  } catch (NGT::Exception &err) {
    std::stringstream msg;
    msg << err.what() << std::endl;
    msg << usage;
    NGTThrowException(msg);
  }
}

void appendTextVectors(std::string &indexPath, std::string &data, size_t dataSize, char appendMode,
                       std::string &destination, size_t ioSearchSize, float ioEpsilon, bool verbose) {
  NGT::Index index(indexPath);
  if (verbose) {
    index.enableLog();
  } else {
    index.disableLog();
  }
  auto append     = destination.find('n') == std::string::npos;
  auto refinement = destination.find('r') != std::string::npos;
  index.appendFromTextObjectFile(data, dataSize, append, refinement);

  if (appendMode == 't') {
    if (ioSearchSize > 0) {
      NGT::Index::InsertionOrder insertionOrder;
      insertionOrder.nOfNeighboringNodes = ioSearchSize;
      insertionOrder.epsilon             = ioEpsilon;
      std::cerr << "append: insertion order optimization is enabled. " << ioSearchSize << ":" << ioEpsilon
                << std::endl;
      index.extractInsertionOrder(insertionOrder);
      index.createIndexWithInsertionOrder(insertionOrder);
    } else {
      index.createIndex();
    }
  }
  index.save();
  index.close();
}

void appendRefinementVectors(std::string &indexPath, char appendMode, size_t ioSearchSize, float ioEpsilon,
                             bool verbose) {
  NGT::Index index(indexPath);
  if (verbose) {
    index.enableLog();
  } else {
    index.disableLog();
  }
  index.appendFromRefinementObjectFile();
  if (appendMode == 'r') {
    if (ioSearchSize > 0) {
      NGT::Index::InsertionOrder insertionOrder;
      insertionOrder.nOfNeighboringNodes = ioSearchSize;
      insertionOrder.epsilon             = ioEpsilon;
      std::cerr << "append: insertion order optimization is enabled. " << ioSearchSize << ":" << ioEpsilon
                << std::endl;
      index.extractInsertionOrder(insertionOrder);
      index.createIndexWithInsertionOrder(insertionOrder);
    } else {
      index.createIndex();
    }
  }
  index.save();
  index.close();
}

void appendTextVectorsInMemory(std::string &indexPath, std::string &data, size_t dataSize, char appendMode,
                               size_t ioSearchSize, float ioEpsilon) {
  NGT::Index index(indexPath);
  index.enableLog();
  {
    ifstream is(data);
    if (!is) {
      std::stringstream msg;
      msg << "Cannot open the specified data file. " << data;
      NGTThrowException(msg);
    }
    std::string line;
    size_t counter = 0;
    std::vector<float> objects;
    while (getline(is, line)) {
      if (is.eof()) break;
      vector<string> tokens;
      NGT::Common::tokenize(line, tokens, "\t, ");
      for (auto &v : tokens)
        objects.emplace_back(NGT::Common::strtod(v));
      counter++;
    }
    index.append(objects.data(), counter);
    index.save();
  }
  if (appendMode == 'm') {
    if (ioSearchSize > 0) {
      NGT::Index::InsertionOrder insertionOrder;
      insertionOrder.nOfNeighboringNodes = ioSearchSize;
      insertionOrder.epsilon             = ioEpsilon;
      std::cerr << "append: insertion order optimization is enabled. " << ioSearchSize << ":" << ioEpsilon
                << std::endl;
      index.extractInsertionOrder(insertionOrder);
      index.createIndexWithInsertionOrder(insertionOrder);
    } else {
      index.createIndex();
    }
  }
  index.save();
  index.close();
}

#ifdef NGT_APPENDING_BINARY

void appendBinaryVectors(std::string &indexPath, std::string &data, size_t dataSize, char appendMode,
                         std::string &destination, bool verbose) {
  NGT::Index index(indexPath);
  if (verbose) {
    index.enableLog();
  } else {
    index.disableLog();
  }
  std::vector<std::string> tokens;
  NGT::Common::tokenize(data, tokens, ".");
  auto append     = destination.find('n') == std::string::npos;
  auto refinement = destination.find('r') != std::string::npos;
  index.appendFromBinaryObjectFile(data, dataSize, append, refinement);

  if (appendMode == 'b') {
    index.createIndex(32);
  }
  index.save();
  index.close();
}
#endif

void NGT::Command::append(Args &args) {
  const string usage = "Usage: ngt append [-p #-of-thread] [-d dimension] [-n data-size] "
                       "[-D o|r] index(output) [data.tsv(input)]";
  args.parse("v");
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified." << endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }
  string data;
  try {
    data = args.get("#2");
  } catch (...) {
    cerr << "ngt: Warning: No specified object file. Just build an index for the existing objects." << endl;
  }

  int threadSize   = args.getl("p", 50);
  size_t dimension = args.getl("d", 0);
  size_t dataSize  = args.getl("n", 0);

  size_t ioSearchSize = args.getl("S", 0);
  float ioEpsilon     = args.getf("E", 0.1);
  bool verbose        = args.getBool("v");

  if (debugLevel >= 1) {
    cerr << "thread size=" << threadSize << endl;
    cerr << "dimension=" << dimension << endl;
  }

  char appendMode  = args.getChar("m", 't');
  auto destination = args.getString("D", "o");
  if (appendMode == 'n') {
    try {
      NGT::Index::append(indexPath, data, threadSize, dataSize);
    } catch (NGT::Exception &err) {
      std::stringstream msg;
      msg << err.what() << std::endl;
      msg << usage;
      NGTThrowException(msg);
    } catch (...) {
      std::stringstream msg;
      msg << usage;
      NGTThrowException(msg);
    }
  } else if (appendMode == 't' || appendMode == 'T') {
    appendTextVectors(indexPath, data, dataSize, appendMode, destination, ioSearchSize, ioEpsilon, verbose);
  } else if (appendMode == 'r' || appendMode == 'R') {
    appendRefinementVectors(indexPath, appendMode, ioSearchSize, ioEpsilon, verbose);
  } else if (appendMode == 'm' || appendMode == 'M') {
    appendTextVectorsInMemory(indexPath, data, dataSize, appendMode, ioSearchSize, ioEpsilon);
#ifdef NGT_APPENDING_BINARY
  } else if (appendMode == 'b' || appendMode == 'B') {
    appendBinaryVectors(indexPath, data, dataSize, appendMode, destination, verbose);
  }
#else
  }
#endif
}

void NGT::Command::search(NGT::Index &index, NGT::Command::SearchParameters &searchParameters, istream &is,
                          ostream &stream) {

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
    std::vector<float> object;
    NGT::Common::extractVector(line, " \t,", object);
    queryCount++;
    size_t step = searchParameters.step == 0 ? UINT_MAX : searchParameters.step;
    for (size_t n = 0; n <= step; n++) {
      NGT::SearchQuery sc(object);
      double epsilon = searchParameters.beginOfEpsilon;
      if (searchParameters.step != 0) {
        epsilon = searchParameters.beginOfEpsilon +
                  (searchParameters.endOfEpsilon - searchParameters.beginOfEpsilon) * n / step;
      } else {
        if (searchParameters.stepOfEpsilon <= 0.0) {
          std::stringstream msg;
          msg << "the step of epsilon must be greater than 0.0. " << searchParameters.stepOfEpsilon;
          NGTThrowException(msg);
        }
#ifdef NGT_REFINEMENT
        if (searchParameters.beginOfRefinementExpansion == searchParameters.endOfRefinementExpansion) {
          epsilon = searchParameters.beginOfEpsilon + searchParameters.stepOfEpsilon * n;
          if (epsilon > searchParameters.endOfEpsilon) {
            break;
          }
        }
#else
        epsilon = searchParameters.beginOfEpsilon + searchParameters.stepOfEpsilon * n;
        if (epsilon > searchParameters.endOfEpsilon) {
          break;
        }
#endif
      }
#ifdef NGT_REFINEMENT
      float refinementExpansion = searchParameters.beginOfRefinementExpansion;
      if (searchParameters.step != 0) {
        refinementExpansion =
            searchParameters.beginOfRefinementExpansion +
            (searchParameters.endOfRefinementExpansion - searchParameters.beginOfRefinementExpansion) * n /
                step;
      } else {
        if (searchParameters.stepOfRefinementExpansion <= 1.0) {
          std::stringstream msg;
          msg << "the step of refinement expansion must be greater than 1.0. "
              << searchParameters.stepOfRefinementExpansion;
          NGTThrowException(msg);
        }
        if (searchParameters.beginOfEpsilon == searchParameters.endOfEpsilon) {
          refinementExpansion = searchParameters.beginOfRefinementExpansion +
                                (n == 0 ? 0 : pow(searchParameters.stepOfRefinementExpansion, n));
          if (refinementExpansion > searchParameters.endOfRefinementExpansion) {
            break;
          }
        }
      }
#endif
      NGT::ObjectDistances objects;
      sc.setResults(&objects);
      sc.setSize(searchParameters.size);
      sc.setRadius(searchParameters.radius);
      if (searchParameters.accuracy > 0.0) {
        sc.setExpectedAccuracy(searchParameters.accuracy);
      } else {
        sc.setEpsilon(epsilon);
      }
      sc.setEdgeSize(searchParameters.edgeSize);
#ifdef NGT_REFINEMENT
      sc.setRefinementExpansion(refinementExpansion);
#endif
#ifdef RESULT_DEFINED_RANGE
      sc.setExpandedSizeByEpsilon(searchParameters.expandedSizeByEpsilon);
#endif
      NGT::Timer timer;
      try {
        if (searchParameters.outputMode[0] == 'e') {
          double time    = 0.0;
          uint64_t ntime = 0;
          double minTime = DBL_MAX;
          size_t trial   = searchParameters.trial <= 0 ? 1 : searchParameters.trial;
          for (size_t t = 0; t < trial; t++) {
            switch (searchParameters.indexType) {
            case 't':
              timer.start();
              index.search(sc);
              timer.stop();
              break;
            case 'g':
              timer.start();
              index.searchUsingOnlyGraph(sc);
              timer.stop();
              break;
            case 's':
              timer.start();
              index.linearSearch(sc);
              timer.stop();
              break;
            }
            if (minTime > timer.time) {
              minTime = timer.time;
            }
            time += timer.time;
            ntime += timer.ntime;
          }
          time /= (double)trial;
          ntime /= trial;
          timer.time  = minTime;
          timer.ntime = ntime;
        } else {
          switch (searchParameters.indexType) {
          case 't':
            timer.start();
            index.search(sc);
            timer.stop();
            break;
          case 'g':
            timer.start();
            index.searchUsingOnlyGraph(sc);
            timer.stop();
            break;
          case 's':
            timer.start();
            index.linearSearch(sc);
            timer.stop();
            break;
          }
        }
      } catch (NGT::Exception &err) {
        if (searchParameters.outputMode != "ei") {
          // not ignore exceptions
          throw err;
        }
      }
      totalTime += timer.time;
      if (searchParameters.outputMode[0] == 'e') {
        stream << "# Query No.=" << queryCount << endl;
        stream << "# Query=" << line.substr(0, 20) + " ..." << endl;
        stream << "# Index Type=" << searchParameters.indexType << endl;
        stream << "# Size=" << searchParameters.size << endl;
        stream << "# Radius=" << searchParameters.radius << endl;
        stream << "# Epsilon=" << epsilon << endl;
#ifdef NGT_REFINEMENT
        stream << "# Expansion=" << refinementExpansion << endl;
        if (searchParameters.beginOfRefinementExpansion != searchParameters.endOfRefinementExpansion) {
          stream << "# Factor=" << refinementExpansion << endl;
        } else {
          stream << "# Factor=" << epsilon << endl;
        }
#endif
        stream << "# Query Time (msec)=" << timer.time * 1000.0 << endl;
        stream << "# Distance Computation=" << sc.distanceComputationCount << endl;
        stream << "# Visit Count=" << sc.visitCount << endl;
      } else {
        stream << "Query No." << queryCount << endl;
        stream << "Rank\tID\tDistance" << endl;
      }
      for (size_t i = 0; i < objects.size(); i++) {
        if (searchParameters.outputMode == "e-") {
          stream << i + 1 << "\t" << objects[i].id << "\t" << 0.0 << std::endl;
        } else {
          stream << i + 1 << "\t" << objects[i].id << "\t" << objects[i].distance << std::endl;
        }
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
    stream << "# VM size=" << NGT::Common::getProcessVmSizeStr() << std::endl;
    stream << "# Peak VM size=" << NGT::Common::getProcessVmPeakStr() << std::endl;
    stream << "# End of Evaluation" << endl;

    if (searchParameters.outputMode == "e+") {
      // show graph information
      size_t esize         = searchParameters.edgeSize;
      long double distance = 0.0;
      size_t numberOfNodes = 0;
      size_t numberOfEdges = 0;

      NGT::GraphIndex &graph = (NGT::GraphIndex &)index.getIndex();
      for (size_t id = 1; id < graph.repository.size(); id++) {
        NGT::GraphNode *node = 0;
        try {
          node = graph.getNode(id);
        } catch (NGT::Exception &err) {
          cerr << "Graph::search: Warning. Cannot get the node. ID=" << id << ":" << err.what()
               << " If the node was removed, no problem." << endl;
          continue;
        }
        numberOfNodes++;
        if (numberOfNodes % 1000000 == 0) {
          cerr << "Processed " << numberOfNodes << endl;
        }
        for (size_t i = 0; i < node->size(); i++) {
          if (esize != 0 && i >= esize) {
            break;
          }
          numberOfEdges++;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          distance += (*node).at(i, graph.repository.allocator).distance;
#else
          distance += (*node)[i].distance;
#endif
        }
      }

      stream << "# # of nodes=" << numberOfNodes << endl;
      stream << "# # of edges=" << numberOfEdges << endl;
      stream << "# Average number of edges=" << (double)numberOfEdges / (double)numberOfNodes << endl;
      stream << "# Average distance of edges=" << setprecision(10) << distance / (double)numberOfEdges
             << endl;
    }
  } else {
    stream << "Average Query Time= " << totalTime / (double)queryCount << " (sec), "
           << totalTime * 1000.0 / (double)queryCount << " (msec), (" << totalTime << "/" << queryCount << ")"
           << endl;
  }
}

void NGT::Command::search(Args &args) {
  const string usage =
      "Usage: ngt search [-i index-type(g|t|s)] [-n result-size] [-e epsilon] [-E edge-size] "
      "[-m open-mode(r|w)] [-o output-mode] index(input) query.tsv(input)";

#ifdef RESULT_DEFINED_RANGE
  args.parse("N");
#endif

  string database;
  try {
    database = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified" << endl;
    msg << usage;
    NGTThrowException(msg);
  }

  SearchParameters searchParameters(args);

  if (debugLevel >= 1) {
    cerr << "indexType=" << searchParameters.indexType << endl;
    cerr << "size=" << searchParameters.size << endl;
    cerr << "edgeSize=" << searchParameters.edgeSize << endl;
    cerr << "epsilon=" << searchParameters.beginOfEpsilon << "<->" << searchParameters.endOfEpsilon << ","
         << searchParameters.stepOfEpsilon << endl;
  }

  try {
    NGT::Index index(database, searchParameters.openMode == 'r');
    search(index, searchParameters, cout);
    if (debugLevel >= 1) {
      cerr << "Peak VM size=" << NGT::Common::getProcessVmPeakStr() << std::endl;
    }
  } catch (NGT::Exception &err) {
    std::stringstream msg;
    msg << err.what() << std::endl;
    msg << usage;
    NGTThrowException(msg);
  } catch (...) {
    std::stringstream msg;
    msg << usage;
    NGTThrowException(msg);
  }
}

void NGT::Command::remove(Args &args) {
  const string usage = "Usage: ngt remove [-d object-ID-type(f|d)] [-m f] index(input) object-ID(input)";
  string database;
  try {
    database = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified" << endl;
    msg << usage;
    NGTThrowException(msg);
  }
  try {
    args.get("#2");
  } catch (...) {
    std::stringstream msg;
    msg << "ID is not specified" << endl;
    msg << usage;
    NGTThrowException(msg);
  }
  char dataType = args.getChar("d", 'f');
  char mode     = args.getChar("m", '-');
  bool force    = false;
  if (mode == 'f') {
    force = true;
  }
  if (debugLevel >= 1) {
    cerr << "dataType=" << dataType << endl;
  }

  try {
    vector<NGT::ObjectID> objects;
    if (dataType == 'f') {
      string ids;
      try {
        ids = args.get("#2");
      } catch (...) {
        std::stringstream msg;
        msg << "Data file is not specified" << endl;
        NGTThrowException(msg);
      }
      ifstream is(ids);
      if (!is) {
        std::stringstream msg;
        msg << "Cannot open the specified file. " << ids << endl;
        NGTThrowException(msg);
      }
      string line;
      int count = 0;
      while (getline(is, line)) {
        count++;
        vector<string> tokens;
        NGT::Common::tokenize(line, tokens, "\t, ");
        if (tokens.size() == 0 || tokens[0].size() == 0) {
          continue;
        }
        char *e;
        size_t id;
        try {
          id = strtol(tokens[0].c_str(), &e, 10);
          objects.push_back(id);
        } catch (...) {
          cerr << "Illegal data. " << tokens[0] << endl;
        }
        if (*e != 0) {
          cerr << "Illegal data. " << e << endl;
        }
        cerr << "removed ID=" << id << endl;
      }
    } else {
      size_t id = args.getl("#2", 0);
      cerr << "removed ID=" << id << endl;
      objects.push_back(id);
    }
    NGT::Index::remove(database, objects, force);
  } catch (NGT::Exception &err) {
    std::stringstream msg;
    msg << err.what() << std::endl;
    msg << usage;
    NGTThrowException(msg);
  } catch (...) {
    std::stringstream msg;
    msg << usage;
    NGTThrowException(msg);
  }
}

void NGT::Command::exportIndex(Args &args) {
  const string usage = "Usage: ngt export index(input) export-file(output)";
  string database;
  try {
    database = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified." << endl;
    msg << usage;
    NGTThrowException(msg);
  }
  string exportFile;
  try {
    exportFile = args.get("#2");
  } catch (...) {
    std::stringstream msg;
    msg << "ID is not specified" << endl;
    msg << usage;
    NGTThrowException(msg);
  }
  try {
    NGT::Index::exportIndex(database, exportFile);
  } catch (NGT::Exception &err) {
    std::stringstream msg;
    msg << err.what() << std::endl;
    msg << usage;
    NGTThrowException(msg);
  } catch (...) {
    std::stringstream msg;
    msg << usage;
    NGTThrowException(msg);
  }
}

void NGT::Command::importIndex(Args &args) {
  const string usage = "Usage: ngt import index(output) import-file(input)";
  string database;
  try {
    database = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified" << endl;
    msg << usage;
    NGTThrowException(msg);
  }
  string importFile;
  try {
    importFile = args.get("#2");
  } catch (...) {
    std::stringstream msg;
    msg << "ID is not specified" << endl;
    msg << usage;
    NGTThrowException(msg);
  }

  try {
    NGT::Index::importIndex(database, importFile);
  } catch (NGT::Exception &err) {
    cerr << err.what() << endl;
    cerr << usage << endl;
  } catch (...) {
    cerr << usage << endl;
  }
}

void NGT::Command::prune(Args &args) {
  const string usage =
      "Usage: ngt prune -e #-of-forcedly-pruned-edges -s #-of-selecively-pruned-edge index(in/out)";
  string indexName;
  try {
    indexName = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified" << endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }

  // the number of forcedly pruned edges
  size_t forcedlyPrunedEdgeSize = args.getl("e", 0);
  // the number of selectively pruned edges
  size_t selectivelyPrunedEdgeSize = args.getl("s", 0);

  cerr << "forcedly pruned edge size=" << forcedlyPrunedEdgeSize << endl;
  cerr << "selectively pruned edge size=" << selectivelyPrunedEdgeSize << endl;

  if (selectivelyPrunedEdgeSize == 0 && forcedlyPrunedEdgeSize == 0) {
    std::stringstream msg;
    msg << "prune: Error! Either of selective edge size or remaining edge size should be specified." << endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }

  if (forcedlyPrunedEdgeSize != 0 && selectivelyPrunedEdgeSize != 0 &&
      selectivelyPrunedEdgeSize >= forcedlyPrunedEdgeSize) {
    std::stringstream msg;
    msg << "prune: Error! selective edge size is less than remaining edge size." << endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }

  NGT::Index index(indexName);
  cerr << "loaded the input index." << endl;

  NGT::GraphIndex &graph = (NGT::GraphIndex &)index.getIndex();

  for (size_t id = 1; id < graph.repository.size(); id++) {
    try {
      NGT::GraphNode &node = *graph.getNode(id);
      if (id % 1000000 == 0) {
        cerr << "Processed " << id << endl;
      }
      if (forcedlyPrunedEdgeSize > 0 && node.size() >= forcedlyPrunedEdgeSize) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        node.resize(forcedlyPrunedEdgeSize, graph.repository.allocator);
#else
        node.resize(forcedlyPrunedEdgeSize);
#endif
      }
      if (selectivelyPrunedEdgeSize > 0 && node.size() >= selectivelyPrunedEdgeSize) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        cerr << "not implemented" << endl;
        abort();
#else
        size_t rank = 0;
        for (NGT::GraphNode::iterator i = node.begin(); i != node.end(); ++rank) {
          if (rank >= selectivelyPrunedEdgeSize) {
            bool found = false;
            for (size_t t1 = 0; t1 < node.size() && found == false; ++t1) {
              if (t1 >= selectivelyPrunedEdgeSize) {
                break;
              }
              if (rank == t1) {
                continue;
              }
              NGT::GraphNode &node2 = *graph.getNode(node[t1].id);
              for (size_t t2 = 0; t2 < node2.size(); ++t2) {
                if (t2 >= selectivelyPrunedEdgeSize) {
                  break;
                }
                if (node2[t2].id == (*i).id) {
                  found = true;
                  break;
                }
              } // for
            } // for
            if (found) {
              //remove
              i = node.erase(i);
              continue;
            }
          }
          i++;
        } // for
#endif
      }

    } catch (NGT::Exception &err) {
      cerr << "Graph::search: Warning. Cannot get the node. ID=" << id << ":" << err.what() << endl;
      continue;
    }
  }

  graph.saveIndex(indexName);
}

void NGT::Command::reconstructGraph(Args &args) {
  const string usage = "Usage: ngt reconstruct-graph [-m mode] [-P path-adjustment-mode] -o "
                       "#-of-outgoing-edges -i #-of-incoming(reversed)-edges [-q #-of-queries] [-n "
                       "#-of-results] [-E minimum-#-of-edges] index(input) index(output)\n"
                       "\t-m mode\n"
                       "\t\ts: Edge adjustment.\n"
                       "\t\tS: Edge adjustment and path adjustment. (default)\n"
                       "\t\tc: Edge adjustment with the constraint.\n"
                       "\t\tC: Edge adjustment with the constraint and path adjustment.\n"
                       "\t\tP: Path adjustment.\n"
                       "\t-P path-adjustment-mode\n"
                       "\t\ta: Advanced method. High-speed. Not guarantee the paper's method. (default)\n"
                       "\t\tothers: Slow and less memory usage, but guarantee the paper's method.\n";

  args.parse("v");

  string inIndexPath;
  try {
    inIndexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "ngt::reconstructGraph: Input index is not specified." << endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }
  string outIndexPath;
  try {
    outIndexPath = args.get("#2");
  } catch (...) {
    std::stringstream msg;
    msg << "ngt::reconstructGraph: Output index is not specified." << endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }

  char mode            = args.getChar("m", 'S');
  char srmode          = args.getChar("P", '-');
  size_t nOfQueries    = args.getl("q", 100); // # of query objects
  size_t nOfResults    = args.getl("n", 20);  // # of resultant objects
  double gtEpsilon     = args.getf("e", 0.1);
  double margin        = args.getf("M", 0.2);
  char smode           = args.getChar("s", '-');
  bool verbose         = args.getBool("v");
  char graphConversion = args.getChar("C", '-');

  // the number (rank) of original edges
  int numOfOutgoingEdges = args.getl("o", -1);
  // the number (rank) of reverse edges
  int numOfIncomingEdges = args.getl("i", -1);

  NGT::GraphOptimizer graphOptimizer(false);

  if (mode == 'P') {
    numOfOutgoingEdges = 0;
    numOfIncomingEdges = 0;
    std::cerr << "ngt::reconstructGraph: Warning. \'-m P\' and not zero for # of in/out edges are specified "
                 "at the same time."
              << std::endl;
  }
  graphOptimizer.shortcutReduction               = (mode == 'S' || mode == 'C' || mode == 'P') ? true : false;
  graphOptimizer.searchParameterOptimization     = (smode == '-' || smode == 's') ? true : false;
  graphOptimizer.prefetchParameterOptimization   = (smode == '-' || smode == 'p') ? true : false;
  graphOptimizer.accuracyTableGeneration         = (smode == '-' || smode == 'a') ? true : false;
  graphOptimizer.shortcutReductionWithLessMemory = srmode == 's' ? true : false;
  graphOptimizer.margin                          = margin;
  graphOptimizer.gtEpsilon                       = gtEpsilon;
  graphOptimizer.minNumOfEdges                   = args.getl("E", 0);
  graphOptimizer.maxNumOfEdges                   = args.getl("A", std::numeric_limits<int64_t>::max());
  graphOptimizer.numOfThreads                    = args.getl("T", 0);
#ifdef NGT_SHORTCUT_REDUCTION_WITH_ANGLE
  graphOptimizer.shortcutReductionRange = args.getf("R", 0.38);
#else
  graphOptimizer.shortcutReductionRange = args.getf("R", 18.0);
#endif
  graphOptimizer.undirectedGraphConversion = graphConversion == '-' ? false : true;
  graphOptimizer.logDisabled               = !verbose;

  graphOptimizer.set(numOfOutgoingEdges, numOfIncomingEdges, nOfQueries, nOfResults);
  graphOptimizer.execute(inIndexPath, outIndexPath);

  std::cout << "Successfully completed." << std::endl;
}

void NGT::Command::optimizeSearchParameters(Args &args) {
  const string usage =
      "Usage: ngt optimize-search-parameters [-m optimization-target(s|p|a)] [-q #-of-queries] [-n "
      "#-of-results] index\n"
      "\t-m mode\n"
      "\t\ts: optimize search parameters (the number of explored edges).\n"
      "\t\tp: optimize prefetch parameters.\n"
      "\t\ta: generate an accuracy table to specify an expected accuracy instead of an epsilon for search.\n";

  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified" << endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }

  char mode = args.getChar("m", '-');

  size_t nOfQueries = args.getl("q", 100); // # of query objects
  size_t nOfResults = args.getl("n", 20);  // # of resultant objects

  try {
    NGT::GraphOptimizer graphOptimizer(false);

    graphOptimizer.searchParameterOptimization   = (mode == '-' || mode == 's') ? true : false;
    graphOptimizer.prefetchParameterOptimization = (mode == '-' || mode == 'p') ? true : false;
    graphOptimizer.accuracyTableGeneration       = (mode == '-' || mode == 'a') ? true : false;
    graphOptimizer.numOfQueries                  = nOfQueries;
    graphOptimizer.numOfResults                  = nOfResults;

    graphOptimizer.set(0, 0, nOfQueries, nOfResults);
    graphOptimizer.optimizeSearchParameters(indexPath);

    std::cout << "Successfully completed." << std::endl;
  } catch (NGT::Exception &err) {
    cerr << err.what() << endl;
    cerr << usage << endl;
  }
}

void NGT::Command::refineANNG(Args &args) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  NGTThrowException("refineANNG is unavailable for shared memory.");
#else
  const string usage =
      "Usage: ngt refine-anng [-e epsilon] [-a expected-accuracy] anng-index refined-anng-index";

  string inIndexPath;
  try {
    inIndexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Input index is not specified" << endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }

  string outIndexPath;
  try {
    outIndexPath = args.get("#2");
  } catch (...) {
    std::stringstream msg;
    msg << "Output index is not specified" << endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }

  NGT::Index index(inIndexPath);

  float epsilon          = args.getf("e", 0.1);
  float expectedAccuracy = args.getf("a", 0.0);
  int noOfEdges          = args.getl("k", 0); // to reconstruct kNNG
  int exploreEdgeSize    = args.getf("E", INT_MIN);
  size_t batchSize       = args.getl("b", 10000);

  try {
    GraphReconstructor::refineANNG(index, epsilon, expectedAccuracy, noOfEdges, exploreEdgeSize, batchSize);
  } catch (NGT::Exception &err) {
    std::stringstream msg;
    msg << "Error!! Cannot refine the index. " << err.what() << std::endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }
  index.saveIndex(outIndexPath);
#endif
}

void NGT::Command::repair(Args &args) {
  const string usage = "Usage: ngt [-m c|r|R] repair index \n"
                       "\t-m mode\n"
                       "\t\tc: Check. (default)\n"
                       "\t\tr: Repair and save it as [index].repair.\n"
                       "\t\tR: Repair and overwrite into the specified index.\n";

  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified." << endl;
    msg << usage << endl;
    NGTThrowException(msg);
  }

  char mode = args.getChar("m", 'c');

  bool repair = false;
  if (mode == 'r' || mode == 'R') {
    repair = true;
  }
  string path = indexPath;
  if (mode == 'r') {
    path             = indexPath + ".repair";
    const string com = "cp -r " + indexPath + " " + path;
    int stat         = system(com.c_str());
    if (stat != 0) {
      std::stringstream msg;
      msg << "ngt::repair: Cannot create the specified index. " << path << std::endl;
      msg << usage;
      NGTThrowException(msg);
    }
  }

  NGT::Index index(path);

  NGT::ObjectRepository &objectRepository   = index.getObjectSpace().getRepository();
  NGT::GraphIndex &graphIndex               = static_cast<GraphIndex &>(index.getIndex());
  NGT::GraphAndTreeIndex &graphAndTreeIndex = static_cast<GraphAndTreeIndex &>(index.getIndex());
  size_t objSize                            = objectRepository.size();
  std::cerr << "aggregate removed objects from the repository." << std::endl;
  std::set<ObjectID> removedIDs;
  for (ObjectID id = 1; id < objSize; id++) {
    if (objectRepository.isEmpty(id)) {
      removedIDs.insert(id);
    }
  }

  std::cerr << "aggregate objects from the tree." << std::endl;
  std::set<ObjectID> ids;
  graphAndTreeIndex.DVPTree::getAllObjectIDs(ids, true);
  size_t idsSize = ids.size() == 0 ? 0 : (*ids.rbegin()) + 1;
  if (objSize < idsSize) {
    std::cerr << "The sizes of the repository and tree are inconsistent. " << objSize << ":" << idsSize
              << std::endl;
  }
  size_t invalidTreeObjectCount    = 0;
  size_t uninsertedTreeObjectCount = 0;
  std::cerr << "remove invalid objects from the tree." << std::endl;
  size_t size = objSize > idsSize ? objSize : idsSize;
  for (size_t id = 1; id < size; id++) {
    if (ids.find(id) != ids.end()) {
      if (removedIDs.find(id) != removedIDs.end() || id >= objSize) {
        if (repair) {
          graphAndTreeIndex.DVPTree::removeNaively(id);
          std::cerr << "Found the removed object in the tree. Removed it from the tree. " << id << std::endl;
        } else {
          std::cerr << "Found the removed object in the tree. " << id << std::endl;
        }
        invalidTreeObjectCount++;
      }
    } else {
      if (removedIDs.find(id) == removedIDs.end() && id < objSize) {
        std::cerr << "Not found an object in the tree. However, it might be a duplicated same object. " << id
                  << "/" << removedIDs.size() << std::endl;
        uninsertedTreeObjectCount++;
        if (repair) {
          try {
            graphIndex.repository.remove(id);
          } catch (...) {
          }
        }
      }
    }
  }

  if (objSize != graphIndex.repository.size()) {
    std::cerr << "The sizes of the repository and graph are inconsistent. " << objSize << ":"
              << graphIndex.repository.size() << std::endl;
  }
  size_t invalidGraphObjectCount    = 0;
  size_t uninsertedGraphObjectCount = 0;
  size = objSize > graphIndex.repository.size() ? objSize : graphIndex.repository.size();
  std::cerr << "remove invalid objects from the graph." << std::endl;
  for (size_t id = 1; id < size; id++) {
    try {
      graphIndex.getNode(id);
      if (removedIDs.find(id) != removedIDs.end() || id >= objSize) {
        if (repair) {
          graphAndTreeIndex.DVPTree::removeNaively(id);
          try {
            graphIndex.repository.remove(id);
          } catch (...) {
          }
          std::cerr << "Found the removed object in the graph. Removed it from the graph. " << id
                    << std::endl;
        } else {
          std::cerr << "Found the removed object in the graph. " << id << std::endl;
        }
        invalidGraphObjectCount++;
      }
    } catch (NGT::Exception &err) {
      if (removedIDs.find(id) == removedIDs.end() && id < objSize) {
        std::cerr << "Not found an object in the graph. It should be inserted into the graph. " << err.what()
                  << " ID=" << id << std::endl;
        uninsertedGraphObjectCount++;
        if (repair) {
          try {
            graphAndTreeIndex.DVPTree::removeNaively(id);
          } catch (...) {
          }
        }
      }
    } catch (...) {
      std::cerr << "Unexpected error!" << std::endl;
    }
  }

  size_t invalidEdgeCount = 0;
  //#pragma omp parallel for
  for (size_t id = 1; id < graphIndex.repository.size(); id++) {
    try {
      NGT::GraphNode &node = *graphIndex.getNode(id);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      for (auto n = node.begin(graphIndex.repository.allocator);
           n != node.end(graphIndex.repository.allocator);) {
#else
      for (auto n = node.begin(); n != node.end();) {
#endif
        if (removedIDs.find((*n).id) != removedIDs.end() || (*n).id >= objSize) {

          std::cerr << "Not found the destination object of the edge. " << id << ":" << (*n).id << std::endl;
          invalidEdgeCount++;
          if (repair) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            n = node.erase(n, graphIndex.repository.allocator);
#else
            n = node.erase(n);
#endif
            continue;
          }
        }
        ++n;
      }
    } catch (...) {
    }
  }

  if (repair) {
    if (objSize < graphIndex.repository.size()) {
      graphIndex.repository.resize(objSize);
    }
  }

  std::cerr << "The number of invalid tree objects=" << invalidTreeObjectCount << std::endl;
  std::cerr << "The number of invalid graph objects=" << invalidGraphObjectCount << std::endl;
  std::cerr << "The number of uninserted tree objects (Can be ignored)=" << uninsertedTreeObjectCount
            << std::endl;
  std::cerr << "The number of uninserted graph objects=" << uninsertedGraphObjectCount << std::endl;
  std::cerr << "The number of invalid edges=" << invalidEdgeCount << std::endl;

  if (repair) {
    try {
      if (uninsertedGraphObjectCount > 0) {
        std::cerr << "Building index." << std::endl;
        index.createIndex(16);
      }
      std::cerr << "Saving index." << std::endl;
      index.saveIndex(path);
    } catch (NGT::Exception &err) {
      std::stringstream msg;
      msg << err.what() << endl;
      msg << usage;
      NGTThrowException(msg);
    }
  }
}

#ifdef NGT_FOREST
void NGT::Command::constructForestGraph(NGT::Args &args) {
  const string usage = "Usage: ngt construct-forest target-index search-index(ANNG)";
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "No index is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }
  string anngPath;
  try {
    anngPath = args.get("#2");
  } catch (...) {
    std::stringstream msg;
    msg << "No ANNG is specified." << std::endl;
    msg << usage << std::endl;
    NGTThrowException(msg);
  }
  std::cerr << "constructForestGraph" << std::endl;
  std::string clusterSizeStr = args.getString("c", "-"); // Size of clusters to form
  size_t clusterSize         = 0;
  float clusterSizeFactor    = 0.0;
  if (clusterSizeStr.size() > 0 && clusterSizeStr[0] == 'x') {
    clusterSizeFactor = NGT::Common::strtod(clusterSizeStr.substr(1));
  } else {
    clusterSize = NGT::Common::strtol(clusterSizeStr);
  }
  std::cerr << "Cluster size: " << clusterSize << ":" << clusterSizeFactor << std::endl;
  size_t incomingEdge = args.getl("i", 10);
  size_t outgoingEdge = args.getl("o", 10);
  size_t epsilon      = args.getf("e", 0.0);
  std::cerr << "incomingEdge: " << incomingEdge << " outgoingEdge: " << outgoingEdge << std::endl;
  try {
    NGT::Property prop;
    prop.clear();
    prop.seedType = NGT::Property::SeedType::SeedTypeFirstNode;
    NGT::Index::create(indexPath, anngPath, prop, true);
    std::filesystem::copy_file(anngPath + "/obj", indexPath + "/obj",
                               std::filesystem::copy_options::overwrite_existing);
    std::filesystem::copy_file(anngPath + "/tre", indexPath + "/tre",
                               std::filesystem::copy_options::overwrite_existing);

    std::cerr << "Opening index: " << indexPath << std::endl;
    NGT::Index index(indexPath);
    std::cerr << "Index opened successfully." << std::endl;
    size_t repositorySize = index.getObjectRepositorySize();
    std::cerr << "Repository size: " << repositorySize << std::endl;

    NGT::GraphIndex &graphIndex = static_cast<NGT::GraphIndex &>(index.getIndex());
    std::cerr << "GraphIndex obtained." << std::endl;

    NGT::ObjectSpace &objectSpace           = index.getObjectSpace();
    NGT::ObjectRepository &objectRepository = objectSpace.getRepository();
    NGT::Index anng(anngPath);
#define USE_LEAF
    std::vector<bool> isClustered(repositorySize, false);
    std::vector<NGT::ObjectDistances> clusterIds;
    std::vector<NGT::ObjectID> seedNodes;
    std::vector<Node::ID> leafIDs;
    {
      NGT::GraphAndTreeIndex &graphAndTreeIndex = static_cast<NGT::GraphAndTreeIndex &>(index.getIndex());
      graphAndTreeIndex.getAllLeafNodeIDs(leafIDs);
      clusterIds.reserve(leafIDs.size());
      size_t objcount = 0;
      size_t existed  = 0;
      for (auto &leafid : leafIDs) {
#ifdef USE_LEAF
        NGT::LeafNode &leaf = *static_cast<NGT::LeafNode *>(graphAndTreeIndex.DVPTree::getNode(leafid));
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        NGT::ObjectDistances objs(leaf.getObjectIDs(graphAndTreeIndex.DVPTree::leafNodes.allocator),
                                  leaf.getObjectIDs(graphAndTreeIndex.DVPTree::leafNodes.allocator) +
                                      leaf.getObjectSize());
#else
        NGT::ObjectDistances objs(leaf.getObjectIDs(), leaf.getObjectIDs() + leaf.getObjectSize());
#endif
#ifdef USE_CENTROID
        {
          size_t count = 0;
          std::vector<float> centroid;
          for (auto &obj : objs) {
            std::vector<float> v;
            if (objectRepository.isEmpty(obj.id)) {
              continue;
            }
            objectSpace.getObject(obj.id, v);
            if (centroid.empty()) {
              centroid = v;
            } else {
              for (size_t i = 0; i < v.size(); ++i) {
                centroid[i] += v[i];
              }
            }
            count++;
          }
          for (auto &value : centroid) {
            value /= count; // Calculate the mean
          }
          NGT::Object *cent = objectSpace.allocateNormalizedObject(centroid);
          for (auto &obj : objs) {

            NGT::Object &o = *objectRepository.get(obj.id);
            obj.distance   = objectSpace.getComparator()(*cent, o);
          }
          objectSpace.deleteObject(cent);
          std::cerr << "the head of objects in leaf " << objs[0].id << std::endl;
          std::sort(objs.begin(), objs.end());
          std::cerr << "the centroid of objects in leaf " << objs[0].id << std::endl;
        }
#endif
        if (!objs.empty()) {
          seedNodes.emplace_back(objs[0].id);
        }
        for (auto obj : objs) {
          if (isClustered[obj.id]) {
            std::cerr << "already extracted! " << obj.id << ":" << leafid.getID() << std::endl;
            existed++;
            continue;
          }
          isClustered[obj.id] = true;
          objcount++;
        }
        clusterIds.emplace_back(std::move(objs));
#else
        NGT::ObjectDistances ids;
        graphAndTreeIndex.getObjectIDsFromLeaf(leafid, ids);
        if (!ids.empty()) {
          seedNodes.emplace_back(ids[0].id);
        }
#endif
      }
      std::cerr << "Total objects in clusters from leaves: " << objcount << ":" << existed << std::endl;
    }

    if (clusterSize > 0 || clusterSizeFactor > 0.0) {
#pragma omp parallel for
      for (auto it = seedNodes.begin(); it != seedNodes.end(); ++it) {
        auto nodeId = *it;
        {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          NGT::Object *optr =
              anng.getObjectSpace().allocateObject(*anng.getObjectSpace().getRepository().get(nodeId));
          NGT::Object &queryObject = *optr;
#else
          NGT::Object &queryObject = *anng.getObjectSpace().getRepository().get(nodeId);
#endif
          NGT::ObjectDistances results;
          NGT::SearchContainer searchContainer(queryObject);
          searchContainer.setResults(&results);
          size_t size = clusterSize;
          if (clusterSizeFactor > 0.0) {
            size = clusterIds[std::distance(seedNodes.begin(), it)].size() * clusterSizeFactor;
          }
          searchContainer.setSize(size);
          searchContainer.setEpsilon(0.1);
          anng.search(searchContainer);
          for (const auto &result : results) {
            clusterIds[std::distance(seedNodes.begin(), it)].emplace_back(result);
            isClustered[result.id] = true;
          }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          anng.getObjectSpace().deleteObject(optr);
#endif
        }
      }
    }
    for (auto &cluster : clusterIds) {
      std::cerr << "Cluster size=" << cluster.size() << std::endl;
    }
    std::cerr << "Cluster IDs collected." << std::endl;
    size_t nOfAddedObjects = 0;
    for (size_t nodeId = 1; nodeId < repositorySize; nodeId++) {
      if (isClustered[nodeId]) {
        continue;
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      NGT::Object *optr =
          anng.getObjectSpace().allocateObject(*anng.getObjectSpace().getRepository().get(nodeId));
      NGT::Object &queryObject = *optr;
#else
      NGT::Object &queryObject = *anng.getObjectSpace().getRepository().get(nodeId);
#endif
      float minDistance          = std::numeric_limits<float>::max();
      size_t closestClusterIndex = 0;
      for (size_t j = 0; j < seedNodes.size(); j++) {
        const auto &seedNodeId = seedNodes[j];
        float distance         = objectSpace.getComparator()(queryObject, *objectRepository.get(seedNodeId));
        if (distance < minDistance) {
          minDistance         = distance;
          closestClusterIndex = j;
        }
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      anng.getObjectSpace().deleteObject(optr);
#endif
      clusterIds[closestClusterIndex].emplace_back(nodeId, minDistance);
      nOfAddedObjects++;
      isClustered[nodeId] = true;
      std::cerr << "Node " << nodeId << " added to cluster of seed node " << seedNodes[closestClusterIndex]
                << " with distance " << minDistance << std::endl;
    }
    std::cerr << "Total nodes added to clusters: " << nOfAddedObjects << std::endl;

    std::cerr << "objectrepository size: " << repositorySize << ":" << objectRepository.size() << std::endl;
    for (size_t nodeId = repositorySize - 1; nodeId > 0; nodeId--) {
      if (objectRepository.isEmpty(nodeId)) {
        continue;
      }
      try {
        NGT::ObjectDistances empty;
        graphIndex.repository.insert(nodeId, empty);
      } catch (NGT::Exception &err) {
        std::cerr << "Error adding object " << nodeId << ": " << err.what() << std::endl;
        continue;
      }
      if (nodeId % 10000 == 0) {
        std::cerr << "Added object " << nodeId << " to graph repository." << std::endl;
      }
    }
    std::cerr << "All objects added to graph." << std::endl;

#pragma omp parallel for
    for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
      if (clusterIds[seedidx].empty()) {
        std::cerr << "Warning! No cluster members found for seed node " << seedNodes[seedidx] << std::endl;
        continue;
      }
      {
        std::sort(clusterIds[seedidx].begin(), clusterIds[seedidx].end());
      }
    }
#define ADD_EDGE_PARALLEL_WITH_PARTIAL_MULTI_THREADS
#if defined(ADD_EDGE_LINEAR)
    for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
      std::cerr << "seedNode[" << seedidx << "] " << seedNodes[seedidx] << " has "
                << clusterIds[seedidx].size() << " cluster members." << std::endl;
      std::cerr << clusterIds[seedidx][0] << std::endl;
      if (seedNodes[seedidx] != clusterIds[seedidx][0].id) {
        std::cerr << "Seed node is not the first in its cluster, adding it now." << std::endl;
      }
      size_t count = 0;
      for (const auto &clusterNode : clusterIds[seedidx]) {
        if (objectRepository.isEmpty(clusterNode.id)) {
          std::cerr << "Warning! Cluster member " << clusterNode.id << " is empty, skipping." << std::endl;
          continue;
        }
        NGT::Object &queryObject = *objectRepository.get(clusterNode.id);
        NGT::ObjectDistances results;
        NGT::SearchContainer searchContainer(queryObject);
        searchContainer.setResults(&results);
        searchContainer.setSize(std::max(incomingEdge, outgoingEdge) + 1);
        searchContainer.setEdgeSize(0);
        searchContainer.setEpsilon(epsilon);
        NGT::ObjectDistances seeds;
        seeds.emplace_back(clusterIds[seedidx][0]);
        index.search(searchContainer, seeds);
        size_t edgeCount = 0;
        for (size_t idx = 0; idx < results.size(); idx++) {
          const auto &result = results[idx];
          if (result.id == clusterNode.id) {
            continue;
          }
          try {
            if (edgeCount < incomingEdge) {
              graphIndex.addEdge(result.id, clusterNode.id, result.distance, false);
            }
            if (edgeCount < outgoingEdge) {
              graphIndex.addEdge(clusterNode.id, result.id, result.distance, false);
            }
          } catch (...) {
          }
          edgeCount++;
        }
      }
    }
#elif defined(ADD_EDGE_PARALLEL)
    bool done = false;
    for (size_t idx = 0; !done; idx++) {
      done = true;
      for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
        if (idx >= clusterIds[seedidx].size()) {
          continue;
        }
        done = false;
        std::cerr << clusterIds[seedidx][idx] << std::endl;
        const auto &clusterNode = clusterIds[seedidx][idx];
        if (seedNodes[seedidx] != clusterIds[seedidx][0].id) {
          std::cerr << "Seed node is not the first in its cluster, adding it now." << std::endl;
        }
        if (objectRepository.isEmpty(clusterNode.id)) {
          std::cerr << "Warning! Cluster member " << clusterNode.id << " is empty, skipping." << std::endl;
          continue;
        }
        NGT::Object &queryObject = *objectRepository.get(clusterNode.id);
        NGT::ObjectDistances results;
        NGT::SearchContainer searchContainer(queryObject);
        searchContainer.setResults(&results);
        searchContainer.setSize(std::max(incomingEdge, outgoingEdge) + 1);
        searchContainer.setEdgeSize(0);
        searchContainer.setEpsilon(epsilon);
        NGT::ObjectDistances seeds;
        seeds.emplace_back(clusterIds[seedidx][0]);
        index.search(searchContainer, seeds);
        size_t edgeCount = 0;

        for (size_t ridx = 0; ridx < results.size(); ridx++) {
          const auto &result = results[ridx];
          if (result.id == clusterNode.id) {
            continue;
          }
          try {
            if (edgeCount < incomingEdge) {
              graphIndex.addEdge(result.id, clusterNode.id, result.distance, false);
            }
            if (edgeCount < outgoingEdge) {
              graphIndex.addEdge(clusterNode.id, result.id, result.distance, false);
            }
          } catch (...) {
          }
          edgeCount++;
        }
      }
    }
#elif defined(ADD_EDGE_PARALLEL_WITH_MULTI_THREADS)
    bool done = false;
    for (size_t idx = 0; !done; idx++) {
      done = true;
      std::vector<std::vector<std::tuple<NGT::ObjectID, NGT::ObjectID, float>>> edges(seedNodes.size());
#pragma omp parallel for
      for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
        if (idx >= clusterIds[seedidx].size()) {
          continue;
        }
        done                    = false;
        const auto &clusterNode = clusterIds[seedidx][idx];
        if (seedNodes[seedidx] != clusterIds[seedidx][0].id) {
          std::cerr << "Seed node is not the first in its cluster, adding it now." << std::endl;
        }
        if (objectRepository.isEmpty(clusterNode.id)) {
          std::cerr << "Warning! Cluster member " << clusterNode.id << " is empty, skipping." << std::endl;
          continue;
        }
        NGT::Object &queryObject = *objectRepository.get(clusterNode.id);
        NGT::ObjectDistances results;
        NGT::SearchContainer searchContainer(queryObject);
        searchContainer.setResults(&results);
        searchContainer.setSize(std::max(incomingEdge, outgoingEdge) + 1);
        searchContainer.setEdgeSize(0);
        searchContainer.setEpsilon(epsilon);
        NGT::ObjectDistances seeds;
        seeds.emplace_back(clusterIds[seedidx][0]);
        index.search(searchContainer, seeds);
        size_t edgeCount = 0;
        // Add edges from seed node to each cluster member

        for (size_t ridx = 0; ridx < results.size(); ridx++) {
          const auto &result = results[ridx];
          if (result.id == clusterNode.id) {
            continue;
          }
          if (edgeCount < incomingEdge) {
            edges[seedidx].emplace_back(result.id, clusterNode.id, result.distance);
          }
          if (edgeCount < outgoingEdge) {
            edges[seedidx].emplace_back(clusterNode.id, result.id, result.distance);
          }
          edgeCount++;
        }
      }
      for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
        for (const auto &edge : edges[seedidx]) {
          try {
            graphIndex.addEdge(std::get<0>(edge), std::get<1>(edge), std::get<2>(edge), false);
          } catch (...) {
          }
        }
      }
    }
#elif defined(ADD_EDGE_PARALLEL_WITH_PARTIAL_MULTI_THREADS)
    bool done        = false;
    size_t batchSize = 8;
    for (size_t idx = 0; !done; idx++) {
      std::cerr << "Processing the rank=" << idx << std::endl;
      done = true;
      std::vector<size_t> seedidxes;
      seedidxes.reserve(batchSize);
      for (size_t sidx = 0;; sidx++) {
        if (seedidxes.size() >= batchSize || sidx >= seedNodes.size()) {
          std::vector<std::vector<std::tuple<NGT::ObjectID, NGT::ObjectID, float>>> edges(seedidxes.size());
#pragma omp parallel for
          for (size_t seedidxesidx = 0; seedidxesidx < seedidxes.size(); seedidxesidx++) {
            size_t seedidx = seedidxes[seedidxesidx];
            if (seedidx >= seedNodes.size()) {
              continue;
            }
            if (idx >= clusterIds[seedidx].size()) {
              continue;
            }
            done                    = false;
            const auto &clusterNode = clusterIds[seedidx][idx];
            if (seedNodes[seedidx] != clusterIds[seedidx][0].id) {
              std::cerr << "Seed node is not the first in its cluster, adding it now." << std::endl;
            }
            if (objectRepository.isEmpty(clusterNode.id)) {
              std::cerr << "Warning! Cluster member " << clusterNode.id << " is empty, skipping."
                        << std::endl;
              continue;
            }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
            NGT::Object *optr = anng.getObjectSpace().allocateObject(
                *anng.getObjectSpace().getRepository().get(clusterNode.id));
            NGT::Object &queryObject = *optr;
#else
            NGT::Object &queryObject = *objectRepository.get(clusterNode.id);
#endif
            NGT::ObjectDistances results;
            NGT::SearchContainer searchContainer(queryObject);
            searchContainer.setResults(&results);
            searchContainer.setSize(std::max(incomingEdge, outgoingEdge) + 1);
            searchContainer.setEdgeSize(0);
            searchContainer.setEpsilon(epsilon);
            NGT::ObjectDistances seeds;
            seeds.emplace_back(clusterIds[seedidx][0]);
            index.search(searchContainer, seeds);
            size_t edgeCount = 0;
            for (size_t ridx = 0; ridx < results.size(); ridx++) {
              const auto &result = results[ridx];
              if (result.id == clusterNode.id) {
                continue;
              }
              if (edgeCount < incomingEdge) {
                edges[seedidxesidx].emplace_back(result.id, clusterNode.id, result.distance);
              }
              if (edgeCount < outgoingEdge) {
                edges[seedidxesidx].emplace_back(clusterNode.id, result.id, result.distance);
              }
              edgeCount++;
            }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
            anng.getObjectSpace().deleteObject(optr);
#endif
          }
          for (auto &sdedge : edges) {
            for (const auto &edge : sdedge) {
              try {
                graphIndex.addEdge(std::get<0>(edge), std::get<1>(edge), std::get<2>(edge), false);
              } catch (...) {
              }
            }
          }
          seedidxes.clear();
          if (sidx >= seedNodes.size()) {
            break;
          }
        }
        if (idx >= clusterIds[sidx].size()) {
          continue;
        }
        seedidxes.emplace_back(sidx);
      }
    }
#elif defined(ADD_EDGE_PARALLEL_WITH_EXTRA_EDGES)
    bool done = false;
    for (size_t idx = 0; !done; idx++) {
      done = true;
      for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
        if (idx >= clusterIds[seedidx].size()) {
          continue;
        }
        done = false;
        std::cerr << clusterIds[seedidx][idx] << std::endl;
        const auto &clusterNode = clusterIds[seedidx][idx];
        if (seedNodes[seedidx] != clusterIds[seedidx][0].id) {
          std::cerr << "Seed node is not the first in its cluster, adding it now." << std::endl;
        }
        if (objectRepository.isEmpty(clusterNode.id)) {
          std::cerr << "Warning! Cluster member " << clusterNode.id << " is empty, skipping." << std::endl;
          continue; // Skip if the cluster member is empty
        }
        NGT::ObjectDistances resultsFine;
        {
          NGT::Object &queryObject = *objectRepository.get(clusterNode.id);
          NGT::SearchContainer searchContainer(queryObject);
          searchContainer.setResults(&resultsFine);
          searchContainer.setSize(std::max(incomingEdge, outgoingEdge) + 1);
          searchContainer.setEdgeSize(0);
          searchContainer.setEpsilon(epsilon);
          NGT::ObjectDistances seeds;
          seeds.emplace_back(clusterIds[seedidx][0]);
          index.search(searchContainer, seeds);
        }
        NGT::ObjectDistances results;
        NGT::Object &queryObject = *objectRepository.get(clusterNode.id);
        NGT::SearchContainer searchContainer(queryObject);
        searchContainer.setResults(&results);
        searchContainer.setSize(std::max(incomingEdge, outgoingEdge) + 1);
        searchContainer.setEdgeSize(0);
        searchContainer.setEpsilon(0.0);
        NGT::ObjectDistances seeds;
        seeds.emplace_back(clusterIds[seedidx][0]);
        index.search(searchContainer, seeds);
        for (size_t rf = 0; rf < resultsFine.size(); rf++) {
          bool found = false;
          for (size_t r = 0; r < results.size(); r++) {
            if (resultsFine[rf].id == results[r].id) {
              found = true;
              break;
            }
          }
          if (!found) {
            std::cerr << "Warning! Fine result " << resultsFine[rf].id << " is not in zero-epsilon result."
                      << std::endl;
          }
        }

        size_t edgeCount = 0;
        for (size_t ridx = 0; ridx < results.size(); ridx++) {
          const auto &result = results[ridx];
          if (result.id == clusterNode.id) {
            continue;
          }
          try {
            if (edgeCount < incomingEdge) {
              graphIndex.addEdge(result.id, clusterNode.id, result.distance, false);
            }
            if (edgeCount < outgoingEdge) {
              graphIndex.addEdge(clusterNode.id, result.id, result.distance, false);
            }
          } catch (...) {
          }
          edgeCount++;
        }
      }
    }
#else
    for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
      if (seedNodes[seedidx] != clusterIds[seedidx][0].id) {
        std::cerr << "Seed node is not the first in its cluster, adding it now." << std::endl;
      }
      NGT::GraphIndex tmpGraph(indexPath, false, NGT::Index::OpenTypeObjectDisabled);
      tmpGraph.objectSpace = graphIndex.objectSpace;
      for (const auto &clusterNode : clusterIds[seedidx]) {
        if (objectRepository.isEmpty(clusterNode.id)) {
          std::cerr << "Warning! Cluster member " << clusterNode.id << " is empty, skipping." << std::endl;
          continue;
        }
        try {
          NGT::ObjectDistances empty;
          tmpGraph.repository.insert(clusterNode.id, empty);
        } catch (NGT::Exception &err) {
          std::cerr << "Error adding object " << clusterNode.id << ": " << err.what() << std::endl;
          continue;
        }
        std::cerr << "repository size=" << tmpGraph.repository.size() << std::endl;
        if (tmpGraph.repository.size() <= 1) {
          continue;
        }
        NGT::Object &queryObject = *objectRepository.get(clusterNode.id);
        NGT::ObjectDistances results;
        NGT::SearchContainer searchContainer(queryObject);
        searchContainer.setResults(&results);
        searchContainer.setSize(std::max(incomingEdge, outgoingEdge) + 1);
        searchContainer.setEdgeSize(0);
        searchContainer.setEpsilon(epsilon);
        NGT::ObjectDistances seeds;
        seeds.emplace_back(clusterIds[seedidx][0]);
        tmpGraph.search(searchContainer, seeds);
        std::cerr << "Search results for cluster member " << clusterNode.id << ": " << results.size()
                  << " results found." << std::endl;
        size_t edgeCount = 0;
        for (size_t idx = 0; idx < results.size(); idx++) {
          const auto &result = results[idx];
          if (result.id == clusterNode.id) {
            continue;
          }
          try {
            if (edgeCount < incomingEdge) {
              tmpGraph.addEdge(result.id, clusterNode.id, result.distance, false);
            }
            if (edgeCount < outgoingEdge) {
              tmpGraph.addEdge(clusterNode.id, result.id, result.distance, false);
            }
          } catch (...) {
          }
          edgeCount++;
        }
      }
      tmpGraph.objectSpace = 0;
    }
#endif
    index.save();
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
}
#endif

void NGT::Command::optimizeNumberOfEdgesForANNG(Args &args) {
  const string usage =
      "Usage: ngt optimize-#-of-edges [-q #-of-queries] [-k #-of-retrieved-objects] "
      "[-p #-of-threads] [-a target-accuracy] [-o target-#-of-objects] [-s #-of-sampe-objects] "
      "[-e maximum-#-of-edges] anng-index";

  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified." << endl;
    msg << usage;
    NGTThrowException(msg);
  }

  GraphOptimizer::ANNGEdgeOptimizationParameter parameter;

  parameter.noOfQueries       = args.getl("q", 200);
  parameter.noOfResults       = args.getl("k", 50);
  parameter.noOfThreads       = args.getl("p", 16);
  parameter.targetAccuracy    = args.getf("a", 0.9);
  parameter.targetNoOfObjects = args.getl("o", 0); // zero will replaced # of the repository size.
  parameter.noOfSampleObjects = args.getl("s", 100000);
  parameter.maxNoOfEdges      = args.getl("e", 100);

  NGT::GraphOptimizer graphOptimizer(false); // false=log
  auto optimizedEdge = graphOptimizer.optimizeNumberOfEdgesForANNG(indexPath, parameter);
  std::cout << "The optimized # of edges=" << optimizedEdge.first << "(" << optimizedEdge.second << ")"
            << std::endl;
  std::cout << "Successfully completed." << std::endl;
}

void NGT::Command::info(Args &args) {
  const string usage = "Usage: ngt info [-E #-of-edges] [-m a|e|h|p] index";

  std::cout << "NGT version: " << NGT::Index::getVersion() << std::endl;
  std::cout << "CPU SIMD types: ";
  CpuInfo::showSimdTypes();

  string database;
  try {
    database = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified" << endl;
    msg << usage;
    NGTThrowException(msg);
  }

  size_t edgeSize = args.getl("E", UINT_MAX);
  char mode       = args.getChar("m", '-');

  try {
    NGT::Index index(database);
    NGT::GraphIndex::showStatisticsOfGraph(static_cast<NGT::GraphIndex &>(index.getIndex()), mode, edgeSize);
    if (mode == 'v') {
      vector<uint8_t> status;
      index.verify(status);
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  } catch (MemoryManager::MmapManagerException &err) {
    cerr << "ngt: MmapManager Error. " << err.what() << endl;
    cerr << usage << endl;
#endif
  } catch (NGT::Exception &err) {
    cerr << "ngt: NGT Error. " << err.what() << endl;
    cerr << usage << endl;
  } catch (...) {
    cerr << usage << endl;
  }
}

void NGT::Command::exportGraph(Args &args) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  NGTThrowException("exportGraph is not implemented.");
#else
  std::string usage = "ngt export-graph [-k #-of-edges] index";
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified" << endl;
    msg << usage;
    NGTThrowException(msg);
  }

  int k = args.getl("k", 0);

  NGT::Index index(indexPath);
  NGT::GraphIndex &graph = static_cast<NGT::GraphIndex &>(index.getIndex());

  size_t size = index.getObjectRepositorySize();

  for (size_t id = 1; id < size; ++id) {
    NGT::GraphNode *node = 0;
    try {
      node = graph.getNode(id);
    } catch (...) {
      continue;
    }
    std::cout << id << "\t";
    for (auto ei = (*node).begin(); ei != (*node).end(); ++ei) {
      if (k != 0 && k <= distance((*node).begin(), ei)) {
        break;
      }
      std::cout << (*ei).id << "\t" << (*ei).distance;
      if (ei + 1 != (*node).end()) {
        std::cout << "\t";
      }
    }
    std::cout << std::endl;
  }
#endif
}

void NGT::Command::exportObjects(Args &args) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  NGTThrowException("exportObjects is not implemented.");
#else
  std::string usage = "ngt export-objects index";
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified" << endl;
    msg << usage;
    NGTThrowException(msg);
  }

  NGT::Index index(indexPath);
  auto &objectSpace = index.getObjectSpace();
  size_t size       = objectSpace.getRepository().size();

  for (size_t id = 1; id < size; ++id) {
    std::vector<float> object;
    if (objectSpace.getRepository().isEmpty(id)) continue;
    objectSpace.getObject(id, object);
    for (auto v = object.begin(); v != object.end(); ++v) {
      std::cout << *v;
      if (v + 1 != object.end()) {
        std::cout << "\t";
      }
    }
    std::cout << std::endl;
  }
#endif
}

void NGT::Command::rebuild(Args &args) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  NGTThrowException("rebuild is not implemented.");
#else
  std::string usage = "ngt rebuild [-m c] index";
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified" << endl;
    msg << usage;
    NGTThrowException(msg);
  }

  char mode = args.getChar("m", '-');

  NGT::Index index(indexPath);
  index.clearIndex();
  if (mode == '-') {
    index.createIndex();
  }
  index.save();

#endif
}

void NGT::Command::preprocessForPQ(Args &args) {
#ifdef NGT_QBG_DISABLED
  NGTThrowException("prep-pq is not implemented.");
#else
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  NGTThrowException("rebuild is not implemented.");
#else
  const std::string usage = "Usage: ngt prep-pq index";
  args.parse("v");
  string indexPath;
  try {
    indexPath = args.get("#1");
  } catch (...) {
    std::stringstream msg;
    msg << "Index is not specified" << endl;
    msg << usage;
    NGTThrowException(msg);
  }

  bool verbose = args.getBool("v");

  NGT::Quantizer::build(indexPath, verbose);
#endif
#endif
}
