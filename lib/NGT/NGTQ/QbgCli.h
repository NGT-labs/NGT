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

#include "NGT/NGTQ/QuantizedBlobGraph.h"
#include "NGT/Command.h"

namespace QBG {

class CLI {
 public:
  int debugLevel;

#if !defined(NGTQ_QBG) || defined(NGTQ_SHARED_INVERTED_INDEX)
  void create(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void load(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void append(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void insert(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void remove(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void expandBlob(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void buildIndex(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void hierarchicalKmeans(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void search(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void assign(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void extract(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void gt(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void gtRange(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void optimize(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void build(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void rebuild(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void preprocessForNGT(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void createQG(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void buildQG(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void appendQG(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
  void searchQG(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
#ifdef NGTQ_OBGRAPH
  void setBlobID(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
#endif
  void info(NGT::Args &args) { std::cerr << "not implemented." << std::endl; };
#else
  void create(NGT::Args &args);
  void load(NGT::Args &args);
  void append(NGT::Args &args);
  void insert(NGT::Args &args);
  void remove(NGT::Args &args);
  void expandBlob(NGT::Args &args);
  void buildIndex(NGT::Args &args);
  void hierarchicalKmeans(NGT::Args &args);
  void search(NGT::Args &args);
  void assign(NGT::Args &args);
  void extract(NGT::Args &args);
  void gt(NGT::Args &args);
  void gtRange(NGT::Args &args);
  void optimize(NGT::Args &args);
  void build(NGT::Args &args);
  void rebuild(NGT::Args &args);
  void preprocessForNGT(NGT::Args &args);
  void createQG(NGT::Args &args);
  void buildQG(NGT::Args &args);
  void appendQG(NGT::Args &args);
  void searchQG(NGT::Args &args);
#ifdef NGTQ_QUANTIZED_TREE
  void quantizeTree(NGT::Args &args);
#endif
#ifdef NGTQ_OBGRAPH
  void setBlobID(NGT::Args &args);
#endif
  void info(NGT::Args &args);
#endif

  void setDebugLevel(int level) { debugLevel = level; }
  int getDebugLevel() { return debugLevel; }

  void help() {
    cerr << "Usage : qbg command database [data]" << endl;
    cerr << "           command : create build quantize search" << endl;
  }

  void execute(NGT::Args args) {
    string command;
    try {
      command = args.get("#0");
    } catch (...) {
      help();
      return;
    }

    debugLevel = args.getl("X", 0);

    if (debugLevel >= 1) {
      cerr << "ngt::command=" << command << endl;
    }
    if (command == "search") {
      search(args);
    } else if (command == "create") {
      create(args);
    } else if (command == "load") {
      load(args);
    } else if (command == "append") {
      append(args);
    } else if (command == "insert") {
      insert(args);
    } else if (command == "remove") {
      remove(args);
    } else if (command == "expand-blob") {
      expandBlob(args);
    } else if (command == "build-index") {
      buildIndex(args);
    } else if (command == "kmeans") {
      hierarchicalKmeans(args);
    } else if (command == "assign") {
      assign(args);
    } else if (command == "extract") {
      extract(args);
    } else if (command == "gt") {
      gt(args);
    } else if (command == "gt-range") {
      gtRange(args);
    } else if (command == "optimize") {
      optimize(args);
    } else if (command == "build") {
      build(args);
    } else if (command == "rebuild") {
      rebuild(args);
    } else if (command == "prep") {
      preprocessForNGT(args);
    } else if (command == "create-qg") {
      createQG(args);
    } else if (command == "build-qg") {
      buildQG(args);
    } else if (command == "append-qg") {
      appendQG(args);
    } else if (command == "search-qg") {
      searchQG(args);
#ifdef NGTQ_QUANTIZED_TREE
    } else if (command == "quantize-tree") {
      quantizeTree(args);
#endif
#ifdef NGTQ_OBGRAPH
    } else if (command == "set-blob-id") {
      setBlobID(args);
#endif
    } else if (command == "info") {
      info(args);
    } else if (command == "-h") {
      help();
    } else {
      help();
      std::stringstream msg;
      msg << "qbg: Illegal command. " << command << endl;
      NGTThrowException(msg);
    }
  }
};

}; // namespace QBG
