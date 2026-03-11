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
#include "NGT/Optimizer.h"

#define NGT_VERSION_FOR_HEADER
#include "NGT/Version.h"

#ifndef BUILD_DATE
#define BUILD_DATE "-"
#endif
#ifndef GIT_HASH
#define GIT_HASH "-"
#endif
#ifndef GIT_DATE
#define GIT_DATE "-"
#endif
#ifndef GIT_TAG
#define GIT_TAG "-"
#endif

using namespace std;

static void version(ostream &os) {
  os << "ngt:" << endl;
  NGT::Version::get(os);
}

void help() {
  cerr << "Usage : ngt command [options] index [data]" << endl;
  cerr << "           command : info create search remove append export import prune reconstruct-graph "
          "optimize-search-parameters optimize-#-of-edges repair rebuild"
       << endl;
  cerr << "Version : " << NGT::Index::getVersion() << endl;
  if (NGT::Index::getVersion() != NGT::Version::getVersion()) {
    version(cerr);
    NGT::Index::version(cerr);
  }
}

int main(int argc, char **argv) {
  NGT::Args args(argc, argv);

  NGT::Command ngt;

  string command;
  try {
    command = args.get("#0");
  } catch (...) {
    help();
    return 0;
  }

  ngt.setDebugLevel(args.getl("X", 0));

  try {
    if (ngt.getDebugLevel() >= 1) {
      cerr << "ngt: command=" << command << endl;
    }
    if (command == "search") {
      ngt.search(args);
    } else if (command == "create") {
      ngt.create(args);
    } else if (command == "append") {
      ngt.append(args);
    } else if (command == "remove") {
      ngt.remove(args);
    } else if (command == "export") {
      ngt.exportIndex(args);
    } else if (command == "import") {
      ngt.importIndex(args);
    } else if (command == "prune") {
      ngt.prune(args);
    } else if (command == "reconstruct-graph") {
      ngt.reconstructGraph(args);
    } else if (command == "eval") {
      NGT::Optimizer::evaluate(args);
    } else if (command == "optimize-search-parameters") {
      ngt.optimizeSearchParameters(args);
    } else if (command == "refine-anng") {
      ngt.refineANNG(args);
    } else if (command == "repair") {
      ngt.repair(args);
    } else if (command == "optimize-#-of-edges") {
      ngt.optimizeNumberOfEdgesForANNG(args);
    } else if (command == "export-graph") {
      ngt.exportGraph(args);
    } else if (command == "export-objects") {
      ngt.exportObjects(args);
    } else if (command == "rebuild") {
      ngt.rebuild(args);
    } else if (command == "prep-pq") {
      ngt.preprocessForPQ(args);
#ifdef NGT_FOREST
    } else if (command == "construct-forest") {
      ngt.constructForest(args);
#endif
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
    } else if (command == "extract-query") {
      NGT::Optimizer::extractQueries(args);
    } else if (command == "adjust-edge-size") {
      NGT::Optimizer::adjustSearchEdgeSize(args);
#endif
    } else if (command == "info") {
      if (NGT::Index::getVersion() != NGT::Version::getVersion()) {
        version(cerr);
        NGT::Index::version(cerr);
      }
      ngt.info(args);
    } else {
      cerr << "ngt: Error: Illegal command. " << command << endl;
      help();
    }
  } catch (NGT::Exception &err) {
    cerr << "ngt: Error: " << err.what() << endl;
    return 1;
  }
  return 0;
}
