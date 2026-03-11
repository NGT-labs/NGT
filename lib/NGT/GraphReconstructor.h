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

#include <unordered_map>
#include <unordered_set>
#include <list>

#ifdef _OPENMP
#include <omp.h>
#else
#warning "*** OMP is *NOT* available! ***"
#endif

#define NGT_SHORTCUT_REDUCTION_WITH_ANGLE

namespace NGT {

class GraphReconstructor {
 public:
  static void extractGraph(std::vector<NGT::ObjectDistances> &graph, NGT::GraphIndex &graphIndex) {
    graph.reserve(graphIndex.repository.size());
    for (size_t id = 1; id < graphIndex.repository.size(); id++) {
      if (id % 1000000 == 0) {
        std::cerr << "GraphReconstructor::extractGraph: Processed " << id
                  << " objects. vm size=" << NGT::Common::getProcessVmSizeStr() << ":"
                  << NGT::Common::getProcessVmPeakStr() << std::endl;
      }
      try {
        NGT::GraphNode &node = *graphIndex.getNode(id);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        NGT::ObjectDistances nd;
        nd.reserve(node.size());
        for (auto n = node.begin(graphIndex.repository.allocator);
             n != node.end(graphIndex.repository.allocator); ++n) {
          nd.push_back(ObjectDistance((*n).id, (*n).distance));
        }
        graph.push_back(nd);
#else
        graph.push_back(node);
#endif
        if (graph.back().size() != graph.back().capacity()) {
          std::cerr << "GraphReconstructor::extractGraph: Warning! The graph size must be the same as the "
                       "capacity. "
                    << id << std::endl;
        }
      } catch (NGT::Exception &err) {
        graph.push_back(NGT::ObjectDistances());
        continue;
      }
    }
  }

  static void adjustPaths(NGT::Index &outIndex) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    std::cerr << "construct index is not implemented." << std::endl;
    exit(1);
#else
    NGT::GraphIndex &outGraph = dynamic_cast<NGT::GraphIndex &>(outIndex.getIndex());
    size_t rStartRank         = 0;
    std::list<std::pair<size_t, NGT::GraphNode>> tmpGraph;
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      NGT::GraphNode &node = *outGraph.getNode(id);
      tmpGraph.push_back(std::pair<size_t, NGT::GraphNode>(id, node));
      if (node.size() > rStartRank) {
        node.resize(rStartRank);
      }
    }
    size_t removeCount = 0;
    for (size_t rank = rStartRank;; rank++) {
      bool edge = false;
      Timer timer;
      for (auto it = tmpGraph.begin(); it != tmpGraph.end();) {
        size_t id = (*it).first;
        try {
          NGT::GraphNode &node = (*it).second;
          if (rank >= node.size()) {
            it = tmpGraph.erase(it);
            continue;
          }
          edge = true;
          if (rank >= 1 && node[rank - 1].distance > node[rank].distance) {
            std::cerr << "distance order is wrong!" << std::endl;
            std::cerr << id << ":" << rank << ":" << node[rank - 1].id << ":" << node[rank].id << std::endl;
          }
          NGT::GraphNode &tn  = *outGraph.getNode(id);
          volatile bool found = false;
          if (rank < 1000) {
            for (size_t tni = 0; tni < tn.size() && !found; tni++) {
              if (tn[tni].id == node[rank].id) {
                continue;
              }
              NGT::GraphNode &dstNode = *outGraph.getNode(tn[tni].id);
              for (size_t dni = 0; dni < dstNode.size(); dni++) {
                if ((dstNode[dni].id == node[rank].id) && (dstNode[dni].distance < node[rank].distance)) {
                  found = true;
                  break;
                }
              }
            }
          } else {
#ifdef _OPENMP
#pragma omp parallel for num_threads(10)
#endif
            for (size_t tni = 0; tni < tn.size(); tni++) {
              if (found) {
                continue;
              }
              if (tn[tni].id == node[rank].id) {
                continue;
              }
              NGT::GraphNode &dstNode = *outGraph.getNode(tn[tni].id);
              for (size_t dni = 0; dni < dstNode.size(); dni++) {
                if ((dstNode[dni].id == node[rank].id) && (dstNode[dni].distance < node[rank].distance)) {
                  found = true;
                }
              }
            }
          }
          if (!found) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            outGraph.addEdge(id, node.at(i, outGraph.repository.allocator).id,
                             node.at(i, outGraph.repository.allocator).distance, true);
#else
            tn.push_back(NGT::ObjectDistance(node[rank].id, node[rank].distance));
#endif
          } else {
            removeCount++;
          }
        } catch (NGT::Exception &err) {
          std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                    << std::endl;
          it++;
          continue;
        }
        it++;
      }
      if (edge == false) {
        break;
      }
    }
#endif // NGT_SHARED_MEMORY_ALLOCATOR
  }

  static void adjustPathsEffectively(NGT::Index &outIndex, size_t minNoOfEdges = 0) {
    NGT::GraphIndex &outGraph = dynamic_cast<NGT::GraphIndex &>(outIndex.getIndex());
    adjustPathsEffectively(outGraph, minNoOfEdges);
  }

  static bool edgeComp(NGT::ObjectDistance a, NGT::ObjectDistance b) {
    return (a.id & 0x7FFFFFFF) < (b.id & 0x7FFFFFFF);
  }

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  static void insert(NGT::GraphNode &node, size_t edgeID, NGT::Distance edgeDistance,
                     NGT::GraphIndex &graph) {
    NGT::ObjectDistance edge(edgeID, edgeDistance);
    GraphNode::iterator ni = std::lower_bound(node.begin(graph.repository.allocator),
                                              node.end(graph.repository.allocator), edge, edgeComp);
    node.insert(ni, edge, graph.repository.allocator);
  }

  static bool hasEdge(NGT::GraphIndex &graph, size_t srcNodeID, size_t dstNodeID) {
    NGT::GraphNode &srcNode = *graph.getNode(srcNodeID);
    GraphNode::iterator ni =
        std::lower_bound(srcNode.begin(graph.repository.allocator), srcNode.end(graph.repository.allocator),
                         ObjectDistance(dstNodeID, 0.0), edgeComp);
    return (ni != srcNode.end(graph.repository.allocator)) && ((*ni).id == dstNodeID);
  }

#else
  static void insert(NGT::GraphNode &node, size_t edgeID, NGT::Distance edgeDistance) {
    NGT::ObjectDistance edge(edgeID, edgeDistance);
    GraphNode::iterator ni = std::lower_bound(node.begin(), node.end(), edge, edgeComp);
    node.insert(ni, edge);
  }

  static bool hasEdge(NGT::GraphIndex &graph, size_t srcNodeID, size_t dstNodeID) {
    NGT::GraphNode &srcNode = *graph.getNode(srcNodeID);
    GraphNode::iterator ni =
        std::lower_bound(srcNode.begin(), srcNode.end(), ObjectDistance(dstNodeID, 0.0), edgeComp);
    return (ni != srcNode.end()) && ((*ni).id == dstNodeID);
  }
#endif

  static void adjustPathsEffectively(NGT::GraphIndex &outGraph, size_t minNoOfEdges) {
    Timer timer;
    timer.start();
    std::vector<NGT::GraphNode> tmpGraph;
    tmpGraph.reserve(outGraph.repository.size());
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      if (id % 1000000 == 0) {
        std::cerr << "GraphReconstructor::adjustPaths: # of the extracted nodes=" << id
                  << " peak vm size=" << NGT::Common::getProcessVmPeakStr() << std::endl;
      }
      try {
        NGT::GraphNode &node = *outGraph.getNode(id);
        tmpGraph.push_back(node);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        node.clear(outGraph.repository.allocator);
#else
        node.clear();
#endif
      } catch (NGT::Exception &err) {
        std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                  << std::endl;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        tmpGraph.push_back(NGT::GraphNode(outGraph.repository.allocator));
#else
        tmpGraph.push_back(NGT::GraphNode());
#endif
      }
    }
    if (outGraph.repository.size() != tmpGraph.size() + 1) {
      std::stringstream msg;
      msg << "GraphReconstructor: Fatal inner error. " << outGraph.repository.size() << ":" << tmpGraph.size()
          << ", " << outGraph.getPath();
      NGTThrowException(msg);
    }
    timer.stop();
    std::cerr << "GraphReconstructor::adjustPaths: graph preparing time=" << timer << std::endl;
    timer.reset();
    timer.start();

    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> removeCandidates(tmpGraph.size());
    int removeCandidateCount = 0;
#ifdef _OPENMP
#pragma omp parallel for
#endif
    for (size_t idx = 0; idx < tmpGraph.size(); ++idx) {
      auto it   = tmpGraph.begin() + idx;
      size_t id = idx + 1;
      try {
        NGT::GraphNode &srcNode = *it;
        std::unordered_map<uint32_t, std::pair<uint32_t, float>> neighbors;
        for (uint32_t sni = 0; sni < srcNode.size(); ++sni) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          neighbors[srcNode.at(sni, outGraph.repository.allocator).id] =
              std::pair<uint32_t, float>(sni, srcNode.at(sni, outGraph.repository.allocator).distance);
#else
          neighbors[srcNode[sni].id] = std::pair<uint32_t, float>(sni, srcNode[sni].distance);
#endif
        }

        std::vector<std::pair<uint32_t, std::pair<uint32_t, uint32_t>>> candidates;
        for (size_t sni = 0; sni < srcNode.size(); sni++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          NGT::GraphNode &pathNode = tmpGraph[srcNode.at(sni, outGraph.repository.allocator).id - 1];
#else
          NGT::GraphNode &pathNode = tmpGraph[srcNode[sni].id - 1];
#endif
          for (size_t pni = 0; pni < pathNode.size(); pni++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            auto dstNodeID = pathNode.at(pni, outGraph.repository.allocator).id;
#else
            auto dstNodeID = pathNode[pni].id;
#endif
            auto dstNode = neighbors.find(dstNodeID);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            if (dstNode != neighbors.end() &&
                srcNode.at(sni, outGraph.repository.allocator).distance < (*dstNode).second.second &&
                pathNode.at(pni, outGraph.repository.allocator).distance < (*dstNode).second.second) {
#else
            if (dstNode != neighbors.end() && srcNode[sni].distance < (*dstNode).second.second &&
                pathNode[pni].distance < (*dstNode).second.second) {
#endif
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
              candidates.push_back(std::pair<uint32_t, std::pair<uint32_t, uint32_t>>(
                  (*dstNode).second.first,
                  std::pair<uint32_t, uint32_t>(srcNode.at(sni, outGraph.repository.allocator).id,
                                                dstNodeID)));
#else
              candidates.push_back(std::pair<uint32_t, std::pair<uint32_t, uint32_t>>(
                  (*dstNode).second.first, std::pair<uint32_t, uint32_t>(srcNode[sni].id, dstNodeID)));
#endif
              removeCandidateCount++;
            }
          }
        }
        sort(candidates.begin(), candidates.end(),
             std::greater<std::pair<uint32_t, std::pair<uint32_t, uint32_t>>>());
        removeCandidates[id - 1].reserve(candidates.size());
        for (size_t i = 0; i < candidates.size(); i++) {
          removeCandidates[id - 1].push_back(candidates[i].second);
        }
      } catch (NGT::Exception &err) {
        std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                  << std::endl;
        continue;
      }
    }
    timer.stop();
    std::cerr << "GraphReconstructor::adjustPaths: extracting removed edge candidates time=" << timer
              << std::endl;
    std::cerr << "removeCandidateCount=" << removeCandidateCount << std::endl;
    timer.reset();
    timer.start();

    std::list<uint32_t> ids;
    for (size_t idx = 0; idx < tmpGraph.size(); ++idx) {
      ids.push_back(idx + 1);
    }

    int removeCount      = 0;
    removeCandidateCount = 0;
    for (size_t rank = 0; ids.size() != 0; rank++) {
      for (auto it = ids.begin(); it != ids.end();) {
        size_t id  = *it;
        size_t idx = id - 1;
        try {
          NGT::GraphNode &srcNode = tmpGraph[idx];
          if (rank >= srcNode.size()) {
            if (!removeCandidates[idx].empty() && minNoOfEdges == 0) {
              std::cerr << "Something wrong! ID=" << id
                        << " # of remaining candidates=" << removeCandidates[idx].size() << std::endl;
              abort();
            }
#if !defined(NGT_SHARED_MEMORY_ALLOCATOR)
            NGT::GraphNode empty;
            tmpGraph[idx] = empty;
#endif
            it = ids.erase(it);
            continue;
          }
          if (removeCandidates[idx].size() > 0 &&
              ((*outGraph.getNode(id)).size() + srcNode.size() - rank) > minNoOfEdges) {
            removeCandidateCount++;
            bool pathExist = false;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            while (!removeCandidates[idx].empty() && (removeCandidates[idx].back().second ==
                                                      srcNode.at(rank, outGraph.repository.allocator).id)) {
#else
            while (!removeCandidates[idx].empty() &&
                   (removeCandidates[idx].back().second == srcNode[rank].id)) {
#endif
              size_t path = removeCandidates[idx].back().first;
              size_t dst  = removeCandidates[idx].back().second;
              removeCandidates[idx].pop_back();
              if (removeCandidates[idx].empty()) {
                std::vector<std::pair<uint32_t, uint32_t>> empty;
                removeCandidates[idx] = empty;
              }
              if ((hasEdge(outGraph, id, path)) && (hasEdge(outGraph, path, dst))) {
                pathExist = true;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
                while (!removeCandidates[idx].empty() &&
                       (removeCandidates[idx].back().second ==
                        srcNode.at(rank, outGraph.repository.allocator).id)) {
#else
                while (!removeCandidates[idx].empty() &&
                       (removeCandidates[idx].back().second == srcNode[rank].id)) {
#endif
                  removeCandidates[idx].pop_back();
                  if (removeCandidates[idx].empty()) {
                    std::vector<std::pair<uint32_t, uint32_t>> empty;
                    removeCandidates[idx] = empty;
                  }
                }
                break;
              }
            }
            if (pathExist) {
              removeCount++;
              it++;
              continue;
            }
          }
          NGT::GraphNode &outSrcNode = *outGraph.getNode(id);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          insert(outSrcNode, srcNode.at(rank, outGraph.repository.allocator).id,
                 srcNode.at(rank, outGraph.repository.allocator).distance, outGraph);
#else
          insert(outSrcNode, srcNode[rank].id, srcNode[rank].distance);
#endif
        } catch (NGT::Exception &err) {
          std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                    << std::endl;
          it++;
          continue;
        }
        it++;
      }
    }
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      try {
        NGT::GraphNode &node = *outGraph.getNode(id);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        std::sort(node.begin(outGraph.repository.allocator), node.end(outGraph.repository.allocator));
#else
        std::sort(node.begin(), node.end());
#endif
      } catch (...) {
      }
    }
  }

  static void removeShortcutEdges(NGT::GraphIndex &outGraph, int &removeCount, float range,
                                  size_t nOfThreads = 0) {
    std::cerr << "removeShortcutEdges";
#ifdef NGT_SHORTCUT_REDUCTION_WITH_ANGLE
    std::cerr << " with angle" << std::endl;
#endif
    std::cerr << ", range=" << range << std::endl;
    std::cerr << "  vm size(1)=" << NGT::Common::getProcessVmSizeStr() << ":"
              << NGT::Common::getProcessVmPeakStr() << std::endl;
    Timer timer;
    timer.start();

    uint32_t maxRank = 0;
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      if (id % 1000000 == 0) {
        std::cerr << "GraphReconstructor::adjustPathsInStages: # of the extracted nodes=" << id
                  << " vm size=" << NGT::Common::getProcessVmSizeStr() << ":"
                  << NGT::Common::getProcessVmPeakStr() << std::endl;
      }
      try {
        NGT::GraphNode &node = *outGraph.getNode(id);
        if (node.size() > maxRank) maxRank = node.size();
        for (size_t rank = 1; rank < node.size(); rank++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          auto &edge = node.at(rank, outGraph.repository.allocator);
#else
          auto &edge = node[rank];
#endif
          if ((edge.id & 0x80000000) != 0) {
            std::stringstream msg;
            msg << "ID is too large to reduce edges. ID=" << edge.id;
            NGTThrowException(msg);
          }
          edge.id |= 0x80000000;
        }
      } catch (NGT::Exception &err) {
        std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                  << std::endl;
      }
    }

    std::cerr << "vm size(2)=" << NGT::Common::getProcessVmSizeStr() << ":"
              << NGT::Common::getProcessVmPeakStr() << std::endl;
    std::cerr << "# of max threads=" << omp_get_max_threads() << std::endl;
    if (nOfThreads != 0) {
      omp_set_num_threads(nOfThreads);
    }
    auto nthreads = omp_get_max_threads();
    std::cerr << "# of threads=" << nthreads << std::endl;
    removeCount         = 0;
    auto repositorySize = outGraph.repository.size();
    std::cerr << "  vm size(3)=" << NGT::Common::getProcessVmSizeStr() << ":"
              << NGT::Common::getProcessVmPeakStr() << std::endl;
    double prevTime = 0.0;
    for (uint32_t rank = 1; rank < maxRank; rank++) {
      timer.stop();
      if (timer.time - prevTime > 4.0) {
        std::cerr << "rank=" << rank << " "
                  << "removed=" << removeCount << " " << NGT::Common::getProcessVmSizeStr() << "/"
                  << NGT::Common::getProcessVmPeakStr() << " time=" << timer << std::endl;
        prevTime = timer.time;
        timer.restart();
      }
#ifdef _OPENMP
#pragma omp parallel for
#endif
      for (uint32_t id = 1; id < repositorySize; ++id) {
        try {
          NGT::GraphNode &srcNode = *outGraph.getNode(id);
          if (rank >= srcNode.size()) continue;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          auto dstNodeID       = srcNode.at(rank, outGraph.repository.allocator).id & 0x7FFFFFFF;
          auto dstNodeDistance = srcNode.at(rank, outGraph.repository.allocator).distance;
#else
          auto dstNodeID       = srcNode[rank].id & 0x7FFFFFFF;
          auto dstNodeDistance = srcNode[rank].distance;
#endif
#ifdef NGT_SHORTCUT_REDUCTION_WITH_ANGLE
          auto dstNodeDistance2 = dstNodeDistance * dstNodeDistance;
#endif
          bool found = false;
          for (size_t sni = 0; sni < srcNode.size() && sni < rank; sni++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            auto pathNodeID = srcNode.at(sni, outGraph.repository.allocator).id;
#else
            auto pathNodeID = srcNode[sni].id;
#endif
            if ((pathNodeID & 0x80000000) != 0) continue;
#ifdef NGT_SHORTCUT_REDUCTION_WITH_ANGLE
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            auto srcNodeDistance2 = srcNode.at(sni, outGraph.repository.allocator).distance *
                                    srcNode.at(sni, outGraph.repository.allocator).distance;
#else
            auto srcNodeDistance2 = srcNode[sni].distance * srcNode[sni].distance;
#endif
#else
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            if (srcNode.at(sni, outGraph.repository.allocator).distance >= dstNodeDistance) continue;
#else
            if (srcNode[sni].distance >= dstNodeDistance) continue;
#endif
#endif
            NGT::GraphNode &pathNode = *outGraph.getNode(pathNodeID);
            for (size_t pni = 0; pni < pathNode.size() && pni <= rank; pni++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
              auto nodeID = pathNode.at(pni, outGraph.repository.allocator).id;
#else
              auto nodeID = pathNode[pni].id;
#endif
              if ((nodeID & 0x80000000) != 0) continue;
              if (nodeID != dstNodeID) continue;
#ifdef NGT_SHORTCUT_REDUCTION_WITH_ANGLE
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
              auto pathNodeDistance2 = pathNode.at(pni, outGraph.repository.allocator).distance *
                                       pathNode.at(pni, outGraph.repository.allocator).distance;
#else
              auto pathNodeDistance2 = pathNode[pni].distance * pathNode[pni].distance;
#endif
              auto v1 = srcNodeDistance2 + pathNodeDistance2 - dstNodeDistance2;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
              auto v2 = 2.0 * srcNode.at(sni, outGraph.repository.allocator).distance *
                        pathNode.at(pni, outGraph.repository.allocator).distance;
#else
              auto v2 = 2.0 * srcNode[sni].distance * pathNode[pni].distance;
#endif
              auto cosAlpha = v1 / v2;
              if (cosAlpha >= range) {
                break;
              }
#else
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
              if (pathNode.at(pni, outGraph.repository.allocator).distance >= dstNodeDistance) break;
#else
              if (pathNode[pni].distance >= dstNodeDistance) break;
#endif
#ifdef NGT_SHORTCUT_REDUCTION_WITH_ADDITIONAL_CONDITION
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
              if (srcNode.at(sni, outGraph.repository.allocator).distance +
                      pathNode.at(pni, outGraph.repository.allocator).distance >=
                  dstNodeDistance * range)
                break;
#else
              if (srcNode[sni].distance + pathNode[pni].distance >= dstNodeDistance * range) break;
#endif
#endif
#endif
              found = true;
              removeCount++;
              break;
            }
            if (found) break;
          }
          if (!found) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            srcNode.at(rank, outGraph.repository.allocator).id &= 0x7FFFFFFF;
#else
            srcNode[rank].id &= 0x7FFFFFFF;
#endif
          }
        } catch (NGT::Exception &err) {
          std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                    << std::endl;
          continue;
        }
      }
    }

    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      try {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        std::cerr << "Not implemented yet." << std::endl;
        abort();
#else
        NGT::GraphNode &node = *outGraph.getNode(id);
        node.erase(std::remove_if(node.begin(), node.end(),
                                  [](NGT::ObjectDistance &n) { return (n.id & 0x80000000) != 0; }),
                   node.end());
#endif
      } catch (...) {
      }
    }
  }

  static void removeShortcutEdges(NGT::GraphIndex &outGraph, const std::string &outIndexPath, float range,
                                  size_t nOfThreads, size_t minNoOfEdges) {
    Timer timer;
    timer.start();
    timer.stop();
    std::cerr << "GraphReconstructor::adjustPaths: graph preparing time=" << timer << std::endl;
    timer.reset();
    timer.start();

    int removeCandidateCount = 0;

    std::cerr << "Info vm size=" << NGT::Common::getProcessVmSizeStr() << ":"
              << NGT::Common::getProcessVmPeakStr() << ":" << NGT::Common::getSystemHWMStr() << std::endl;

    removeShortcutEdges(outGraph, removeCandidateCount, range, nOfThreads);
  }

  static void convertToANNG(std::vector<NGT::ObjectDistances> &graph) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    std::cerr << "convertToANNG is not implemented for shared memory." << std::endl;
    return;
#else
    std::cerr << "convertToANNG begin" << std::endl;
    for (size_t idx = 0; idx < graph.size(); idx++) {
      NGT::GraphNode &node = graph[idx];
      for (auto ni = node.begin(); ni != node.end(); ++ni) {
        graph[(*ni).id - 1].push_back(NGT::ObjectDistance(idx + 1, (*ni).distance));
      }
    }
    for (size_t idx = 0; idx < graph.size(); idx++) {
      NGT::GraphNode &node = graph[idx];
      if (node.size() == 0) {
        continue;
      }
      std::sort(node.begin(), node.end());
      NGT::ObjectID prev = 0;
      for (auto it = node.begin(); it != node.end();) {
        if (prev == (*it).id) {
          it = node.erase(it);
          continue;
        }
        prev = (*it).id;
        it++;
      }
      NGT::GraphNode tmp = node;
      node.swap(tmp);
    }
    std::cerr << "convertToANNG end" << std::endl;
#endif
  }

  static void reconstructGraph(std::vector<NGT::ObjectDistances> &graph, NGT::GraphIndex &outGraph,
                               size_t originalEdgeSize, size_t reverseEdgeSize,
                               int64_t maxNoOfEdges = std::numeric_limits<int64_t>::max()) {
    std::cerr << "GraphReconstructor::reconstructGraph:" << originalEdgeSize << ":" << reverseEdgeSize << ":"
              << maxNoOfEdges << std::endl;
    if (reverseEdgeSize > 10000) {
      std::cerr << "something wrong. Edge size=" << reverseEdgeSize << std::endl;
      exit(1);
    }

    NGT::Timer originalEdgeTimer, reverseEdgeTimer, normalizeEdgeTimer;
    originalEdgeTimer.start();

    size_t warningCount       = 0;
    const size_t warningLimit = 10;
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      if (id % 500000 == 0) {
        std::cerr << "Processed " << id << " out nodes. vm size=" << NGT::Common::getProcessVmSizeStr() << ":"
                  << NGT::Common::getProcessVmPeakStr() << std::endl;
      }
      try {
        NGT::GraphNode &node = *outGraph.getNode(id);
        if (originalEdgeSize == 0) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          node.clear(outGraph.repository.allocator);
#else
          NGT::GraphNode empty;
          node.swap(empty);
#endif
        } else {
          NGT::ObjectDistances n = graph[id - 1];
          if (n.size() < originalEdgeSize) {
            warningCount++;
            if (warningCount <= warningLimit) {
              std::cerr << "GraphReconstructor: Warning. The edges are too few. " << n.size() << ":"
                        << originalEdgeSize << " for " << id << std::endl;
            }
            if (warningCount == warningLimit) {
              std::cerr << "GraphReconstructor: Info. Too many warnings. Warning is disabled." << std::endl;
            }
            continue;
          }
          n.resize(originalEdgeSize);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          node.copy(n, outGraph.repository.allocator);
#else
          node.swap(n);
#endif
        }
      } catch (NGT::Exception &err) {
        warningCount++;
        if (warningCount <= warningLimit) {
          std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                    << std::endl;
        }
        if (warningCount == warningLimit) {
          std::cerr << "GraphReconstructor: Info. Too many warnings. Warning is disabled." << std::endl;
        }
        continue;
      }
    }
    if (warningCount > warningLimit) {
      std::cerr << "GraphReconstructor: The total " << warningCount << " Warnings." << std::endl;
    }
    originalEdgeTimer.stop();

    reverseEdgeTimer.start();
    int insufficientNodeCount = 0;
    if (maxNoOfEdges == 0) {
      maxNoOfEdges = std::max(originalEdgeSize, reverseEdgeSize);
    }
    for (size_t id = 1; id <= graph.size(); ++id) {
      try {
        NGT::ObjectDistances &node = graph[id - 1];
        if (id % 500000 == 0) {
          std::cerr << "Processed " << id << " in nodes. vm size=" << NGT::Common::getProcessVmSizeStr()
                    << ":" << NGT::Common::getProcessVmPeakStr() << std::endl;
        }
        size_t rsize = reverseEdgeSize;
        if (rsize > node.size()) {
          insufficientNodeCount++;
          rsize = node.size();
        }
        for (size_t i = 0; i < rsize; ++i) {
          NGT::Distance distance = node[i].distance;
          size_t nodeID          = node[i].id;
          try {
            NGT::GraphNode &n = *outGraph.getNode(nodeID);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            n.push_back(NGT::ObjectDistance(id, distance), outGraph.repository.allocator);
#else
            if (static_cast<int64_t>(n.size()) >= maxNoOfEdges) {
              if ((n.back().distance > distance) || (n.back().distance == distance && n.back().id > nodeID)) {
                NGT::ObjectDistance obj(id, distance);
                auto ni = std::lower_bound(n.begin(), n.end(), obj);
                n.insert(ni, obj);
                n.resize(maxNoOfEdges);
              }
            } else {
              n.emplace_back(NGT::ObjectDistance(id, distance));
              if (static_cast<int64_t>(n.size()) >= maxNoOfEdges) {
                std::sort(n.begin(), n.end());
              }
            }
#endif
          } catch (...) {
          }
        }
        node.clear();
        node.shrink_to_fit();
      } catch (NGT::Exception &err) {
        std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                  << std::endl;
        continue;
      }
    }
    reverseEdgeTimer.stop();
    if (insufficientNodeCount != 0) {
      std::cerr << "# of the nodes edges of which are in short = " << insufficientNodeCount << std::endl;
    }

    normalizeEdgeTimer.start();
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      try {
        NGT::GraphNode &n = *outGraph.getNode(id);
        if (id % 1000000 == 0) {
          std::cerr << "Processed " << id << " sorted nodes" << std::endl;
        }
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        std::sort(n.begin(outGraph.repository.allocator), n.end(outGraph.repository.allocator));
#else
        std::sort(n.begin(), n.end());
#endif
        NGT::ObjectID prev = 0;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        for (auto it = n.begin(outGraph.repository.allocator); it != n.end(outGraph.repository.allocator);) {
#else
        for (auto it = n.begin(); it != n.end();) {
#endif
          if (prev == (*it).id) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            it = n.erase(it, outGraph.repository.allocator);
#else
            it = n.erase(it);
#endif
            continue;
          }
          prev = (*it).id;
          it++;
        }
#if !defined(NGT_SHARED_MEMORY_ALLOCATOR)
        NGT::GraphNode tmp = n;
        n.swap(tmp);
#endif
      } catch (NGT::Exception &err) {
        std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                  << std::endl;
        continue;
      }
    }
    normalizeEdgeTimer.stop();
    std::cerr << "Reconstruction time=" << originalEdgeTimer.time << ":" << reverseEdgeTimer.time << ":"
              << normalizeEdgeTimer.time << std::endl;

    NGT::Property prop;
    outGraph.getProperty().get(prop);
    prop.graphType = NGT::NeighborhoodGraph::GraphTypeONNG;
    outGraph.getProperty().set(prop);
  }

  static void reconstructGraphWithConstraint(std::vector<NGT::ObjectDistances> &graph,
                                             NGT::GraphIndex &outGraph, size_t originalEdgeSize,
                                             size_t reverseEdgeSize, char mode = 'a') {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    std::cerr << "reconstructGraphWithConstraint is not implemented." << std::endl;
    abort();
#else

    NGT::Timer originalEdgeTimer, reverseEdgeTimer, normalizeEdgeTimer;

    if (reverseEdgeSize > 10000) {
      std::cerr << "something wrong. Edge size=" << reverseEdgeSize << std::endl;
      exit(1);
    }

    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      if (id % 1000000 == 0) {
        std::cerr << "Processed " << id << std::endl;
      }
      try {
        NGT::GraphNode &node = *outGraph.getNode(id);
        if (node.size() == 0) {
          continue;
        }
        node.clear();
        NGT::GraphNode empty;
        node.swap(empty);
      } catch (NGT::Exception &err) {
        std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                  << std::endl;
        continue;
      }
    }
    NGT::GraphIndex::showStatisticsOfGraph(outGraph);

    std::vector<ObjectDistances> reverse(graph.size() + 1);
    for (size_t id = 1; id <= graph.size(); ++id) {
      try {
        NGT::GraphNode &node = graph[id - 1];
        if (id % 100000 == 0) {
          std::cerr << "Processed (summing up) " << id << std::endl;
        }
        for (size_t rank = 0; rank < node.size(); rank++) {
          reverse[node[rank].id].push_back(ObjectDistance(id, node[rank].distance));
        }
      } catch (NGT::Exception &err) {
        std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                  << std::endl;
        continue;
      }
    }

    std::vector<std::pair<size_t, size_t>> reverseSize(graph.size() + 1);
    reverseSize[0] = std::pair<size_t, size_t>(0, 0);
    for (size_t rid = 1; rid <= graph.size(); ++rid) {
      reverseSize[rid] = std::pair<size_t, size_t>(reverse[rid].size(), rid);
    }
    std::sort(reverseSize.begin(), reverseSize.end());

    std::vector<uint32_t> indegreeCount(graph.size(), 0);
    size_t zeroCount = 0;
    for (size_t sizerank = 0; sizerank <= reverseSize.size(); sizerank++) {

      if (reverseSize[sizerank].first == 0) {
        zeroCount++;
        continue;
      }
      size_t rid             = reverseSize[sizerank].second;
      ObjectDistances &rnode = reverse[rid];
      for (auto rni = rnode.begin(); rni != rnode.end(); ++rni) {
        if (indegreeCount[(*rni).id] >= reverseEdgeSize) {
          continue;
        }
        NGT::GraphNode &node = *outGraph.getNode(rid);
        if (indegreeCount[(*rni).id] > 0 && node.size() >= originalEdgeSize) {
          continue;
        }

        node.push_back(NGT::ObjectDistance((*rni).id, (*rni).distance));
        indegreeCount[(*rni).id]++;
      }
    }
    reverseEdgeTimer.stop();
    std::cerr << "The number of nodes with zero outdegree by reverse edges=" << zeroCount << std::endl;
    NGT::GraphIndex::showStatisticsOfGraph(outGraph);

    normalizeEdgeTimer.start();
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      try {
        NGT::GraphNode &n = *outGraph.getNode(id);
        if (id % 100000 == 0) {
          std::cerr << "Processed " << id << std::endl;
        }
        std::sort(n.begin(), n.end());
        NGT::ObjectID prev = 0;
        for (auto it = n.begin(); it != n.end();) {
          if (prev == (*it).id) {
            it = n.erase(it);
            continue;
          }
          prev = (*it).id;
          it++;
        }
        NGT::GraphNode tmp = n;
        n.swap(tmp);
      } catch (NGT::Exception &err) {
        std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                  << std::endl;
        continue;
      }
    }
    normalizeEdgeTimer.stop();
    NGT::GraphIndex::showStatisticsOfGraph(outGraph);

    originalEdgeTimer.start();
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      if (id % 1000000 == 0) {
        std::cerr << "Processed " << id << std::endl;
      }
      NGT::GraphNode &node = graph[id - 1];
      try {
        NGT::GraphNode &onode = *outGraph.getNode(id);
        bool stop             = false;
        for (size_t rank = 0; (rank < node.size() && rank < originalEdgeSize) && stop == false; rank++) {
          switch (mode) {
          case 'a':
            if (onode.size() >= originalEdgeSize) {
              stop = true;
              continue;
            }
            break;
          case 'c': break;
          }
          NGT::Distance distance = node[rank].distance;
          size_t nodeID          = node[rank].id;
          outGraph.addEdge(id, nodeID, distance, false);
        }
      } catch (NGT::Exception &err) {
        std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what()
                  << std::endl;
        continue;
      }
    }
    originalEdgeTimer.stop();
    NGT::GraphIndex::showStatisticsOfGraph(outGraph);

    std::cerr << "Reconstruction time=" << originalEdgeTimer.time << ":" << reverseEdgeTimer.time << ":"
              << normalizeEdgeTimer.time << std::endl;

#endif
  }

  // reconstruct a pseudo ANNG with a fewer edges from an actual ANNG with more edges.
  // graph is a source ANNG
  // index is an index with a reconstructed ANNG
  static void reconstructANNGFromANNG(std::vector<NGT::ObjectDistances> &graph, NGT::Index &index,
                                      size_t edgeSize) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    std::cerr << "reconstructANNGFromANNG is not implemented." << std::endl;
    abort();
#else

    NGT::GraphIndex &outGraph = dynamic_cast<NGT::GraphIndex &>(index.getIndex());

    // remove all edges in the index.
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      if (id % 1000000 == 0) {
        std::cerr << "Processed " << id << " nodes." << std::endl;
      }
      try {
        NGT::GraphNode &node = *outGraph.getNode(id);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        node.clear(outGraph.repository.allocator);
#else
        NGT::GraphNode empty;
        node.swap(empty);
#endif
      } catch (NGT::Exception &err) {
      }
    }

    for (size_t id = 1; id <= graph.size(); ++id) {
      size_t edgeCount = 0;
      try {
        NGT::ObjectDistances &node = graph[id - 1];
        NGT::GraphNode &n          = *outGraph.getNode(id);
        NGT::Distance prevDistance = 0.0;
        assert(n.size() == 0);
        for (size_t i = 0; i < node.size(); ++i) {
          NGT::Distance distance = node[i].distance;
          if (prevDistance > distance) {
            NGTThrowException("Edge distance order is invalid");
          }
          prevDistance  = distance;
          size_t nodeID = node[i].id;
          if (node[i].id < id) {
            try {
              NGT::GraphNode &dn = *outGraph.getNode(nodeID);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
              n.push_back(NGT::ObjectDistance(nodeID, distance), outGraph.repository.allocator);
              dn.push_back(NGT::ObjectDistance(id, distance), outGraph.repository.allocator);
#else
              n.push_back(NGT::ObjectDistance(nodeID, distance));
              dn.push_back(NGT::ObjectDistance(id, distance));
#endif
            } catch (...) {
            }
            edgeCount++;
          }
          if (edgeCount >= edgeSize) {
            break;
          }
        }
      } catch (NGT::Exception &err) {
      }
    }

    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      try {
        NGT::GraphNode &n = *outGraph.getNode(id);
        std::sort(n.begin(), n.end());
        NGT::ObjectID prev = 0;
        for (auto it = n.begin(); it != n.end();) {
          if (prev == (*it).id) {
            it = n.erase(it);
            continue;
          }
          prev = (*it).id;
          it++;
        }
        NGT::GraphNode tmp = n;
        n.swap(tmp);
      } catch (...) {
      }
    }
#endif
  }

  static void refineANNG(NGT::Index &index, bool unlog, float epsilon = 0.1, float accuracy = 0.0,
                         int noOfEdges = 0, int exploreEdgeSize = INT_MIN, size_t batchSize = 10000) {
    NGT::StdOstreamRedirector redirector(unlog);
    redirector.begin();
    try {
      refineANNG(index, epsilon, accuracy, noOfEdges, exploreEdgeSize, batchSize);
    } catch (NGT::Exception &err) {
      redirector.end();
      throw(err);
    }
  }

  static void refineANNG(NGT::Index &index, float epsilon = 0.1, float accuracy = 0.0, int noOfEdges = 0,
                         int exploreEdgeSize = INT_MIN, size_t batchSize = 10000) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    NGTThrowException("GraphReconstructor::refineANNG: Not implemented for the shared memory option.");
#else
    auto prop                               = static_cast<GraphIndex &>(index.getIndex()).getGraphProperty();
    NGT::ObjectRepository &objectRepository = index.getObjectSpace().getRepository();
    NGT::GraphIndex &graphIndex             = static_cast<GraphIndex &>(index.getIndex());
    size_t nOfObjects                       = objectRepository.size();
    bool error                              = false;
    std::string errorMessage;

    size_t noOfSearchedEdges =
        noOfEdges < 0 ? -noOfEdges
                      : (noOfEdges > prop.edgeSizeForCreation ? noOfEdges : prop.edgeSizeForCreation);
    noOfSearchedEdges++;
    for (size_t bid = 1; bid < nOfObjects; bid += batchSize) {
      NGT::ObjectDistances results[batchSize];
      // search
#pragma omp parallel for
      for (size_t idx = 0; idx < batchSize; idx++) {
        size_t id = bid + idx;
        if (id % 100000 == 0) {
          std::cerr << "# of processed objects=" << id
                    << " objects. vm size=" << NGT::Common::getProcessVmSizeStr() << std::endl;
        }
        if (objectRepository.isEmpty(id)) {
          continue;
        }
        NGT::SearchContainer searchContainer(*objectRepository.get(id));
        searchContainer.setResults(&results[idx]);
        assert(prop.edgeSizeForCreation > 0);
        searchContainer.setSize(noOfSearchedEdges);
        if (accuracy > 0.0) {
          searchContainer.setExpectedAccuracy(accuracy);
        } else {
          searchContainer.setEpsilon(epsilon);
        }
        if (exploreEdgeSize != INT_MIN) {
          searchContainer.setEdgeSize(exploreEdgeSize);
        }
        if (!error) {
          try {
            index.search(searchContainer);
          } catch (NGT::Exception &err) {
#pragma omp critical
            {
              error        = true;
              errorMessage = err.what();
            }
          }
        }
      }
      if (error) {
        std::stringstream msg;
        msg << "GraphReconstructor::refineANNG: " << errorMessage;
        NGTThrowException(msg);
      }
      // outgoing edges
#pragma omp parallel for
      for (size_t idx = 0; idx < batchSize; idx++) {
        size_t id = bid + idx;
        if (objectRepository.isEmpty(id)) {
          continue;
        }
        NGT::GraphNode &node = *graphIndex.getNode(id);
        for (auto i = results[idx].begin(); i != results[idx].end(); ++i) {
          if ((*i).id != id) {
            node.emplace_back(*i);
          }
        }
        std::sort(node.begin(), node.end());
        // dedupe
        ObjectID prev = 0;
        for (GraphNode::iterator ni = node.begin(); ni != node.end();) {
          if (prev == (*ni).id) {
            ni = node.erase(ni);
            continue;
          }
          prev = (*ni).id;
          ni++;
        }
        node.shrink_to_fit();
      }
      // incomming edges
      if (noOfEdges > 0) {
        continue;
      }
      for (size_t idx = 0; idx < batchSize; idx++) {
        size_t id = bid + idx;
        if (id % 10000 == 0) {
          std::cerr << "# of processed objects=" << id << std::endl;
        }
        for (auto i = results[idx].begin(); i != results[idx].end(); ++i) {
          if ((*i).id != id) {
            NGT::GraphNode &node = *graphIndex.getNode((*i).id);
            graphIndex.addEdge(node, id, (*i).distance, false);
          }
        }
      }
    }
    if (noOfEdges > 0) {
      // prune to build knng
      size_t nedges = noOfEdges < 0 ? -noOfEdges : noOfEdges;
#pragma omp parallel for
      for (ObjectID id = 1; id < nOfObjects; ++id) {
        if (objectRepository.isEmpty(id)) {
          continue;
        }
        NGT::GraphNode &node = *graphIndex.getNode(id);
        if (node.size() > nedges) {
          node.resize(nedges);
        }
      }
    }
#endif // defined(NGT_SHARED_MEMORY_ALLOCATOR)
  }
}; // namespace NGT

}; // namespace NGT
