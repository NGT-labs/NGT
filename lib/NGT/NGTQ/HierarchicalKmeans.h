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

#include "Quantizer.h"
#include "NGT/GraphOptimizer.h"

namespace QBG {
class Index;
class BuildParameters;
class HierarchicalClusteringParameters;
class OptimizationParameters;

class HierarchicalKmeans {
 public:
  typedef NGTQ::Quantizer::ObjectList QBGObjectList;

  enum ClusteringType {
    ClusteringTypeMultiLayer             = 0,
    ClusteringTypeThreeLayer             = 1,
    ClusteringTypeTwoPlusOneLayer        = 2,
    ClusteringTypeTwoPlusOneLayerWithNGT = 3,
    ClusteringTypeTwoPlusTwoLayer        = 4
  };

  class HKNode {
   public:
    bool leaf;
  };

  class HKLeafNode : public HKNode {
   public:
    HKLeafNode() : id(0) { leaf = true; }
    std::vector<uint32_t> members;
    uint32_t id;
  };

  class HKInternalNode : public HKNode {
   public:
    HKInternalNode() { leaf = false; }
    std::vector<std::pair<uint32_t, std::vector<float>>> children;
  };

  HierarchicalKmeans() { initialize(); }
  HierarchicalKmeans(QBG::BuildParameters &param);
  HierarchicalKmeans(QBG::HierarchicalClusteringParameters &hierarchicalClustering);

  void setParameters(QBG::HierarchicalClusteringParameters &hierarchicalClustering);
  void initialize();

  static int32_t searchLeaf(std::vector<HKNode *> &nodes, int32_t rootID, float *object) {
    auto nodeID = rootID;
    while (true) {
      auto *node = nodes[nodeID];
      if (node->leaf) {
        return nodeID;
      } else {
        HKInternalNode &internalNode = static_cast<HKInternalNode &>(*node);
        float min                    = std::numeric_limits<float>::max();
        int32_t minid                = 0;
        for (auto &c : internalNode.children) {
          auto d = NGT::PrimitiveComparator::compareL2(reinterpret_cast<float *>(&object[0]), c.second.data(),
                                                       c.second.size());
          if (d < min) {
            min   = d;
            minid = c.first;
          }
        }
        nodeID = minid;
      }
    }
    return -1;
  }

  static void aggregateObjects(HKLeafNode &leafNode, std::vector<std::vector<float>> &vectors,
                               NGT::ObjectSpace &objectSpace, QBGObjectList &objectList) {
    vectors.reserve(leafNode.members.size() + 1);
    std::vector<float> obj;
    for (auto &m : leafNode.members) {
      objectList.get(m, obj, &objectSpace);
      vectors.push_back(obj);
    }
  }

  static void aggregateObjects(HKLeafNode &leafNode, std::vector<std::vector<float>> &vectors,
                               NGT::ObjectSpace &objectSpace, QBGObjectList &objectList,
                               std::vector<float> &object) {
    aggregateObjects(leafNode, vectors, objectSpace, objectList);
    vectors.push_back(std::move(object));
  }

  static void split(uint32_t id, std::vector<std::vector<float>> &vectors, std::vector<HKNode *> &nodes,
                    int32_t leafNodeID, NGT::Clustering &clustering) {
    HKLeafNode &leafNode = static_cast<HKLeafNode &>(*nodes[leafNodeID]);
    std::vector<NGT::Clustering::Cluster> clusters;
    clustering.kmeans(vectors, clusters);
    auto *newNode = new HKInternalNode;
    for (auto &cluster : clusters) {
      auto centroid = std::move(cluster.centroid);
      centroid.resize(vectors[0].size());
      newNode->children.push_back(std::make_pair(nodes.size(), std::move(centroid)));
      auto *cnode = new HKLeafNode;
      nodes.push_back(cnode);
      for (auto &member : cluster.members) {
        if (member.vectorID > leafNode.members.size()) {
          std::cerr << "Fatal error. member:" << member.vectorID << ":" << leafNode.members.size()
                    << std::endl;
          abort();
        }
        if (member.vectorID == leafNode.members.size()) {
          cnode->members.push_back(id);
        } else {
          cnode->members.push_back(leafNode.members[member.vectorID]);
        }
      }
    }
    delete nodes[leafNodeID];
    nodes[leafNodeID] = newNode;
  }

  static double computeError(std::vector<HKNode *> &nodes, NGT::ObjectSpace &objectSpace,
                             QBGObjectList &objectList) {
    std::cerr << "node size=" << nodes.size() << std::endl;
    double distance = 0.0;
    size_t dcount   = 0;
    for (auto *node : nodes) {
      if (node->leaf) {
      } else {
        HKInternalNode &internalNode = static_cast<HKInternalNode &>(*node);
        std::vector<float> obj;
        for (auto &child : internalNode.children) {
          if (nodes[child.first]->leaf) {
            if (dcount % 100000 == 0) {
              std::cerr << "Processed leaves=" << dcount << std::endl;
            }
            auto centroid        = child.second;
            HKLeafNode &leafNode = static_cast<HKLeafNode &>(*nodes[child.first]);
            for (auto &m : leafNode.members) {
              objectList.get(m, obj, &objectSpace);
              distance += NGT::Clustering::distanceL2(centroid, obj);
              dcount++;
            }
          }
        }
      }
    }
    distance /= dcount;
    std::cout << "# of vectors=" << dcount << std::endl;
    std::cout << "Quantization error=" << distance << std::endl;
    return distance;
  }

  static size_t extractCentroids(std::ostream &oStream, std::vector<HKNode *> &nodes) {
    std::cerr << "node size=" << nodes.size() << std::endl;
    size_t clusterCount = 0;
    size_t objectCount  = 0;
    size_t leafID       = 0;
    for (auto *node : nodes) {
      if (node->leaf) {
        HKLeafNode &leafNode = static_cast<HKLeafNode &>(*node);
        objectCount += leafNode.members.size();
      } else {
        HKInternalNode &internalNode = static_cast<HKInternalNode &>(*node);
        for (auto &child : internalNode.children) {
          if (nodes[child.first]->leaf) {
            if (static_cast<HKLeafNode *>(nodes[child.first])->id == 0) {
              static_cast<HKLeafNode *>(nodes[child.first])->id = leafID;
            } else if (static_cast<HKLeafNode *>(nodes[child.first])->id != leafID) {
              std::cerr << "leaf ID is invalid?" << std::endl;
            }
            leafID++;
            size_t count = 0;
            clusterCount++;
            for (auto &v : child.second) {
              oStream << v;
              if (++count == child.second.size()) {
                oStream << std::endl;
              } else {
                oStream << "\t";
              }
            }
          }
        }
      }
    }
    std::cerr << "# of clusters=" << clusterCount << std::endl;
    return objectCount;
  }

  static size_t extractIndex(std::ostream &oStream, std::vector<HKNode *> &nodes, size_t numOfObjects) {
    std::vector<int32_t> clusterID(numOfObjects, -1);
    std::cerr << "numOfObjects=" << numOfObjects << std::endl;
    std::cerr << "node size=" << nodes.size() << std::endl;
    for (auto *node : nodes) {
      if (node->leaf) {
        HKLeafNode &leafNode = static_cast<HKLeafNode &>(*node);
        for (auto &member : leafNode.members) {
          if (member > numOfObjects) {
            std::cerr << "output index: Internal fatal error. " << member << ":" << numOfObjects - 1
                      << std::endl;
            abort();
          }
          if (member == 0) {
            std::cerr << "output index: Internal fatal error. Invalid ID" << std::endl;
            abort();
          }
          clusterID[member - 1] = leafNode.id;
        }
      }
    }
    std::cerr << "clusterID.size=" << clusterID.size() << std::endl;
    size_t count = 0;
    for (auto cid : clusterID) {
      count++;
      oStream << cid << std::endl;
    }
    std::cerr << "# of id=" << count << std::endl;
    return count;
  }

  static void extractBtoQAndQCentroid(std::ostream &btoqStream, std::ostream &qStream,
                                      std::vector<HKNode *> &nodes, size_t numOfThirdClusters) {
    std::cerr << "extractBtoQ" << std::endl;
    std::vector<int32_t> btoq(numOfThirdClusters);
    std::cerr << "numOfThirdClusters=" << numOfThirdClusters << std::endl;
    std::cerr << "node size=" << nodes.size() << std::endl;
    size_t rootID        = 0;
    HKInternalNode &root = static_cast<HKInternalNode &>(*nodes[rootID]);
    std::cerr << "first=" << root.children.size() << std::endl;
    size_t secondCount = 0;
    size_t thirdCount  = 0;
    size_t objectCount = 0;
    size_t leafID      = 0;
    size_t qID         = 0;
    for (auto &c1 : root.children) {
      HKInternalNode &node1 = static_cast<HKInternalNode &>(*nodes[c1.first]);
      std::cerr << "second=" << node1.children.size() << std::endl;
      secondCount += node1.children.size();
      for (auto &c2 : node1.children) {
        HKInternalNode &node2 = static_cast<HKInternalNode &>(*nodes[c2.first]);
        std::cerr << "third=" << node2.children.size() << std::endl;
        thirdCount += node2.children.size();
        size_t count = 0;
        for (auto &v : c2.second) {
          qStream << v;
          if (++count == c2.second.size()) {
            qStream << std::endl;
          } else {
            qStream << "\t";
          }
        }
        for (auto &c3 : node2.children) {
          btoqStream << qID << std::endl;
          HKLeafNode &leaf = static_cast<HKLeafNode &>(*nodes[c3.first]);
          objectCount += leaf.members.size();
          if (leaf.id != leafID++) {
            std::cerr << "leaf is invalid" << leaf.id << ":" << leafID << std::endl;
            abort();
          }
        }
        qID++;
      }
    }
    std::cerr << "second=" << secondCount << std::endl;
    std::cerr << "third=" << thirdCount << std::endl;
    std::cerr << "object=" << objectCount << std::endl;
  }

  static void extractRandomObjectsFromEachBlob(std::ostream &oStream, std::vector<HKNode *> &nodes,
                                               size_t numOfObjects, size_t numOfRandomObjects,
                                               NGTQ::QuantizerInstance<uint8_t> &quantizer,
                                               bool extractCentroid) {
    std::cerr << "node size=" << nodes.size() << std::endl;
    std::vector<std::vector<std::vector<float>>> randomObjects(numOfObjects);
    std::vector<std::vector<float>> centroids(numOfObjects);
    for (auto *node : nodes) {
      if (node->leaf) {
        HKLeafNode &leafNode = static_cast<HKLeafNode &>(*node);
        std::vector<uint32_t> randomObjectIDXs;
        if (numOfRandomObjects >= leafNode.members.size()) {
          randomObjectIDXs = leafNode.members;
          while (randomObjectIDXs.size() < numOfRandomObjects) {
            double random = ((double)rand() + 1.0) / ((double)RAND_MAX + 2.0);
            uint32_t idx  = floor(leafNode.members.size() * random);
            if (idx >= leafNode.members.size()) {
              std::cerr << "Internal error. " << idx << ":" << leafNode.members.size() << std::endl;
              abort();
            }
            randomObjectIDXs.push_back(leafNode.members[idx]);
          }
        } else {
          srand(leafNode.id);
          while (randomObjectIDXs.size() < numOfRandomObjects) {
            uint32_t idx = 0;
            do {
              double random = ((double)rand() + 1.0) / ((double)RAND_MAX + 2.0);
              idx           = floor(leafNode.members.size() * random);
              if (idx >= leafNode.members.size()) {
                std::cerr << "Internal error. " << idx << ":" << leafNode.members.size() << std::endl;
                abort();
              }
            } while (std::find(randomObjectIDXs.begin(), randomObjectIDXs.end(), leafNode.members[idx]) !=
                     randomObjectIDXs.end());
            std::cerr << "IDX=" << idx << "/" << leafNode.members.size() << std::endl;
            randomObjectIDXs.push_back(leafNode.members[idx]);
          }
        }
        std::cerr << "randomObjectIDXs=" << randomObjectIDXs.size() << std::endl;
        for (auto member : randomObjectIDXs) {
          if (member == 0) {
            std::cerr << "output index: Internal fatal error. Invalid ID. " << member << std::endl;
            abort();
          }
          std::vector<float> object;
          quantizer.objectList.get(member, object, &quantizer.globalCodebookIndex.getObjectSpace());
          if (leafNode.id >= numOfObjects) {
            std::cerr << "Internal error! Wrong leaf ID. " << leafNode.id << ":" << numOfObjects << std::endl;
            abort();
          }
          randomObjects[leafNode.id].push_back(object);
        }
      } else {
        if (extractCentroid) {
          HKInternalNode &internalNode = static_cast<HKInternalNode &>(*node);
          for (auto &child : internalNode.children) {
            if (nodes[child.first]->leaf) {
              HKLeafNode &leafNode   = static_cast<HKLeafNode &>(*nodes[child.first]);
              centroids[leafNode.id] = child.second;
            }
          }
        }
      }
    }
    for (size_t idx = 0; idx < centroids.size(); idx++) {
      auto &c = centroids[idx];
      if (extractCentroid && c.empty()) {
        std::cerr << "qbg: Fatal error! The centroid is empty." << std::endl;
        abort();
      }
      for (size_t i = 0; i < c.size(); i++) {
        oStream << c[i];
        if (i + 1 != c.size()) {
          oStream << "\t";
        } else {
          oStream << std::endl;
          ;
        }
      }
      auto &ros = randomObjects[idx];
      for (auto &ro : ros) {
        if (ro.empty()) {
          std::cerr << "qbg: Fatal error! The random object vector is empty." << std::endl;
          abort();
        }
        for (size_t i = 0; i < ro.size(); i++) {
          oStream << ro[i];
          if (i + 1 != ro.size()) {
            oStream << "\t";
          } else {
            oStream << std::endl;
            ;
          }
        }
      }
    }
  }

  static void extractBtoQIndex(std::ofstream &of, std::vector<HKNode *> &nodes,
                               std::vector<uint32_t> &qNodeIDs) {
    size_t leafID = 0;
    for (size_t qnidx = 0; qnidx < qNodeIDs.size(); qnidx++) {
      if (nodes[qNodeIDs[qnidx]]->leaf) {
        std::cerr << "Fatal error. this should be an internal node." << std::endl;
        abort();
      }
      HKInternalNode &inode = static_cast<HKInternalNode &>(*nodes[qNodeIDs[qnidx]]);
      for (auto &c : inode.children) {
        if (!nodes[c.first]->leaf) {
          std::cerr << "Fatal error. this should be a leaf." << std::endl;
          abort();
        }
        HKLeafNode &leaf = static_cast<HKLeafNode &>(*nodes[c.first]);
        if (leaf.id == 0) {
          leaf.id = leafID;
        }
        of << qnidx << std::endl;
        leafID++;
      }
    }
  }

  static void hierarchicalKmeans(uint32_t id, int32_t rootID, std::vector<float> &object,
                                 QBGObjectList &objectList, NGT::ObjectSpace &objectSpace,
                                 std::vector<HKNode *> &nodes, NGT::Clustering &clustering, size_t maxSize) {
    NGT::Timer timer;
    objectList.get(id, object, &objectSpace);
    int32_t nodeID = searchLeaf(nodes, rootID, reinterpret_cast<float *>(&object[0]));
    if (nodeID < 0) {
      std::cerr << "Fatal inner error! node ID=" << nodeID << std::endl;
      exit(1);
    }
    auto *node           = nodes[nodeID];
    HKLeafNode &leafNode = static_cast<HKLeafNode &>(*node);
    if (leafNode.members.size() >= maxSize) {
      NGT::Timer subtimer;
      subtimer.start();
      std::vector<std::vector<float>> vectors;
      aggregateObjects(leafNode, vectors, objectSpace, objectList, object);
      subtimer.stop();
      std::cerr << "aggregate time=" << subtimer << std::endl;
      subtimer.start();
      split(id, vectors, nodes, nodeID, clustering);
      subtimer.stop();
      std::cerr << "split time=" << subtimer << std::endl;
    } else {
      leafNode.members.push_back(id);
    }
  }

  static void hierarchicalKmeansBatch(std::vector<uint32_t> &batch,
                                      std::vector<pair<uint32_t, uint32_t>> &exceededLeaves, int32_t rootID,
                                      std::vector<float> &object, QBGObjectList &objectList,
                                      NGT::ObjectSpace &objectSpace, std::vector<HKNode *> &nodes,
                                      NGT::Clustering &clustering, size_t maxSize, size_t &nleaves,
                                      size_t maxExceededLeaves) {

    if (batch.size() == 0) {
      return;
    }

    int32_t nodeIDs[batch.size()];

#pragma omp parallel for
    for (size_t idx = 0; idx < batch.size(); idx++) {
      auto id = batch[idx];
#pragma omp critical
      objectList.get(id, object, &objectSpace);
      int32_t nodeID = searchLeaf(nodes, rootID, reinterpret_cast<float *>(&object[0]));
      if (nodeID < 0) {
        std::cerr << "Fatal inner error! node ID=" << nodeID << std::endl;
        exit(1);
      }
      nodeIDs[idx] = nodeID;
    }

    for (size_t idx = 0; idx < batch.size(); idx++) {
      auto id              = batch[idx];
      HKLeafNode &leafNode = static_cast<HKLeafNode &>(*nodes[nodeIDs[idx]]);
      leafNode.members.push_back(id);
      if (leafNode.members.size() > maxSize) {
        auto i = exceededLeaves.begin();
        for (; i != exceededLeaves.end(); i++) {
          if (static_cast<int32_t>((*i).second) == nodeIDs[idx]) break;
        }
        if (i == exceededLeaves.end()) {
          exceededLeaves.push_back(std::make_pair(batch[idx], nodeIDs[idx]));
        }
      }
    }

    batch.clear();

    if (exceededLeaves.size() < maxExceededLeaves) {
      return;
    }

    std::vector<std::vector<NGT::Clustering::Cluster>> clusters(exceededLeaves.size());
#pragma omp parallel for
    for (size_t idx = 0; idx < exceededLeaves.size(); idx++) {
      HKLeafNode &leafNode = static_cast<HKLeafNode &>(*nodes[exceededLeaves[idx].second]);
      std::vector<std::vector<float>> vectors;
#pragma omp critical
      aggregateObjects(leafNode, vectors, objectSpace, objectList);
      clustering.kmeans(vectors, clusters[idx]);
    }

    std::cerr << "exceeded leaves=" << exceededLeaves.size() << std::endl;
    for (size_t idx = 0; idx < exceededLeaves.size(); idx++) {
      auto leafNodeID      = exceededLeaves[idx].second;
      HKLeafNode &leafNode = static_cast<HKLeafNode &>(*nodes[leafNodeID]);
      auto *newNode        = new HKInternalNode;
      for (auto &cluster : clusters[idx]) {
        newNode->children.push_back(std::make_pair(nodes.size(), std::move(cluster.centroid)));
        auto *cnode = new HKLeafNode;
        nodes.push_back(cnode);
        for (auto &member : cluster.members) {
          cnode->members.push_back(leafNode.members[member.vectorID]);
        }
      }
      nleaves += clusters[idx].size() - 1;
      delete nodes[leafNodeID];
      nodes[leafNodeID] = newNode;
    }
    exceededLeaves.clear();
  }

  static void hierarchicalKmeansWithNumberOfClusters(size_t numOfTotalClusters, size_t numOfObjects,
                                                     size_t numOfLeaves, QBGObjectList &objectList,
                                                     NGT::ObjectSpace &objectSpace,
                                                     std::vector<HKNode *> &nodes,
                                                     NGT::Clustering::InitializationMode initMode) {
    std::cerr << "numOfTotalClusters=" << numOfTotalClusters << std::endl;
    std::cerr << "numOfLeaves=" << numOfLeaves << std::endl;
    if (numOfLeaves > numOfTotalClusters) {
      std::cerr << "# of clusters is invalid. " << numOfLeaves << ":" << numOfTotalClusters << std::endl;
      abort();
    }
    auto numOfRemainingClusters = numOfTotalClusters;
    auto numOfRemainingVectors  = numOfObjects;
    size_t leafCount            = 0;
    size_t nodeSize             = nodes.size();
    for (size_t nidx = 0; nidx < nodeSize; nidx++) {
      if (nodes[nidx]->leaf) {
        leafCount++;
        if (numOfLeaves >= 100 && leafCount % (numOfLeaves / 100) == 0) {
          std::cerr << "Processed leaves: " << leafCount << " " << leafCount * 100 / numOfLeaves << "%"
                    << std::endl;
        }
        HKLeafNode &leafNode = static_cast<HKLeafNode &>(*nodes[nidx]);
        std::vector<std::vector<float>> vectors;
        aggregateObjects(leafNode, vectors, objectSpace, objectList);
        size_t nClusters = round(static_cast<float>(leafNode.members.size()) / numOfRemainingVectors *
                                 numOfRemainingClusters);
        nClusters        = nClusters == 0 ? 1 : nClusters;
        numOfRemainingVectors -= leafNode.members.size();
        numOfRemainingClusters -= nClusters;
        NGT::Clustering clustering(initMode, NGT::Clustering::ClusteringTypeKmeansWithoutNGT, 1000,
                                   nClusters);
        NGT::Timer timer;
        timer.start();
        split(0, vectors, nodes, nidx, clustering);
        timer.stop();
        if (nodes[nidx]->leaf) {
          std::cerr << "At this moment, the second node should be an internal" << std::endl;
          abort();
        }
      }
    }
  }

  static void hierarchicalKmeansWithNumberOfClustersInParallel(size_t numOfTotalClusters, size_t numOfObjects,
                                                               size_t numOfLeaves, QBGObjectList &objectList,
                                                               NGT::ObjectSpace &objectSpace,
                                                               std::vector<HKNode *> &nodes,
                                                               NGT::Clustering::InitializationMode initMode) {
    NGT::Timer timer;
    timer.start();
    auto numOfRemainingClusters = numOfTotalClusters;
    auto numOfRemainingVectors  = numOfObjects;
    size_t leafCount            = 0;

    std::vector<pair<uint32_t, size_t>> leafNodes;
    leafNodes.reserve(numOfLeaves);
    for (size_t nidx = 0; nidx < nodes.size(); nidx++) {
      if (nodes[nidx]->leaf) {
        leafCount++;
        {
          size_t step = 10;
          if (numOfLeaves >= step && leafCount % (numOfLeaves / step) == 0) {
            std::cerr << "Processed leaves: " << leafCount << " " << leafCount * step / numOfLeaves << "%"
                      << std::endl;
          }
        }
        HKLeafNode &leafNode = static_cast<HKLeafNode &>(*nodes[nidx]);
        size_t nClusters     = round(static_cast<float>(leafNode.members.size()) / numOfRemainingVectors *
                                     numOfRemainingClusters);
        nClusters            = nClusters == 0 ? 1 : nClusters;
        numOfRemainingVectors -= leafNode.members.size();
        numOfRemainingClusters -= nClusters;
        leafNodes.push_back(std::make_pair(nidx, nClusters));
      }
    }
    timer.stop();
    std::cerr << "hierarchicalKmeansWithNumberOfClustersInParallel: extract leaves. Time=" << timer
              << std::endl;
    timer.start();

    std::cerr << "start kmeans..." << std::endl;
    std::vector<std::vector<NGT::Clustering::Cluster>> clusters(leafNodes.size());
#pragma omp parallel for
    for (size_t nidx = 0; nidx < leafNodes.size(); nidx++) {
      HKLeafNode &leafNode = static_cast<HKLeafNode &>(*nodes[leafNodes[nidx].first]);
      std::vector<std::vector<float>> vectors;
#pragma omp critical
      aggregateObjects(leafNode, vectors, objectSpace, objectList);
      NGT::Clustering clustering(initMode, NGT::Clustering::ClusteringTypeKmeansWithoutNGT, 1000,
                                 leafNodes[nidx].second);
      clustering.kmeans(vectors, clusters[nidx]);
    }

    timer.stop();
    std::cerr << "hierarchicalKmeansWithNumberOfClustersInParallel: kmeans. Time=" << timer << std::endl;
    timer.start();

    std::cerr << "add nodes..." << std::endl;
    for (size_t idx = 0; idx < leafNodes.size(); idx++) {
      auto leafNodeID      = leafNodes[idx].first;
      HKLeafNode &leafNode = static_cast<HKLeafNode &>(*nodes[leafNodeID]);
      auto *newNode        = new HKInternalNode;
      for (auto &cluster : clusters[idx]) {
        newNode->children.push_back(std::make_pair(nodes.size(), std::move(cluster.centroid)));
        auto *cnode = new HKLeafNode;
        nodes.push_back(cnode);
        for (auto &member : cluster.members) {
          cnode->members.push_back(leafNode.members[member.vectorID]);
        }
      }
      delete nodes[leafNodeID];
      nodes[leafNodeID] = newNode;
    }
    timer.stop();
    std::cerr << "hierarchicalKmeansWithNumberOfClustersInParallel: add nodes. Time=" << timer << std::endl;
  }

  static void flattenClusters(std::vector<NGT::Clustering::Cluster> &upperClusters,
                              std::vector<std::vector<NGT::Clustering::Cluster>> &lowerClusters,
                              size_t numOfLowerClusters,
                              std::vector<NGT::Clustering::Cluster> &flatClusters) {

    flatClusters.clear();
    flatClusters.reserve(numOfLowerClusters);

    for (size_t idx1 = 0; idx1 < lowerClusters.size(); idx1++) {
      for (size_t idx2 = 0; idx2 < lowerClusters[idx1].size(); idx2++) {
        for (auto &m : lowerClusters[idx1][idx2].members) {
          m.vectorID = upperClusters[idx1].members[m.vectorID].vectorID;
        }
        flatClusters.push_back(lowerClusters[idx1][idx2]);
      }
    }
  }

#ifndef MULTIPLE_OBJECT_LISTS
  void subclustering(std::vector<NGT::Clustering::Cluster> &upperClusters, size_t numOfLowerClusters,
                     size_t numOfObjects, NGT::ObjectSpace &objectSpace, QBGObjectList &objectList,
                     NGT::Clustering::InitializationMode initMode,
                     std::vector<std::vector<NGT::Clustering::Cluster>> &lowerClusters,
                     size_t maximumIteration = 1000) {
    std::vector<uint32_t> nPartialClusters(upperClusters.size());
    auto numOfRemainingClusters = numOfLowerClusters;
    auto numOfRemainingVectors  = numOfObjects;
    size_t ts                   = 0;
    for (size_t idx = 0; idx < upperClusters.size(); idx++) {
      size_t ncs = round(static_cast<float>(upperClusters[idx].members.size()) / numOfRemainingVectors *
                         numOfRemainingClusters);
      ncs        = ncs == 0 ? 1 : ncs;
      numOfRemainingVectors -= upperClusters[idx].members.size();
      if (numOfRemainingClusters >= ncs) {
        numOfRemainingClusters -= ncs;
      }
      nPartialClusters[idx] = ncs;
      ts += ncs;
    }
    std::cerr << "numOfRemainingClusters=" << numOfRemainingClusters << std::endl;
    std::cerr << "numOfRemainingVectors=" << numOfRemainingVectors << std::endl;
    std::cerr << "upperClusters=" << upperClusters.size() << std::endl;
    std::cerr << "total=" << ts << ":" << numOfLowerClusters << std::endl;
    if (ts < numOfLowerClusters || numOfRemainingClusters != 0) {
      std::cerr << "subclustering: Internal error! " << std::endl;
      exit(1);
    }

    lowerClusters.resize(upperClusters.size());
#pragma omp parallel for schedule(dynamic)
    for (size_t idx = 0; idx < upperClusters.size(); idx++) {
      std::vector<std::vector<float>> partialVectors;
      partialVectors.reserve(upperClusters[idx].members.size());
      std::vector<float> obj;
#pragma omp critical
      {
        for (auto &m : upperClusters[idx].members) {
          objectList.get(m.vectorID + 1, obj, &objectSpace);
          partialVectors.push_back(obj);
        }
      }
      if (upperClusters[idx].members.size() != partialVectors.size()) {
        std::cerr << "the sizes of members are not consistent" << std::endl;
        abort();
      }
      NGT::Clustering lowerClustering(initMode, NGT::Clustering::ClusteringTypeKmeansWithoutNGT,
                                      maximumIteration);
      lowerClustering.kmeans(partialVectors, nPartialClusters[idx], lowerClusters[idx]);
      if (nPartialClusters[idx] != lowerClusters[idx].size()) {
        std::cerr << "the sizes of cluster members are not consistent" << std::endl;
        abort();
      }
    }
    size_t nc = 0;
    size_t mc = 0;
    for (auto &cs : lowerClusters) {
      nc += cs.size();
      for (auto &c : cs) {
        mc += c.members.size();
      }
    }
    std::cerr << "# of clusters=" << nc << " # of members=" << mc << std::endl;
  }
  void subclustering(std::vector<NGT::Clustering::Cluster> &upperClusters, size_t numOfLowerClusters,
                     size_t numOfObjects, NGT::ObjectSpace &objectSpace, QBGObjectList &objectList,
                     NGT::Clustering::InitializationMode initMode,
                     std::vector<NGT::Clustering::Cluster> &flatLowerClusters,
                     size_t maximumIteration = 1000) {
    std::vector<std::vector<NGT::Clustering::Cluster>> lowerClusters;
    subclustering(upperClusters, numOfLowerClusters, numOfObjects, objectSpace, objectList, initMode,
                  lowerClusters);

    flattenClusters(upperClusters, lowerClusters, numOfLowerClusters, flatLowerClusters);
  }

#else
  static void subclustering(std::vector<NGT::Clustering::Cluster> &upperClusters, size_t numOfLowerClusters,
                            size_t numOfObjects, NGT::ObjectSpace &objectSpace, QBGObjectList &objectList,
                            NGT::Clustering::InitializationMode initMode,
                            std::vector<std::vector<NGT::Clustering::Cluster>> &lowerClusters,
                            size_t maximumIteration = 1000) {
    NGT::Timer timer;
    timer.start();
    std::vector<uint32_t> nPartialClusters(upperClusters.size());
    int numOfRemainingClusters = numOfLowerClusters;
    int numOfRemainingVectors  = numOfObjects;
    size_t ts                  = 0;
    for (size_t idx = 0; idx < upperClusters.size(); idx++) {
      float ncsf = static_cast<float>(upperClusters[idx].members.size()) / numOfRemainingVectors *
                   (numOfRemainingClusters - (upperClusters.size() - idx));
      ncsf += 1.0;
      int ncs = round(ncsf);
      numOfRemainingVectors -= upperClusters[idx].members.size();
      numOfRemainingClusters -= ncs;
      if (numOfRemainingClusters < 0) {
        std::stringstream msg;
        msg << " subclustering: Internal error! " << numOfRemainingClusters << ":" << idx;
        NGTThrowException(msg);
      }
      nPartialClusters[idx] = ncs;
      ts += ncs;
    }

    std::cerr << "numOfRemainingClusters=" << numOfRemainingClusters << std::endl;
    std::cerr << "numOfRemainingVectors=" << numOfRemainingVectors << std::endl;
    std::cerr << "upperClusters=" << upperClusters.size() << std::endl;
    std::cerr << "total=" << ts << ":" << numOfLowerClusters << std::endl;
    std::cerr << "max iteration=" << maximumIteration << std::endl;
    timer.stop();
    std::cerr << "time=" << timer << std::endl;
    timer.restart();
    if (ts != numOfLowerClusters || numOfRemainingClusters != 0) {
      std::stringstream msg;
      msg << "subclustering: Internal error! " << ts << ":" << numOfLowerClusters << ":"
          << numOfRemainingClusters << std::endl;
      NGTThrowException(msg);
    }

    auto nthreads = omp_get_max_threads();
    if (!objectList.openMultipleStreams(nthreads)) {
      std::stringstream msg;
      msg << "subclustering: Internal error! Cannot open multiple streams. " << nthreads;
      NGTThrowException(msg);
    }

    lowerClusters.resize(upperClusters.size());
    std::vector<size_t> counters(nthreads, 0);
    size_t progressStep = upperClusters.size() / 20;
    ;
    progressStep = progressStep < 20 ? 20 : progressStep;
#pragma omp parallel for schedule(dynamic)
    for (size_t idx = 0; idx < upperClusters.size(); idx++) {
      std::vector<std::vector<float>> partialVectors;
      partialVectors.reserve(upperClusters[idx].members.size());
      std::vector<float> obj;
      auto threadid = omp_get_thread_num();
      {
        for (auto &m : upperClusters[idx].members) {
          if (threadid >= nthreads) {
            std::stringstream msg;
            msg << "subclustering: inner fatal error. # of threads=" << nthreads << ":" << threadid;
            NGTThrowException(msg);
          }
          if (!objectList.get(threadid, m.vectorID + 1, obj, &objectSpace)) {
            std::stringstream msg;
            msg << "subclustering: Fatal error! cannot get!!!! " << m.vectorID + 1;
            NGTThrowException(msg);
          }
          partialVectors.push_back(obj);
        }
      }
      if (upperClusters[idx].members.size() != partialVectors.size()) {
        std::stringstream msg;
        msg << "inner fatal error. the sizes of members are inconsistent. "
            << upperClusters[idx].members.size() << ":" << partialVectors.size() << ":" << idx;
        NGTThrowException(msg);
      }
      NGT::Clustering lowerClustering(initMode, NGT::Clustering::ClusteringTypeKmeansWithoutNGT,
                                      maximumIteration);
      lowerClustering.kmeans(partialVectors, nPartialClusters[idx], lowerClusters[idx]);
      if (nPartialClusters[idx] != lowerClusters[idx].size()) {
        std::cerr << "Warning: the sizes of cluster members are inconsistent. " << nPartialClusters[idx]
                  << ":" << lowerClusters[idx].size() << ":" << idx << std::endl;
      }
      counters[threadid]++;
      {
        size_t cnt = 0;
        for (auto c : counters) {
          cnt += c;
        }
        if (cnt % progressStep == 0) {
          timer.stop();
          float progress = (cnt * 100 / upperClusters.size());
          std::cerr << "subclustering: " << cnt << " clusters (" << progress
                    << "%) have been processed. time=" << timer << std::endl;
          timer.restart();
        }
      }
    }
    size_t nc = 0;
    size_t mc = 0;
    for (auto &cs : lowerClusters) {
      nc += cs.size();
      for (auto &c : cs) {
        mc += c.members.size();
      }
    }
    std::cerr << "# of clusters=" << nc << " # of members=" << mc << std::endl;
  }

  static void subclustering(std::vector<NGT::Clustering::Cluster> &upperClusters, size_t numOfLowerClusters,
                            size_t numOfObjects, NGT::ObjectSpace &objectSpace, QBGObjectList &objectList,
                            NGT::Clustering::InitializationMode initMode,
                            std::vector<NGT::Clustering::Cluster> &flatLowerClusters,
                            size_t maximumIteration = 1000) {
    std::vector<std::vector<NGT::Clustering::Cluster>> lowerClusters;
    subclustering(upperClusters, numOfLowerClusters, numOfObjects, objectSpace, objectList, initMode,
                  lowerClusters);

    flattenClusters(upperClusters, lowerClusters, numOfLowerClusters, flatLowerClusters);
  }

#endif

  static void subclustering(std::vector<NGT::Clustering::Cluster> &upperClusters, size_t numOfLowerClusters,
                            size_t numOfObjects, NGT::Clustering::InitializationMode initMode,
                            std::vector<std::vector<NGT::Clustering::Cluster>> &lowerClusters,
                            std::vector<std::vector<float>> &vectors, size_t maximumIteration = 1000) {
    std::vector<uint32_t> nPartialClusters(upperClusters.size());
    int numOfRemainingClusters = numOfLowerClusters;
    int numOfRemainingVectors  = numOfObjects;
    size_t ts                  = 0;
    for (size_t idx = 0; idx < upperClusters.size(); idx++) {
      float ncsf = static_cast<float>(upperClusters[idx].members.size()) / numOfRemainingVectors *
                   (numOfRemainingClusters - (upperClusters.size() - idx));
      ncsf += 1.0;
      int ncs = round(ncsf);
      numOfRemainingVectors -= upperClusters[idx].members.size();
      numOfRemainingClusters -= ncs;
      if (numOfRemainingClusters < 0) {
        std::stringstream msg;
        msg << "subclustering: Internal error! " << numOfRemainingClusters << idx;
        NGTThrowException(msg);
      }
      nPartialClusters[idx] = ncs;
      ts += ncs;
    }

    std::cerr << "numOfRemainingClusters=" << numOfRemainingClusters << std::endl;
    std::cerr << "numOfRemainingVectors=" << numOfRemainingVectors << std::endl;
    std::cerr << "upperClusters=" << upperClusters.size() << std::endl;
    std::cerr << "total=" << ts << ":" << numOfLowerClusters << std::endl;
    if (ts < numOfLowerClusters || numOfRemainingClusters != 0) {
      std::stringstream msg;
      msg << "subclustering: Internal error! " << ts << ":" << numOfLowerClusters << ":"
          << numOfRemainingClusters << std::endl;
      NGTThrowException(msg);
    }

    auto nthreads = omp_get_max_threads();

    lowerClusters.resize(upperClusters.size());
#pragma omp parallel for schedule(dynamic)
    for (size_t idx = 0; idx < upperClusters.size(); idx++) {
      std::vector<std::vector<float>> partialVectors;
      partialVectors.reserve(upperClusters[idx].members.size());
      auto threadid = omp_get_thread_num();
      {
        for (auto &m : upperClusters[idx].members) {
          if (threadid >= nthreads) {
            std::cerr << "inner fatal error. # of threads=" << nthreads << ":" << threadid << std::endl;
            exit(1);
          }
          std::vector<float> &obj = vectors[m.vectorID];
          partialVectors.push_back(obj);
        }
      }
      if (upperClusters[idx].members.size() != partialVectors.size()) {
        std::stringstream msg;
        msg << "the sizes of members are inconsistent. " << upperClusters[idx].members.size() << ":"
            << partialVectors.size() << ":" << idx;
        NGTThrowException(msg);
      }
      NGT::Clustering lowerClustering(initMode, NGT::Clustering::ClusteringTypeKmeansWithoutNGT,
                                      maximumIteration);
      lowerClustering.kmeans(partialVectors, nPartialClusters[idx], lowerClusters[idx]);
      if (nPartialClusters[idx] != lowerClusters[idx].size()) {
        std::cerr << "Warning: the sizes of cluster members are inconsistent. " << nPartialClusters[idx]
                  << ":" << lowerClusters[idx].size() << ":" << idx << std::endl;
      }
    }
    size_t nc = 0;
    size_t mc = 0;
    for (auto &cs : lowerClusters) {
      nc += cs.size();
      for (auto &c : cs) {
        mc += c.members.size();
      }
    }
    std::cerr << "# of clusters=" << nc << " # of members=" << mc << std::endl;
  }

  static void subclustering(std::vector<NGT::Clustering::Cluster> &upperClusters, size_t numOfLowerClusters,
                            size_t numOfObjects, NGT::Clustering::InitializationMode initMode,
                            std::vector<NGT::Clustering::Cluster> &flatLowerClusters,
                            std::vector<std::vector<float>> &vectors, size_t maximumIteration = 1000) {
    std::vector<std::vector<NGT::Clustering::Cluster>> lowerClusters;
    subclustering(upperClusters, numOfLowerClusters, numOfObjects, initMode, lowerClusters, vectors);

    flattenClusters(upperClusters, lowerClusters, numOfLowerClusters, flatLowerClusters);
  }

  static void assign(std::vector<NGT::Clustering::Cluster> &clusters, size_t beginID, size_t endID,
                     std::vector<std::vector<float>> &vectors) {

    size_t count = 0;
#pragma omp parallel for
    for (size_t id = beginID; id <= endID; id++) {
      std::vector<float> &obj = vectors[id];
      float min               = std::numeric_limits<float>::max();
      int minidx              = -1;
      for (size_t cidx = 0; cidx != clusters.size(); cidx++) {
        auto d = NGT::PrimitiveComparator::compareL2(reinterpret_cast<float *>(obj.data()),
                                                     clusters[cidx].centroid.data(), obj.size());
        if (d < min) {
          min    = d;
          minidx = cidx;
        }
      }
      if (minidx < 0) {
        std::stringstream msg;
        msg << "Fatal assignment error(1)! ID=" << id << " cluster size=" << clusters.size()
            << " object size=" << obj.size();
        NGTThrowException(msg);
      }
#pragma omp critical
      {
        clusters[minidx].members.push_back(NGT::Clustering::Entry(id, minidx, min));
        count++;
        if (count % 1000000 == 0) {
          std::cerr << "# of assigned objects=" << count << std::endl;
        }
      }
    }
  }

  static void assign(std::vector<NGT::Clustering::Cluster> &clusters, size_t beginID, size_t endID,
                     NGT::ObjectSpace &objectSpace, QBGObjectList &objectList) {

#ifdef MULTIPLE_OBJECT_LISTS
    if (!objectList.openMultipleStreams(omp_get_max_threads())) {
      std::cerr << "Cannot open multiple streams." << std::endl;
      abort();
    }
#endif

    size_t count = 0;
#pragma omp parallel for
    for (size_t id = beginID; id <= endID; id++) {
      std::vector<float> obj;
      //#pragma omp critical
#ifdef MULTIPLE_OBJECT_LISTS
      auto stat = objectList.get(omp_get_thread_num(), id, obj, &objectSpace);
#else
      auto stat = objectList.get(id, obj, &objectSpace);
#endif
      if (!stat) {
        std::stringstream msg;
        msg << "Cannot get the object from the object list file. ID=" << id;
        NGTThrowException(msg);
      }
      float min  = std::numeric_limits<float>::max();
      int minidx = -1;
      for (size_t cidx = 0; cidx != clusters.size(); cidx++) {
        auto d = NGT::PrimitiveComparator::compareL2(reinterpret_cast<float *>(obj.data()),
                                                     clusters[cidx].centroid.data(), obj.size());
        if (d < min) {
          min    = d;
          minidx = cidx;
        }
      }
      if (minidx < 0) {
        std::stringstream msg;
        msg << "Fatal assignment error(2)! ID=" << id << " cluster size=" << clusters.size()
            << " object size=" << obj.size();
        NGTThrowException(msg);
      }
#pragma omp critical
      {
        clusters[minidx].members.push_back(NGT::Clustering::Entry(id - 1, minidx, min));
        count++;
        if (count % 1000000 == 0) {
          std::cerr << "# of assigned objects=" << count << std::endl;
        }
      }
    }
  }

  static float optimizeEpsilon(NGT::Index &index, size_t beginID, size_t endID, size_t nOfObjects,
                               QBGObjectList &objectList, float expectedRecall,
                               NGT::ObjectSpace &objectSpace) {
    std::cerr << "optimizeEpsilon: expectedRecall=" << expectedRecall << std::endl;
    NGT::ObjectDistances groundTruth[endID - beginID];
#pragma omp parallel for
    for (size_t id = beginID; id < endID; id++) {
      std::vector<float> obj;
#ifdef MULTIPLE_OBJECT_LISTS
      objectList.get(omp_get_thread_num(), id, obj, &objectSpace);
#else
      objectList.get(id, obj, &objectSpace);
#endif
      NGT::SearchQuery sc(obj);
      sc.setResults(&groundTruth[id - beginID]);
      sc.setSize(nOfObjects);
      index.linearSearch(sc);
    }

    float startEpsilon = 0.02;
    float epsilon;
    std::vector<float> recall(endID - beginID, 0.0);
    for (epsilon = startEpsilon; epsilon < 1.0; epsilon += 0.01) {
      float totalRecall = 0.0;
      NGT::ObjectDistances results[endID - beginID];
#pragma omp parallel for
      for (size_t id = beginID; id < endID; id++) {
        if (recall[id - beginID] == 1.0) {
          continue;
        }
        std::vector<float> obj;
#ifdef MULTIPLE_OBJECT_LISTS
        objectList.get(omp_get_thread_num(), id, obj, &objectSpace);
#else
        objectList.get(id, obj, &objectSpace);
#endif
        NGT::SearchQuery sc(obj);
        sc.setResults(&results[id - beginID]);
        sc.setSize(nOfObjects);
        sc.setEpsilon(epsilon);
        index.search(sc);
      }
      size_t notExactResultCount = 0;
      for (size_t id = beginID; id < endID; id++) {
        if (recall[id - beginID] == 1.0) {
          totalRecall += 1.0;
          continue;
        }
        notExactResultCount++;
        size_t count             = 0;
        NGT::ObjectDistances &gt = groundTruth[id - beginID];
        for (auto &r : results[id - beginID]) {
          if (std::find(gt.begin(), gt.end(), r) != gt.end()) {
            count++;
          }
        }
        recall[id - beginID] = static_cast<float>(count) / static_cast<float>(results[id - beginID].size());
        totalRecall += recall[id - beginID];
      }
      totalRecall /= endID - beginID;
      std::cerr << "Info: # of not exact results=" << notExactResultCount << " start epsilon=" << startEpsilon
                << " current epsilon=" << epsilon << " total recall=" << totalRecall << std::endl;
      if (totalRecall >= expectedRecall) {
        break;
      }
    }
    return epsilon;
  }

  static void assignWithNGT(std::vector<NGT::Clustering::Cluster> &clusters, size_t beginID, size_t endID,
                            NGT::ObjectSpace &objectSpace, QBGObjectList &objectList,
                            size_t epsilonExplorationSize = 100, float expectedRecall = 0.98,
                            size_t noOfNearestNeighbors = 1) {
    if (beginID > endID) {
      std::cerr << "assignWithNGT::Warning. beginID:" << beginID << " > endID:" << endID << std::endl;
      return;
    }

    NGT::Property prop;
    prop.dimension           = objectSpace.getDimension();
    prop.objectType          = NGT::Index::Property::ObjectType::Float;
    prop.distanceType        = NGT::Property::DistanceType::DistanceTypeL2;
    prop.edgeSizeForCreation = 10;
    prop.edgeSizeForSearch   = 40;

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    NGT::Index anngIndex(prop, "dummy");
#else
    NGT::Index anngIndex(prop);
#endif
    for (size_t cidx = 0; cidx < clusters.size(); cidx++) {
      if (cidx % 100000 == 0) {
        std::cerr << "# of appended cluster objects=" << cidx << std::endl;
      }
      anngIndex.append(clusters[cidx].centroid);
    }
    std::cerr << "createIndex..." << std::endl;
    anngIndex.createIndex(500);
#ifdef NGTQ_USING_ONNG
    std::string onng;
    std::string tmpDir;
    {
      NGT::Timer timer;
      timer.start();
      const char *ngtDirString = "/tmp/ngt-XXXXXX";
      char ngtDir[strlen(ngtDirString) + 1];
      strcpy(ngtDir, ngtDirString);
      tmpDir           = mkdtemp(ngtDir);
      std::string anng = tmpDir + "/anng";
      onng             = tmpDir + "/onng";
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
      anngIndex.save(anng);
#endif
      auto unlog = false;
      NGT::GraphOptimizer graphOptimizer(unlog);
      graphOptimizer.searchParameterOptimization   = false;
      graphOptimizer.prefetchParameterOptimization = false;
      graphOptimizer.accuracyTableGeneration       = false;
      int numOfOutgoingEdges                       = 10;
      int numOfIncomingEdges                       = 120;
      int numOfQueries                             = 200;
      int numOfResultantObjects                    = noOfNearestNeighbors + 19;
      graphOptimizer.set(numOfOutgoingEdges, numOfIncomingEdges, numOfQueries, numOfResultantObjects);
      graphOptimizer.execute(anng, onng);
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    NGT::Index onngIndex(onng, "dummy");
#else
    NGT::Index onngIndex(onng);
#endif
    NGT::Index &index = onngIndex;
    const string com  = "rm -rf " + tmpDir;
    if (system(com.c_str()) == -1) {
      std::cerr << "Warning. remove is failed. " << com << std::endl;
    }
#else
    NGT::Index &index = anngIndex;
#endif
    std::cerr << "assign with NGT..." << std::endl;
    endID++;
#ifdef MULTIPLE_OBJECT_LISTS
    if (!objectList.openMultipleStreams(omp_get_max_threads())) {
      std::cerr << "Cannot open multiple streams." << std::endl;
      abort();
    }
#endif
    std::vector<std::pair<size_t, float>> distances(omp_get_max_threads(), std::make_pair(0, 0.0));
    size_t endOfEval  = beginID + epsilonExplorationSize;
    endOfEval         = endOfEval > endID ? endID : endOfEval;
    size_t nOfObjects = noOfNearestNeighbors + 19;
    ;
    NGT::Timer timer;
    timer.start();
    auto epsilon =
        optimizeEpsilon(index, beginID, endOfEval, nOfObjects, objectList, expectedRecall, objectSpace);
    timer.stop();
    std::cerr << "assignWithNGT: exploring epsilon. time=" << timer << " epsilon=" << epsilon << std::endl;
    timer.start();
    size_t progressStep = (endID - beginID) / 20;
    ;
    progressStep = progressStep < 20 ? 20 : progressStep;
    size_t step  = 1000000;
    for (size_t bid = beginID; bid < endID; bid += step) {
      std::vector<std::vector<std::pair<uint32_t, float>>> clusterIDs(step);
      auto eid = std::min(endID, bid + step);
#pragma omp parallel for
      for (size_t id = bid; id < eid; id++) {
        std::vector<float> obj;
#ifdef MULTIPLE_OBJECT_LISTS
        objectList.get(omp_get_thread_num(), id, obj, &objectSpace);
#else
        objectList.get(id, obj, &objectSpace);
#endif
        NGT::SearchQuery sc(obj);
        NGT::ObjectDistances objects;
        sc.setResults(&objects);
        sc.setSize(nOfObjects);
        sc.setEpsilon(epsilon);
        index.search(sc);
        if (clusterIDs[id - bid].capacity() < noOfNearestNeighbors) {
          clusterIDs[id - bid].reserve(noOfNearestNeighbors);
        }
        for (size_t i = 0; objects.size(); i++) {
          if (clusterIDs[id - bid].size() == noOfNearestNeighbors) {
            break;
          }
          clusterIDs[id - bid].emplace_back(make_pair(objects[i].id - 1, objects[0].distance));
        }
        auto threadID = omp_get_thread_num();
        distances[threadID].first++;
        distances[threadID].second += objects[0].distance;
        {
          size_t cnt = 0;
          for (auto d : distances) {
            cnt += d.first;
          }
          if (cnt % progressStep == 0) {
            timer.stop();
            float progress = cnt * 100 / (endID - beginID);
            std::cerr << "assignWithNGT: " << cnt << " objects (" << progress
                      << "%) have been assigned. time=" << timer << std::endl;
            timer.restart();
          }
        }
      }
      std::cerr << "pushing..." << std::endl;
      for (size_t id = bid; id < eid; id++) {
        for (auto &e : clusterIDs[id - bid]) {
          auto cid       = e.first;
          auto cdistance = e.second;
          clusters[cid].members.emplace_back(NGT::Clustering::Entry(id - 1, cid, cdistance));
        }
      }
    }
    {
      size_t n  = 0;
      float err = 0;
      for (auto t : distances) {
        n += t.first;
        err += t.second;
      }
      if (n != 0) {
        err /= static_cast<float>(n);
        std::cerr << "assign. quantization error=" << err << "/" << n << std::endl;
      } else {
        std::cerr << "assign. no assigned vectors." << std::endl;
      }
    }
  }

#ifdef NGTQ_QBG
  void treeBasedTopdownClustering(std::string prefix, QBG::Index &index, uint32_t rootID,
                                  std::vector<float> &object, std::vector<HKNode *> &nodes,
                                  NGT::Clustering &clustering);

  static void twoLayerClustering(
      std::vector<std::vector<float>> vectors, size_t numOfClusters,
      std::vector<NGT::Clustering::Cluster> &clusters, size_t numOfObjectsForEachFirstCluster = 0,
      size_t numOfObjectsForEachSecondCluster = 0, size_t maximumIteration = 300,
      NGT::Clustering::InitializationMode initMode = NGT::Clustering::InitializationModeKmeansPlusPlus) {
    std::cerr << "clustering with two layers... # of clusters=" << numOfClusters << std::endl;
    if (numOfClusters == 0) {
      NGTThrowException("HierachicalKmeans:: # of clusters is zero.");
    }
    size_t numOfFirstClusters  = sqrt(numOfClusters);
    size_t numOfFirstObjects   = sqrt(vectors.size());
    numOfFirstObjects          = numOfObjectsForEachFirstCluster == 0
                                     ? numOfFirstObjects
                                     : numOfFirstClusters * numOfObjectsForEachFirstCluster;
    size_t numOfSecondClusters = numOfClusters;
    size_t numOfSecondObjects  = vectors.size();
    numOfSecondObjects         = numOfObjectsForEachSecondCluster == 0
                                     ? numOfSecondObjects
                                     : numOfSecondClusters * numOfObjectsForEachSecondCluster;
    size_t numOfObjects        = vectors.size();
    if (numOfSecondObjects > numOfObjects) {
      numOfSecondObjects = numOfObjects;
    }

    NGT::Clustering firstClustering(initMode, NGT::Clustering::ClusteringTypeKmeansWithoutNGT,
                                    maximumIteration);
    firstClustering.setClusterSizeConstraintCoefficient(false);
    std::vector<NGT::Clustering::Cluster> firstClusters;
    NGT::Timer timer;
    {
      std::vector<std::vector<float>> firstVectors;
      firstVectors.reserve(numOfFirstObjects);
      for (size_t c = 0; c < numOfFirstObjects; c++) {
        firstVectors.push_back(vectors[c]);
      }
      std::cerr << "clustering for the first...." << std::endl;
      timer.start();
      firstClustering.kmeans(firstVectors, numOfFirstClusters, firstClusters);
      timer.stop();
      std::cerr << "clustering for the first. # of clusters=" << firstClusters.size() << " time=" << timer
                << std::endl;
    }

    timer.start();
    std::cerr << "assign for the second. (" << numOfFirstObjects << "-" << numOfSecondObjects << ")..."
              << std::endl;
    assign(firstClusters, numOfFirstObjects, numOfSecondObjects - 1, vectors);
    timer.stop();
    std::cerr << "assign for the second. time=" << timer << std::endl;

    std::cerr << "subclustering for the second..." << std::endl;
    std::vector<NGT::Clustering::Cluster> &secondClusters = clusters;
    timer.start();
    subclustering(firstClusters, numOfSecondClusters, numOfSecondObjects, initMode, secondClusters, vectors,
                  maximumIteration);
    timer.stop();
    std::cerr << "subclustering for the second. time=" << timer << std::endl;
  }

  void threeLayerClustering(std::string prefix, QBG::Index &index);

  void twoPlusLayerClustering(std::string prefix, QBG::Index &index);

  void multiLayerClustering(QBG::Index &index, std::string prefix, std::string objectIDsFile);

  void clustering(std::string indexPath, std::string prefix = "", std::string objectIDsFile = "");
#endif

  void assignAll(std::string indexPath, int64_t lowerBoundOfNoOfObjects, size_t noOfNearestNeighbors = 1);

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
  ClusteringType clusteringType;
  size_t epsilonExplorationSize;
  float expectedRecall;
  bool verbose;
};
} // namespace QBG
