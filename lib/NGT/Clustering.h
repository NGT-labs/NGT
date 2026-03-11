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

#include "NGT/Index.h"

using namespace std;

#if defined(NGT_AVX_DISABLED)
#define NGT_CLUSTER_NO_AVX
#else
#if defined(__AVX2__)
#define NGT_CLUSTER_AVX2
#else
#define NGT_CLUSTER_NO_AVX
#endif
#endif

#if defined(NGT_CLUSTER_NO_AVX)
#warning "*** SIMD is *NOT* available! ***"
#else
#include <immintrin.h>
#endif

#include <omp.h>
#include <random>

namespace NGT {

class Clustering {
 public:
  enum InitializationMode {
    InitializationModeHead                    = 0,
    InitializationModeRandom                  = 1,
    InitializationModeKmeansPlusPlus          = 2,
    InitializationModeRandomFixedSeed         = 3,
    InitializationModeKmeansPlusPlusFixedSeed = 4,
    InitializationModeBest                    = 5
  };

  enum ClusteringType {
    ClusteringTypeKmeansWithNGT             = 0,
    ClusteringTypeKmeansWithoutNGT          = 1,
    ClusteringTypeKmeansWithIteration       = 2,
    ClusteringTypeKmeansWithNGTForCentroids = 3
  };

  class Entry {
   public:
    Entry() : vectorID(0), centroidID(0), distance(0.0) {}
    Entry(size_t vid, size_t cid, double d) : vectorID(vid), centroidID(cid), distance(d) {}
    bool operator<(const Entry &e) const { return distance > e.distance; }
    uint32_t vectorID;
    uint32_t centroidID;
    double distance;
  };

  class DescendingEntry {
   public:
    DescendingEntry(size_t vid, double d) : vectorID(vid), distance(d) {}
    bool operator<(const DescendingEntry &e) const { return distance < e.distance; }
    size_t vectorID;
    double distance;
  };

  class Cluster {
   public:
    Cluster() : radius(0.0) {}
    Cluster(std::vector<float> &c) : centroid(c), radius(0.0) {}
    Cluster(const Cluster &c) { *this = c; }
    Cluster &operator=(const Cluster &c) {
      members  = c.members;
      centroid = c.centroid;
      radius   = c.radius;
      return *this;
    }

    std::vector<Entry> members;
    std::vector<float> centroid;
    double radius;
  };

  Clustering(InitializationMode im = InitializationModeHead, ClusteringType ct = ClusteringTypeKmeansWithNGT,
             size_t mi = 10000, size_t nc = 0, bool s = true)
      : clusteringType(ct), initializationMode(im), numberOfClusters(nc), maximumIteration(mi), silence(s) {
    initialize();
  }

  void initialize() {
    epsilonFrom                      = 0.12;
    epsilonTo                        = epsilonFrom;
    epsilonStep                      = 0.04;
    resultSizeCoefficient            = 5;
    clusterSizeConstraint            = false;
    clusterSizeConstraintCoefficient = 0.0;
  }

  static void convert(std::vector<std::string> &strings, std::vector<float> &vector) {
    vector.clear();
    for (auto it = strings.begin(); it != strings.end(); ++it) {
      vector.push_back(stod(*it));
    }
  }

  static void extractVector(const std::string &str, std::vector<float> &vec) {
    std::vector<std::string> tokens;
    NGT::Common::tokenize(str, tokens, " \t");
    convert(tokens, vec);
  }

  static void loadVectors(const std::string &file, std::vector<std::vector<float>> &vectors) {
    std::ifstream is(file);
    if (!is) {
      throw std::runtime_error("loadVectors::Cannot open " + file);
    }
    std::string line;
    size_t prevdim = 0;
    while (getline(is, line)) {
      std::vector<float> v;
      extractVector(line, v);
      if (v.size() == 0) {
        std::stringstream msg;
        msg << "Clustering:loadVectors: Error! The dimensionality is zero." << std::endl;
        NGTThrowException(msg);
      }
      if (prevdim != 0 && prevdim != v.size()) {
        std::stringstream msg;
        msg << "Clustering:loadVectors: Error! The dimensionality is inconsist. " << prevdim << ":"
            << v.size() << std::endl;
        NGTThrowException(msg);
      }
      vectors.push_back(v);
      prevdim = v.size();
    }
  }

  static void saveVectors(const std::string &file, std::vector<std::vector<float>> &vectors) {
    std::ofstream os(file);
    for (auto vit = vectors.begin(); vit != vectors.end(); ++vit) {
      std::vector<float> &v = *vit;
      for (auto it = v.begin(); it != v.end(); ++it) {
        os << std::setprecision(9) << (*it);
        if (it + 1 != v.end()) {
          os << "\t";
        }
      }
      os << std::endl;
    }
  }

  static void saveVectors(const std::string &file, std::vector<std::vector<uint32_t>> &vectors) {
    std::ofstream os(file);
    for (auto vit = vectors.begin(); vit != vectors.end(); ++vit) {
      std::vector<uint32_t> &v = *vit;
      for (auto it = v.begin(); it != v.end(); ++it) {
        os << (*it);
        if (it + 1 != v.end()) {
          os << "\t";
        }
      }
      os << std::endl;
    }
  }

  static void loadVector(const std::string &file, std::vector<size_t> &vectors) {
    std::ifstream is(file);
    if (!is) {
      throw std::runtime_error("loadVector::Cannot open " + file);
    }
    std::string line;
    while (true) {
      size_t v;
      is >> v;
      if (is.eof()) break;
      vectors.push_back(v);
    }
  }

  template <typename T> static void saveVector(const std::string &file, std::vector<T> &vectors) {
    std::ofstream os(file);
    for (auto vit = vectors.begin(); vit != vectors.end(); ++vit) {
      os << *vit << std::endl;
    }
  }

  static void loadClusters(const std::string &file, std::vector<Cluster> &clusters,
                           size_t numberOfClusters = 0) {
    std::ifstream is(file);
    if (!is) {
      throw std::runtime_error("loadClusters::Cannot open " + file);
    }
    std::string line;
    while (getline(is, line)) {
      std::vector<float> v;
      extractVector(line, v);
      clusters.push_back(v);
      if ((numberOfClusters != 0) && (clusters.size() >= numberOfClusters)) {
        break;
      }
    }
    if ((numberOfClusters != 0) && (clusters.size() < numberOfClusters)) {
      std::stringstream msg;
      msg << "initial cluster data are not enough. " << clusters.size() << ":" << numberOfClusters;
      NGTThrowException(msg);
    }
  }
#if !defined(NGT_CLUSTER_NO_AVX)
  static double sumOfSquares(float *a, float *b, size_t size) {
    __m256 sum       = _mm256_setzero_ps();
    float *last      = a + size;
    float *lastgroup = last - 7;
    while (a < lastgroup) {
      __m256 v = _mm256_sub_ps(_mm256_loadu_ps(a), _mm256_loadu_ps(b));
      sum      = _mm256_add_ps(sum, _mm256_mul_ps(v, v));
      a += 8;
      b += 8;
    }
    __attribute__((aligned(32))) float f[8];
    _mm256_store_ps(f, sum);
    double s = f[0] + f[1] + f[2] + f[3] + f[4] + f[5] + f[6] + f[7];
    while (a < last) {
      double d = *a++ - *b++;
      s += d * d;
    }
    return s;
  }
#else  // !defined(NGT_AVX_DISABLED) && defined(__AVX__)
  static double sumOfSquares(float *a, float *b, size_t size) {
    double csum = 0.0;
    float *x    = a;
    float *y    = b;
    for (size_t i = 0; i < size; i++) {
      double d = (double)*x++ - (double)*y++;
      csum += d * d;
    }
    return csum;
  }
#endif // !defined(NGT_AVX_DISABLED) && defined(__AVX__)

  static void clearMembers(std::vector<Cluster> &clusters) {
    for (auto &cluster : clusters) {
      cluster.members.clear();
    }
  }

  static size_t removeEmptyClusters(std::vector<Cluster> &clusters) {
    size_t count = 0;
    auto dst     = clusters.begin();
    for (auto src = clusters.begin(); src != clusters.end(); ++src) {
      if ((*src).members.size() == 0) {
        count++;
        continue;
      }
      if (dst != src) {
        *dst = std::move(*src);
      }
      ++dst;
    }
    if (count != 0) {
      clusters.resize(clusters.size() - count);
    }
    return count;
  }

  static double distanceL2(std::vector<float> &vector1, std::vector<float> &vector2) {
    return sqrt(sumOfSquares(&vector1[0], &vector2[0], vector1.size()));
  }

  static double distanceL2(std::vector<std::vector<float>> &vector1,
                           std::vector<std::vector<float>> &vector2) {
    assert(vector1.size() == vector2.size());
    double distance = 0.0;
    for (size_t i = 0; i < vector1.size(); i++) {
      distance += distanceL2(vector1[i], vector2[i]);
    }
    distance /= (double)vector1.size();
    return distance;
  }

  static double meanSumOfSquares(std::vector<float> &vector1, std::vector<float> &vector2) {
    return sumOfSquares(&vector1[0], &vector2[0], vector1.size()) / (double)vector1.size();
  }

  static void subtract(std::vector<float> &a, std::vector<float> &b) {
    if (a.size() != b.size()) {
      std::stringstream msg;
      std::cerr << "Clustering::subtract: Mismatched dimensions. " << a.size() << "x" << b.size();
      NGTThrowException(msg);
    }
    auto bit = b.begin();
    for (auto ait = a.begin(); ait != a.end(); ++ait, ++bit) {
      *ait = *ait - *bit;
    }
  }

  static void getInitialCentroidsFromHead(std::vector<std::vector<float>> &vectors,
                                          std::vector<Cluster> &clusters, size_t size) {
    size = size > vectors.size() ? vectors.size() : size;
    clusters.clear();
    for (size_t i = 0; i < size; i++) {
      clusters.push_back(Cluster(vectors[i]));
    }
  }

  static void getInitialCentroidsRandomly(std::vector<std::vector<float>> &vectors,
                                          std::vector<Cluster> &clusters, size_t size, size_t seed = 0) {
    size = size > vectors.size() ? vectors.size() : size;
    clusters.clear();
    if (seed == 0) {
      std::random_device rnd;
      seed = rnd();
    }
    std::mt19937 mt(seed);

    std::uniform_int_distribution<> dist(0, vectors.size() - 1);
    for (size_t i = 0; i < size; i++) {
      size_t idx = dist(mt);
      clusters.push_back(Cluster(vectors[idx]));
    }
    assert(clusters.size() == size);
  }

  static void getInitialCentroidsKmeansPlusPlus(std::vector<std::vector<float>> &vectors,
                                                std::vector<Cluster> &clusters, size_t size,
                                                size_t seed = 0) {
    size = size > vectors.size() ? vectors.size() : size;
    clusters.clear();
    if (seed == 0) {
      std::random_device rnd;
      seed = rnd();
    }
    std::mt19937 mt(seed);
    std::uniform_int_distribution<> dist(0, vectors.size() - 1);
    size_t idx = dist(mt);
    clusters.push_back(Cluster(vectors[idx]));

    NGT::Timer timer;
    for (size_t k = 1; k < size; k++) {
      double sum = 0;
      std::priority_queue<DescendingEntry> sortedObjects;
      // get d^2 and sort
#pragma omp parallel for
      for (size_t vi = 0; vi < vectors.size(); vi++) {
        auto vit    = vectors.begin() + vi;
        double mind = DBL_MAX;
        for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
          double d = distanceL2(*vit, (*cit).centroid);
          d *= d;
          if (d < mind) {
            mind = d;
          }
        }
#pragma omp critical
        {
          sortedObjects.push(DescendingEntry(distance(vectors.begin(), vit), mind));
          sum += mind;
        }
      }
      double l = (double)mt() / (double)mt.max() * sum;
      while (!sortedObjects.empty()) {
        sum -= sortedObjects.top().distance;
        if (l >= sum) {
          clusters.push_back(Cluster(vectors[sortedObjects.top().vectorID]));
          break;
        }
        sortedObjects.pop();
      }
    }
  }

  static void assign(std::vector<std::vector<float>> &vectors, std::vector<Cluster> &clusters,
                     size_t clusterSize = std::numeric_limits<size_t>::max(), bool clear = true) {
    // compute distances to the nearest clusters, and construct heap by the distances.
    NGT::Timer timer;
    timer.start();

    size_t nOfVectors = 0;
    if (!clear) {
      for (auto &cluster : clusters) {
        nOfVectors += cluster.members.size();
      }
    }
    std::vector<Entry> sortedObjects(vectors.size());
#pragma omp parallel for
    for (size_t vi = 0; vi < vectors.size(); vi++) {
      auto vit = vectors.begin() + vi;
      {
        double mind = DBL_MAX;
        int mincidx = -1;
        for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
          double d = distanceL2(*vit, (*cit).centroid);
          if (d < mind) {
            mind    = d;
            mincidx = distance(clusters.begin(), cit);
          }
        }
        if (mincidx == -1) {
          std::cerr << "Clustering: Fatal error " << clusters.size() << std::endl;
          std::cerr << vi << "/" << vectors.size() << std::endl;
          abort();
        }
        sortedObjects[vi] = Entry(vi + nOfVectors, mincidx, mind);
      }
    }
    std::sort(sortedObjects.begin(), sortedObjects.end());

    // clear
    if (clear) {
      for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
        (*cit).members.clear();
      }
    }
    // distribute objects to the nearest clusters in the same size constraint.
    for (auto soi = sortedObjects.rbegin(); soi != sortedObjects.rend();) {
      Entry &entry = *soi;
      if (entry.centroidID >= clusters.size()) {
        std::cerr << "Something wrong. (2) " << entry.centroidID << ":" << clusters.size() << std::endl;
        soi++;
        continue;
      }
      if (clusters[entry.centroidID].members.size() < clusterSize) {
        clusters[entry.centroidID].members.push_back(entry);
        soi++;
      } else {
        std::vector<float> ds(clusters.size());
#pragma omp parallel for
        for (size_t idx = 0; idx < clusters.size(); idx++) {
          if (clusters[idx].members.size() >= clusterSize) {
            ds[idx] = std::numeric_limits<float>::max();
            continue;
          }
          ds[idx] = distanceL2(vectors[entry.vectorID], clusters[idx].centroid);
        }
        float mind     = std::numeric_limits<float>::max();
        size_t mincidx = -1;
        for (size_t idx = 0; idx < clusters.size(); idx++) {
          if (ds[idx] < mind) {
            mind    = ds[idx];
            mincidx = idx;
          }
        }
        entry  = Entry(entry.vectorID, mincidx, mind);
        int pt = distance(sortedObjects.rbegin(), soi);
        std::sort(sortedObjects.begin(), soi.base());
        soi = sortedObjects.rbegin() + pt;
        assert(pt == distance(sortedObjects.rbegin(), soi));
      }
    }

    moveFartherObjectsToEmptyClusters(clusters);
  }

  static void moveFartherObjectsToEmptyClusters(std::vector<Cluster> &clusters) {
    size_t emptyClusterCount = 0;
    for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
      if ((*cit).members.size() == 0) {
        emptyClusterCount++;
        double max = -DBL_MAX;
        auto maxit = clusters.begin();
        for (auto scit = clusters.begin(); scit != clusters.end(); ++scit) {
          if ((*scit).members.size() >= 2 && (*scit).members.back().distance > max) {
            maxit = scit;
            max   = (*scit).members.back().distance;
          }
        }
        if (max == -DBL_MAX) {
          std::stringstream msg;
          msg << "Clustering::moveFartherObjectsToEmptyClusters: Not found max. ";
          for (auto scit = clusters.begin(); scit != clusters.end(); ++scit) {
            msg << distance(clusters.begin(), scit) << ":" << (*scit).members.size() << " ";
          }
          NGTThrowException(msg);
        }
        (*cit).members.push_back((*maxit).members.back());
        (*cit).members.back().centroidID = distance(clusters.begin(), cit);
        (*maxit).members.pop_back();
      }
    }
    emptyClusterCount = 0;
    for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
      if ((*cit).members.size() == 0) {
        emptyClusterCount++;
      }
    }
  }

  static void assignWithNGT(NGT::Index &index, std::vector<std::vector<float>> &vectors,
                            std::vector<Cluster> &clusters, size_t &resultSize, float epsilon = 0.12,
                            size_t clusterSize = std::numeric_limits<size_t>::max()) {
    size_t dataSize = vectors.size();
    assert(index.getObjectRepositorySize() - 1 == vectors.size());
    vector<vector<Entry>> results(clusters.size());
#pragma omp parallel for
    for (size_t ci = 0; ci < clusters.size(); ci++) {
      auto cit = clusters.begin() + ci;
      NGT::ObjectDistances objects;
      NGT::Object *query = 0;
      query              = index.allocateObject((*cit).centroid);
      NGT::SearchContainer sc(*query);
      sc.setResults(&objects);
      sc.setEpsilon(epsilon);
      sc.setSize(resultSize);
      index.search(sc);
      results[ci].reserve(objects.size());
      for (size_t idx = 0; idx < objects.size(); idx++) {
        size_t oidx = objects[idx].id - 1;
        results[ci].push_back(Entry(oidx, ci, objects[idx].distance));
      }
      index.deleteObject(query);
    }
    size_t resultCount = 0;
    for (auto ri = results.begin(); ri != results.end(); ++ri) {
      resultCount += (*ri).size();
    }
    vector<Entry> sortedDistances;
    sortedDistances.reserve(resultCount);
    for (auto ri = results.begin(); ri != results.end(); ++ri) {
      std::copy((*ri).begin(), (*ri).end(), std::back_inserter(sortedDistances));
    }

    vector<bool> assignedObjects(dataSize, false);

    sort(sortedDistances.begin(), sortedDistances.end());

    for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
      (*cit).members.clear();
    }

    size_t assignedObjectCount = 0;
    for (auto i = sortedDistances.rbegin(); i != sortedDistances.rend(); ++i) {
      size_t objectID  = (*i).vectorID;
      size_t clusterID = (*i).centroidID;
      if (clusters[clusterID].members.size() >= clusterSize) {
        continue;
      }
      if (!assignedObjects[objectID]) {
        assignedObjects[objectID] = true;
        clusters[clusterID].members.push_back(*i);
        clusters[clusterID].members.back().centroidID = clusterID;
        assignedObjectCount++;
      }
    }
    vector<uint32_t> notAssignedObjectIDs;
    notAssignedObjectIDs.reserve(dataSize - assignedObjectCount);
    for (size_t idx = 0; idx < dataSize; idx++) {
      if (!assignedObjects[idx]) {
        notAssignedObjectIDs.push_back(idx);
      }
    }

    if (clusterSize < std::numeric_limits<size_t>::max()) {
      do {
        vector<vector<Entry>> notAssignedObjects(notAssignedObjectIDs.size());
        size_t nOfClosestClusters = 1 * 1024 * 1024 * 1024 / 16 /
                                    (notAssignedObjectIDs.size() == 0 ? 1 : notAssignedObjectIDs.size());
#pragma omp parallel for
        for (size_t vi = 0; vi < notAssignedObjectIDs.size(); vi++) {
          auto vit = notAssignedObjectIDs.begin() + vi;
          if (assignedObjects[*vit]) {
            continue;
          }
          vector<Entry> ds;
          ds.reserve(clusters.size());
          for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
            if ((*cit).members.size() >= clusterSize) {
              continue;
            }
            double d = distanceL2(vectors[*vit], (*cit).centroid);
            ds.push_back(Entry(*vit, distance(clusters.begin(), cit), d));
          }
          sort(ds.begin(), ds.end());
          size_t topk = ds.size() < nOfClosestClusters ? ds.size() : nOfClosestClusters;
          std::copy(ds.end() - topk, ds.end(), std::back_inserter(notAssignedObjects[vi]));
        }
        sortedDistances.clear();
        for (auto i = notAssignedObjects.begin(); i != notAssignedObjects.end(); ++i) {
          std::copy((*i).begin(), (*i).end(), std::back_inserter(sortedDistances));
          vector<Entry> empty;
          (*i).swap(empty);
        }
        sort(sortedDistances.begin(), sortedDistances.end());

        for (auto i = sortedDistances.rbegin(); i != sortedDistances.rend(); ++i) {
          size_t objectID  = (*i).vectorID;
          size_t clusterID = (*i).centroidID;
          if (clusters[clusterID].members.size() >= clusterSize) {
            continue;
          }
          if (!assignedObjects[objectID]) {
            assignedObjects[objectID] = true;
            clusters[clusterID].members.push_back(*i);
            clusters[clusterID].members.back().centroidID = clusterID;
          }
        }
      } while (std::any_of(assignedObjects.begin(), assignedObjects.end(), [](bool x) { return !x; }));
    } else {
      vector<Entry> notAssignedObjects(notAssignedObjectIDs.size());
#pragma omp parallel for
      for (size_t vi = 0; vi < notAssignedObjectIDs.size(); vi++) {
        auto vit = notAssignedObjectIDs.begin() + vi;
        {
          double mind    = DBL_MAX;
          size_t mincidx = -1;
          for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
            double d = distanceL2(vectors[*vit], (*cit).centroid);
            if (d < mind) {
              mind    = d;
              mincidx = distance(clusters.begin(), cit);
            }
          }
          notAssignedObjects[vi] = Entry(*vit, mincidx, mind); // Entry(vectorID, centroidID, distance)
        }
      }
      for (auto nroit = notAssignedObjects.begin(); nroit != notAssignedObjects.end(); ++nroit) {
        clusters[(*nroit).centroidID].members.push_back(*nroit);
      }
      moveFartherObjectsToEmptyClusters(clusters);
    }
  }

  static double calculateCentroid(std::vector<std::vector<float>> &vectors, std::vector<Cluster> &clusters) {
    double distance    = 0;
    size_t memberCount = 0;
    for (auto it = clusters.begin(); it != clusters.end(); ++it) {
      memberCount += (*it).members.size();
      if ((*it).members.size() != 0) {
        std::vector<float> mean(vectors[0].size(), 0.0);
        for (auto memit = (*it).members.begin(); memit != (*it).members.end(); ++memit) {
          auto mit = mean.begin();
          auto &v  = vectors[(*memit).vectorID];
          for (auto vit = v.begin(); vit != v.end(); ++vit, ++mit) {
            *mit += *vit;
          }
        }
        for (auto mit = mean.begin(); mit != mean.end(); ++mit) {
          *mit /= (*it).members.size();
        }
        distance += distanceL2((*it).centroid, mean);
        (*it).centroid = mean;
      } else {
        cerr << "Clustering: Fatal Error. No member!" << endl;
        abort();
      }
    }
    return distance;
  }

  static void saveClusters(const std::string &file, std::vector<Cluster> &clusters,
                           bool skipEmptyClusters = false) {
    std::ofstream os(file);
    for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
      if (skipEmptyClusters && (*cit).members.size() == 0) {
        continue;
      }
      std::vector<float> &v = (*cit).centroid;
      for (auto it = v.begin(); it != v.end(); ++it) {
        os << std::setprecision(9) << (*it);
        if (it + 1 != v.end()) {
          os << "\t";
        }
      }
      os << std::endl;
    }
  }

  double kmeansWithoutNGT(std::vector<std::vector<float>> &vectors, std::vector<Cluster> &clusters,
                          size_t clusterSize) {
    NGT::Timer timer;
    timer.start();
    double diff           = std::numeric_limits<double>::max();
    size_t stabilityLimit = 2;
    size_t stabilityCount = 0;
    size_t i;
    for (i = 0; i < maximumIteration; i++) {
      assign(vectors, clusters, clusterSize);
      // centroid is recomputed.
      // diff is distance between the current centroids and the previous centroids.
      auto d = calculateCentroid(vectors, clusters);
      if (d == diff) {
        stabilityCount++;
        if (stabilityCount >= stabilityLimit) {
          break;
        }
      }
      if (d < diff) {
        diff = d;
      }
      if (diff == 0.0) {
        break;
      }
    }
    return diff;
  }
  double kmeansWithoutNGT(std::vector<std::vector<float>> &vectors, size_t numberOfClusters,
                          std::vector<Cluster> &clusters) {
    size_t clusterSize = std::numeric_limits<size_t>::max();

    double diff = kmeansWithoutNGT(vectors, clusters, clusterSize);

    if (clusterSizeConstraint || clusterSizeConstraintCoefficient != 0.0) {
      if (clusterSizeConstraintCoefficient >= 1.0) {
        clusterSize =
            ceil((double)vectors.size() / (double)numberOfClusters) * clusterSizeConstraintCoefficient;
      } else if (clusterSizeConstraintCoefficient != 0.0) {
        std::stringstream msg;
        msg << "kmeansWithoutNGT: clusterSizeConstraintCoefficient is invalid. "
            << clusterSizeConstraintCoefficient << " ";
        throw std::runtime_error(msg.str());
      } else {
        clusterSize = ceil((double)vectors.size() / (double)numberOfClusters);
      }
    } else {
      return diff == 0;
    }

    diff = kmeansWithoutNGT(vectors, clusters, clusterSize);

    return diff == 0;
  }

  double kmeansWithNGT(NGT::Index &index, std::vector<std::vector<float>> &vectors, size_t numberOfClusters,
                       std::vector<Cluster> &clusters, float epsilon) {
    size_t clusterSize = std::numeric_limits<size_t>::max();
    if (clusterSizeConstraint) {
      clusterSize = ceil((double)vectors.size() / (double)numberOfClusters);
      for (size_t ci = 0; ci < clusters.size(); ci++) {
        clusters[ci].members.reserve(clusterSize);
      }
    }

    diffHistory.clear();
    NGT::Timer timer;
    timer.start();
    double diff = 0.0;
    size_t resultSize;
    resultSize = resultSizeCoefficient * vectors.size() / clusters.size();
    size_t i;
    for (i = 0; i < maximumIteration; i++) {
      assignWithNGT(index, vectors, clusters, resultSize, epsilon, clusterSize);
      // centroid is recomputed.
      // diff is distance between the current centroids and the previous centroids.
      double prevDiff                   = diff;
      std::vector<Cluster> prevClusters = clusters;
      diff                              = calculateCentroid(vectors, clusters);
      if (prevDiff == diff) {
        std::cerr << "epsilon=" << epsilon << "->" << epsilon * 1.1 << std::endl;
        epsilon *= 1.1;
      }
      diffHistory.push_back(diff);

      if (diff == 0) {
        break;
      }
    }
    return diff;
  }

#ifndef NGT_SHARED_MEMORY_ALLOCATOR
  double kmeansWithNGT(std::vector<std::vector<float>> &vectors, size_t numberOfClusters,
                       std::vector<Cluster> &clusters) {
    pid_t pid = getpid();
    std::stringstream str;
    str << "cluster-ngt." << pid;
    string database = str.str();
    string dataFile;
    size_t dataSize = 0;
    size_t dim      = clusters.front().centroid.size();
    NGT::Property property;
    property.dimension    = dim;
    property.graphType    = NGT::Property::GraphType::GraphTypeANNG;
    property.objectType   = NGT::Index::Property::ObjectType::Float;
    property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeL2;

    float *data = new float[vectors.size() * dim];
    float *ptr  = data;
    dataSize    = vectors.size();
    for (auto vi = vectors.begin(); vi != vectors.end(); ++vi) {
      memcpy(ptr, &((*vi)[0]), dim * sizeof(float));
      ptr += dim;
    }
    size_t threadSize = 20;

    NGT::Index index(property);
    index.append(data, dataSize);
    index.createIndex(threadSize);

    return kmeansWithNGT(index, vectors, numberOfClusters, clusters, epsilonFrom);
  }
#endif

  double kmeansWithNGT(NGT::Index &index, size_t numberOfClusters, std::vector<Cluster> &clusters) {
    NGT::GraphIndex &graph = static_cast<NGT::GraphIndex &>(index.getIndex());
    NGT::ObjectSpace &os   = graph.getObjectSpace();
    size_t size            = os.getRepository().size();
    std::vector<std::vector<float>> vectors(size - 1);
    for (size_t idx = 1; idx < size; idx++) {
      try {
        os.getObject(idx, vectors[idx - 1]);
      } catch (...) {
        cerr << "Cannot get object " << idx << endl;
      }
    }
    double diff = DBL_MAX;
    clusters.clear();
    setupInitialClusters(vectors, numberOfClusters, clusters);
    for (float epsilon = epsilonFrom; epsilon <= epsilonTo; epsilon += epsilonStep) {
      diff = kmeansWithNGT(index, vectors, numberOfClusters, clusters, epsilon);
      if (diff == 0.0) {
        return diff;
      }
    }
    return diff;
  }

  double kmeansWithNGT(NGT::Index &index, size_t numberOfClusters, NGT::Index &outIndex) {
    std::vector<Cluster> clusters;
    double diff = kmeansWithNGT(index, numberOfClusters, clusters);
    for (auto i = clusters.begin(); i != clusters.end(); ++i) {
      outIndex.insert((*i).centroid);
    }
    outIndex.createIndex(16);
    return diff;
  }

  double kmeansWithNGT(NGT::Index &index, size_t numberOfClusters) {
    NGT::Property prop;
    index.getProperty(prop);
    string path = index.getPath();
    index.save();
    index.close();
    string outIndexName = path;
    string inIndexName  = path + ".tmp";
    std::rename(outIndexName.c_str(), inIndexName.c_str());
    NGT::Index::createGraphAndTree(outIndexName, prop);
    index.open(outIndexName);
    NGT::Index inIndex(inIndexName);
    double diff = kmeansWithNGT(inIndex, numberOfClusters, index);
    inIndex.close();
    NGT::Index::destroy(inIndexName);
    return diff;
  }

  double kmeansWithNGT(string &indexName, size_t numberOfClusters) {
    NGT::Index inIndex(indexName);
    double diff = kmeansWithNGT(inIndex, numberOfClusters);
    inIndex.save();
    inIndex.close();
    return diff;
  }

  static double calculateMSE(std::vector<std::vector<float>> &vectors, std::vector<Cluster> &clusters) {
    double mse   = 0.0;
    size_t count = 0;
    for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
      count += (*cit).members.size();
      for (auto mit = (*cit).members.begin(); mit != (*cit).members.end(); ++mit) {
        mse += meanSumOfSquares((*cit).centroid, vectors[(*mit).vectorID]);
      }
    }
    assert(vectors.size() == count);
    return mse / (double)vectors.size();
  }

  static double calculateML2(std::vector<std::vector<float>> &vectors, std::vector<Cluster> &clusters) {
    double d     = 0.0;
    size_t count = 0;
    for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
      count += (*cit).members.size();
      double localD = 0.0;
      for (auto mit = (*cit).members.begin(); mit != (*cit).members.end(); ++mit) {
        double distance = distanceL2((*cit).centroid, vectors[(*mit).vectorID]);
        d += distance;
        localD += distance;
      }
    }
    if (vectors.size() != count) {
      std::cerr << "Warning! vectors.size() != count" << std::endl;
    }

    return d / (double)vectors.size();
  }

  static double calculateML2FromSpecifiedCentroids(std::vector<std::vector<float>> &vectors,
                                                   std::vector<Cluster> &clusters,
                                                   std::vector<size_t> &centroidIds) {
    double d     = 0.0;
    size_t count = 0;
    for (auto it = centroidIds.begin(); it != centroidIds.end(); ++it) {
      Cluster &cluster = clusters[(*it)];
      count += cluster.members.size();
      for (auto mit = cluster.members.begin(); mit != cluster.members.end(); ++mit) {
        d += distanceL2(cluster.centroid, vectors[(*mit).vectorID]);
      }
    }
    return d / (double)vectors.size();
  }

  void setupInitialClusters(std::vector<std::vector<float>> &vectors, size_t numberOfClusters,
                            std::vector<Cluster> &clusters) {
    if (clusters.empty()) {
      switch (initializationMode) {
      case InitializationModeHead: {
        getInitialCentroidsFromHead(vectors, clusters, numberOfClusters);
        break;
      }
      case InitializationModeRandom: {
        getInitialCentroidsRandomly(vectors, clusters, numberOfClusters);
        break;
      }
      case InitializationModeRandomFixedSeed: {
        getInitialCentroidsRandomly(vectors, clusters, numberOfClusters, 1);
        break;
      }
      case InitializationModeKmeansPlusPlus: {
        getInitialCentroidsKmeansPlusPlus(vectors, clusters, numberOfClusters);
        break;
      }
      case InitializationModeKmeansPlusPlusFixedSeed: {
        getInitialCentroidsKmeansPlusPlus(vectors, clusters, numberOfClusters, 1);
        break;
      }
      default:
        std::stringstream msg;
        msg << " kmeans: invalid initialization mode. " << initializationMode;
        NGTThrowException(msg);
      }
    }
  }

  bool kmeans(std::vector<std::vector<float>> &vectors, std::vector<Cluster> &clusters) {
    return kmeans(vectors, numberOfClusters, clusters);
  }

  bool kmeans(std::vector<std::vector<float>> &vectors, size_t numberOfClusters,
              std::vector<Cluster> &clusters) {
    if (vectors.size() == 0) {
      std::stringstream msg;
      msg << "Clustering::kmeans: No vector.";
      NGTThrowException(msg);
    }
    if (vectors[0].size() == 0) {
      std::stringstream msg;
      msg << "Clustering::kmeans: No dimension.";
      NGTThrowException(msg);
    }

    setupInitialClusters(vectors, numberOfClusters, clusters);
    switch (clusteringType) {
    case ClusteringTypeKmeansWithoutNGT: return kmeansWithoutNGT(vectors, numberOfClusters, clusters); break;
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
    case ClusteringTypeKmeansWithNGT: return kmeansWithNGT(vectors, numberOfClusters, clusters); break;
#endif
    default:
      std::stringstream msg;
      msg << " kmeans: invalid clustering type. " << clusteringType;
      NGTThrowException(msg);
    }
  }

  static void evaluate(std::vector<std::vector<float>> &vectors, std::vector<Cluster> &clusters, char mode,
                       std::vector<size_t> centroidIds = std::vector<size_t>()) {
    size_t clusterSize = std::numeric_limits<size_t>::max();
    assign(vectors, clusters, clusterSize);

    std::cout << "The number of vectors=" << vectors.size() << std::endl;
    std::cout << "The number of centroids=" << clusters.size() << std::endl;
    if (centroidIds.size() == 0) {
      switch (mode) {
      case 'e': std::cout << "MSE=" << calculateMSE(vectors, clusters) << std::endl; break;
      case '2':
      default: std::cout << "ML2=" << calculateML2(vectors, clusters) << std::endl; break;
      }
    } else {
      switch (mode) {
      case 'e': break;
      case '2':
      default:
        std::cout << "ML2=" << calculateML2FromSpecifiedCentroids(vectors, clusters, centroidIds)
                  << std::endl;
        break;
      }
    }
  }

  void setClusterSizeConstraintCoefficient(float v) { clusterSizeConstraintCoefficient = v; }

  ClusteringType clusteringType;
  InitializationMode initializationMode;
  size_t numberOfClusters;
  bool clusterSizeConstraint;
  float clusterSizeConstraintCoefficient;
  size_t maximumIteration;
  float epsilonFrom;
  float epsilonTo;
  float epsilonStep;
  size_t resultSizeCoefficient;
  vector<double> diffHistory;
  bool silence;
};

} // namespace NGT
