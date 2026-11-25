//
// Copyright (C) 2026 Masajiro Iwasaki
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

#include "NGT/Common.h"

namespace NGTQG {

// Filtered QG sorter. All pushed objects remain in the exploration buckets;
// only accepted objects participate in the result slots and radius boundary.
// Unlike the packed sorter, nodes never move: result slots contain indices.
// This keeps the exploration cursor independent of result replacement.
class FilteredDistanceBucketSorter {
 public:
  using Object = NGT::detail::DistanceBucketSorterObject;

 private:
  static constexpr unsigned DISTANCE_SHIFT = QBG_DISTANCE_SHIFT;
  static constexpr size_t NUM_BUCKETS      = (0xFFFF >> DISTANCE_SHIFT) + 1;
  static constexpr size_t NUM_WORDS        = (NUM_BUCKETS + 63) / 64;
  static constexpr uint16_t INVALID_BUCKET = 0xFFFF;
  static constexpr uint32_t INVALID_OBJECT = 0xFFFFFFFFu;

  struct Node {
    Object object;
    uint32_t next;
    uint32_t nextAccepted;
    uint32_t resultSlot;
  };
  struct Position {
    uint16_t bucket = INVALID_BUCKET;
    uint32_t object = INVALID_OBJECT;
  };

  std::vector<Node> nodes;
  std::vector<uint32_t> results;
  uint32_t bucketHead[NUM_BUCKETS];
  uint32_t acceptedHead[NUM_BUCKETS];
  uint16_t bucketEpoch[NUM_BUCKETS]{};
  uint64_t nonempty[NUM_WORDS]{};
  uint16_t epoch = 0;
  Position unvisitedMin;
  Position rangeMax;
  Position sizeMax;
  size_t searchSize        = 0;
  size_t objectListSize    = 0;
  size_t acceptedCount     = 0;
  uint16_t currentDistance = 0;

  static uint16_t bucketOf(uint16_t distance) { return distance >> DISTANCE_SHIFT; }

  uint16_t nextBucket(uint16_t from) const {
    if (from >= NUM_BUCKETS) return INVALID_BUCKET;
    size_t word   = from >> 6;
    uint64_t bits = nonempty[word] & (~uint64_t{0} << (from & 63));
    while (true) {
      if (bits != 0) return static_cast<uint16_t>((word << 6) + __builtin_ctzll(bits));
      if (++word == NUM_WORDS) return INVALID_BUCKET;
      bits = nonempty[word];
    }
  }

  // Move one accepted element towards the nearer buckets. Accepted lists also
  // retain popped/evicted objects, just as the original packed bucket lists do.
  bool moveBoundary(Position &position) {
    auto next = nodes[position.object].nextAccepted;
    if (next != INVALID_OBJECT) {
      position.object = next;
      return false;
    }
    for (uint16_t bucket = position.bucket; bucket > 0;) {
      --bucket;
      if (bucketEpoch[bucket] == epoch && acceptedHead[bucket] != INVALID_OBJECT) {
        position = {bucket, acceptedHead[bucket]};
        return true;
      }
    }
    return false;
  }

 public:
  void reset(size_t size, float expansion) {
    const double expanded = static_cast<double>(size) * std::max(1.0f, expansion);
    if (size == 0 || !std::isfinite(expansion) || expanded > std::numeric_limits<uint16_t>::max()) {
      NGTThrowException("FilteredDistanceBucketSorter: invalid result size or expansion.");
    }
    searchSize = size;
    // Match the existing sorter's float multiplication/truncation.
    objectListSize  = static_cast<size_t>(static_cast<float>(size) * std::max(1.0f, expansion));
    acceptedCount   = 0;
    currentDistance = 0;
    nodes.clear();
    if (nodes.capacity() < NGTQG_DISTANCE_HEAP_RESERVE) nodes.reserve(NGTQG_DISTANCE_HEAP_RESERVE);
    results.clear();
    results.reserve(objectListSize);
    if (++epoch == 0) {
      std::memset(bucketEpoch, 0, sizeof(bucketEpoch));
      epoch = 1;
    }
    std::memset(nonempty, 0, sizeof(nonempty));
    unvisitedMin = {};
    rangeMax     = {};
    sizeMax      = {};
  }

  // Returns true only when the accepted k-th-distance boundary becomes ready
  // or changes bucket. Rejected objects never shrink the exploration radius.
  bool push(const Object &object, bool accepted) {
    const auto bucket = bucketOf(object.distance);
    if (bucketEpoch[bucket] != epoch) {
      bucketEpoch[bucket]  = epoch;
      bucketHead[bucket]   = INVALID_OBJECT;
      acceptedHead[bucket] = INVALID_OBJECT;
    }
    if (nodes.size() >= INVALID_OBJECT) {
      NGTThrowException("FilteredDistanceBucketSorter: too many exploration candidates.");
    }
    const auto idx = static_cast<uint32_t>(nodes.size());
    nodes.push_back({object, bucketHead[bucket], INVALID_OBJECT, INVALID_OBJECT});
    nodes.back().object.visited = false;
    bucketHead[bucket]          = idx;
    nonempty[bucket >> 6] |= uint64_t{1} << (bucket & 63);
    if (unvisitedMin.bucket == INVALID_BUCKET || bucket <= unvisitedMin.bucket) {
      unvisitedMin = {bucket, idx};
    }
    if (!accepted) return false;

    nodes[idx].nextAccepted = acceptedHead[bucket];
    acceptedHead[bucket]    = idx;
    if (acceptedCount < objectListSize) {
      nodes[idx].resultSlot = static_cast<uint32_t>(results.size());
      results.push_back(idx);
    } else if (bucket < sizeMax.bucket) {
      const auto slot                  = nodes[sizeMax.object].resultSlot;
      nodes[sizeMax.object].resultSlot = INVALID_OBJECT;
      nodes[idx].resultSlot            = slot;
      results[slot]                    = idx;
    }
    ++acceptedCount;

    bool changed = false;
    if (acceptedCount > objectListSize) {
      if (bucket < sizeMax.bucket) moveBoundary(sizeMax);
      if (bucket < rangeMax.bucket) {
        if (objectListSize == searchSize) {
          changed  = rangeMax.bucket != sizeMax.bucket;
          rangeMax = sizeMax;
        } else {
          changed = moveBoundary(rangeMax);
        }
      }
    } else if (acceptedCount > searchSize) {
      if (bucket < rangeMax.bucket) {
        changed = moveBoundary(rangeMax);
      } else if (bucket >= sizeMax.bucket) {
        sizeMax = {bucket, idx};
      }
    } else {
      if (rangeMax.bucket == INVALID_BUCKET || bucket >= rangeMax.bucket) rangeMax = {bucket, idx};
      if (acceptedCount == searchSize) {
        sizeMax = rangeMax;
        changed = true;
      }
    }
    return changed;
  }

  bool pop(Object &object) {
    auto bucket = unvisitedMin.bucket;
    auto idx    = unvisitedMin.object;
    while (bucket != INVALID_BUCKET) {
      while (idx != INVALID_OBJECT) {
        auto &node = nodes[idx];
        if (!node.object.visited) {
          node.object.visited = true;
          object              = node.object;
          currentDistance     = bucket << DISTANCE_SHIFT;
          object.distance     = currentDistance;
          unvisitedMin        = {bucket, node.next};
          return true;
        }
        idx = node.next;
      }
      nonempty[bucket >> 6] &= ~(uint64_t{1} << (bucket & 63));
      bucket = nextBucket(bucket + 1);
      if (bucket != INVALID_BUCKET) idx = bucketHead[bucket];
    }
    unvisitedMin = {};
    return false;
  }

  uint16_t getCurrentDistance() const { return currentDistance; }

  uint16_t getMaxDistance() const {
    return acceptedCount < searchSize ? std::numeric_limits<uint16_t>::max()
                                      : static_cast<uint16_t>(rangeMax.bucket << DISTANCE_SHIFT);
  }

  size_t getObjects(Object *output) const {
    for (size_t i = 0; i < results.size(); ++i) {
      output[i]          = nodes[results[i]].object;
      output[i].distance = bucketOf(output[i].distance) << DISTANCE_SHIFT;
    }
    return results.size();
  }
};

} // namespace NGTQG
