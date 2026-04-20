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

#ifdef _OPENMP
#include <omp.h>
#else
#warning "*** OMP is *NOT* available! ***"
#endif

#include "Common.h"
#include "ObjectSpace.h"
#include "ObjectRepository.h"
#include "PrimitiveComparator.h"

namespace NGT {

template <typename OBJECT_TYPE, typename COMPARE_TYPE>
class ObjectSpaceRepository : public ObjectSpace, public ObjectRepository {
 public:
  class ComparatorL1 : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorL1(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareL1((OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb[0],
                                            dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareL1((OBJECT_TYPE *)&objecta[0],
                                            (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareL1((OBJECT_TYPE *)&objecta.at(0, allocator),
                                            (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorL1(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareL1((OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb[0],
                                            dimension);
    }
#endif
  };

  class ComparatorL2 : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorL2(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareL2((OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb[0],
                                            dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareL2((OBJECT_TYPE *)&objecta[0],
                                            (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareL2((OBJECT_TYPE *)&objecta.at(0, allocator),
                                            (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorL2(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareL2((OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb[0],
                                            dimension);
    }
#endif
  };
#ifdef NGT_PQ4
  class ComparatorPq4L2 : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorPq4L2(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareL2((OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb[0],
                                            dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareL2((OBJECT_TYPE *)&objecta[0],
                                            (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareL2((OBJECT_TYPE *)&objecta.at(0, allocator),
                                            (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorPq4L2(size_t d, NGT::ObjectSpace &os) : Comparator(d), objectSpace(os) {}
    ~ComparatorPq4L2() {}
    double operator()(Object &objecta, Object &objectb) {
      auto d = objectSpace.getQuantizer().compareL2((void *)&objecta[0], (void *)&objectb[0], dimension);
      return d;
    }
    NGT::ObjectSpace &objectSpace;
#endif
  };
  class ComparatorPq4CosineSimilarity : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorPq4L2(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareCosineSimilarity((OBJECT_TYPE *)&objecta[0],
                                                          (OBJECT_TYPE *)&objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareCosineSimilarity(
          (OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareCosineSimilarity(
          (OBJECT_TYPE *)&objecta.at(0, allocator), (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorPq4CosineSimilarity(size_t d, NGT::ObjectSpace &os) : Comparator(d), objectSpace(os) {}
    ~ComparatorPq4CosineSimilarity() {}
    double operator()(Object &objecta, Object &objectb) {
      auto d = objectSpace.getQuantizer().compareCosineSimilarity((void *)&objecta[0], (void *)&objectb[0],
                                                                  dimension);
      return d;
    }
    NGT::ObjectSpace &objectSpace;
#endif
  };
  class ComparatorPq4NormalizedCosineSimilarity : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorPq4L2(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareNormalizedCosineSimilarity((OBJECT_TYPE *)&objecta[0],
                                                                    (OBJECT_TYPE *)&objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareNormalizedCosineSimilarity(
          (OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareNormalizedCosineSimilarity(
          (OBJECT_TYPE *)&objecta.at(0, allocator), (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorPq4NormalizedCosineSimilarity(size_t d, NGT::ObjectSpace &os)
        : Comparator(d), objectSpace(os) {}
    ~ComparatorPq4NormalizedCosineSimilarity() {}
    double operator()(Object &objecta, Object &objectb) {
      auto dim = objectSpace.getDimension();
      auto d   = objectSpace.getQuantizer().compareNormalizedCosineSimilarity((void *)&objecta[0],
                                                                              (void *)&objectb[0], dim);
      return d;
    }
    NGT::ObjectSpace &objectSpace;
#endif
  };
#endif

  class ComparatorNormalizedL2 : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorNormalizedL2(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareNormalizedL2((OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb[0],
                                                      dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareNormalizedL2((OBJECT_TYPE *)&objecta[0],
                                                      (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareNormalizedL2((OBJECT_TYPE *)&objecta.at(0, allocator),
                                                      (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorNormalizedL2(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareNormalizedL2((OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb[0],
                                                      dimension);
    }
#endif
  };

  class ComparatorHammingDistance : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorHammingDistance(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareHammingDistance((OBJECT_TYPE *)&objecta[0],
                                                         (OBJECT_TYPE *)&objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareHammingDistance((OBJECT_TYPE *)&objecta[0],
                                                         (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareHammingDistance((OBJECT_TYPE *)&objecta.at(0, allocator),
                                                         (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorHammingDistance(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareHammingDistance((OBJECT_TYPE *)&objecta[0],
                                                         (OBJECT_TYPE *)&objectb[0], dimension);
    }
#endif
  };

  class ComparatorJaccardDistance : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorJaccardDistance(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareJaccardDistance((OBJECT_TYPE *)&objecta[0],
                                                         (OBJECT_TYPE *)&objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareJaccardDistance((OBJECT_TYPE *)&objecta[0],
                                                         (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareJaccardDistance((OBJECT_TYPE *)&objecta.at(0, allocator),
                                                         (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorJaccardDistance(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareJaccardDistance((OBJECT_TYPE *)&objecta[0],
                                                         (OBJECT_TYPE *)&objectb[0], dimension);
    }
#endif
  };

  class ComparatorSparseJaccardDistance : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorSparseJaccardDistance(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareSparseJaccardDistance((OBJECT_TYPE *)&objecta[0],
                                                               (OBJECT_TYPE *)&objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareSparseJaccardDistance(
          (OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareSparseJaccardDistance(
          (OBJECT_TYPE *)&objecta.at(0, allocator), (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorSparseJaccardDistance(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareSparseJaccardDistance((OBJECT_TYPE *)&objecta[0],
                                                               (OBJECT_TYPE *)&objectb[0], dimension);
    }
#endif
  };

  class ComparatorAngleDistance : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorAngleDistance(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareAngleDistance((OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb[0],
                                                       dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareAngleDistance((OBJECT_TYPE *)&objecta[0],
                                                       (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareAngleDistance((OBJECT_TYPE *)&objecta.at(0, allocator),
                                                       (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorAngleDistance(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareAngleDistance((OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb[0],
                                                       dimension);
    }
#endif
  };

  class ComparatorNormalizedAngleDistance : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorNormalizedAngleDistance(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareNormalizedAngleDistance((OBJECT_TYPE *)&objecta[0],
                                                                 (OBJECT_TYPE *)&objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareNormalizedAngleDistance(
          (OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareNormalizedAngleDistance(
          (OBJECT_TYPE *)&objecta.at(0, allocator), (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorNormalizedAngleDistance(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareNormalizedAngleDistance((OBJECT_TYPE *)&objecta[0],
                                                                 (OBJECT_TYPE *)&objectb[0], dimension);
    }
#endif
  };

  class ComparatorCosineSimilarity : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorCosineSimilarity(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareCosineSimilarity((OBJECT_TYPE *)&objecta[0],
                                                          (OBJECT_TYPE *)&objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareCosineSimilarity(
          (OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareCosineSimilarity(
          (OBJECT_TYPE *)&objecta.at(0, allocator), (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorCosineSimilarity(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareCosineSimilarity((OBJECT_TYPE *)&objecta[0],
                                                          (OBJECT_TYPE *)&objectb[0], dimension);
    }
#endif
  };

  class ComparatorNormalizedCosineSimilarity : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorNormalizedCosineSimilarity(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareNormalizedCosineSimilarity((OBJECT_TYPE *)&objecta[0],
                                                                    (OBJECT_TYPE *)&objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareNormalizedCosineSimilarity(
          (OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareNormalizedCosineSimilarity(
          (OBJECT_TYPE *)&objecta.at(0, allocator), (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorNormalizedCosineSimilarity(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareNormalizedCosineSimilarity((OBJECT_TYPE *)&objecta[0],
                                                                    (OBJECT_TYPE *)&objectb[0], dimension);
    }
#endif
  };
  class ComparatorNormalizedCosineSimilarityQsint8Quint8 : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorNormalizedCosineSimilarityQsint8Quint8(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::NormalizedCosineSimilarityQsint8::compare(&objecta[0], &objectb[0],
                                                                            dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::NormalizedCosineSimilarityQsint8::compare(
          &objecta[0], &objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::NormalizedCosineSimilarityQsint8::compare(
          &objecta.at(0, allocator), &objectb.at(0, allocator), dimension);
    }
#else
    ComparatorNormalizedCosineSimilarityQsint8Quint8(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::NormalizedCosineSimilarityQsint8::compare(&objecta[0], &objectb[0],
                                                                            dimension);
    }
#endif
  };
  class ComparatorNormalizedCosineSimilarityQuint8Quint8 : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorNormalizedCosineSimilarityQuint8Quint8(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::NormalizedCosineSimilarityQuint8::compare(&objecta[0], &objectb[0],
                                                                            dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::NormalizedCosineSimilarityQuint8::compare(
          &objecta[0], &objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::NormalizedCosineSimilarityQuint8::compare(
          &objecta.at(0, allocator), &objectb.at(0, allocator), dimension);
    }
#else
    ComparatorNormalizedCosineSimilarityQuint8Quint8(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::NormalizedCosineSimilarityQuint8::compare(&objecta[0], &objectb[0],
                                                                            dimension);
    }
#endif
  };

  class ComparatorPoincareDistance : public Comparator { // added by Nyapicom
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorPoincareDistance(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::comparePoincareDistance((OBJECT_TYPE *)&objecta[0],
                                                          (OBJECT_TYPE *)&objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::comparePoincareDistance(
          (OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::comparePoincareDistance(
          (OBJECT_TYPE *)&objecta.at(0, allocator), (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorPoincareDistance(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::comparePoincareDistance((OBJECT_TYPE *)&objecta[0],
                                                          (OBJECT_TYPE *)&objectb[0], dimension);
    }
#endif
  };

  class ComparatorLorentzDistance : public Comparator { // added by Nyapicom
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorLorentzDistance(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareLorentzDistance((OBJECT_TYPE *)&objecta[0],
                                                         (OBJECT_TYPE *)&objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareLorentzDistance((OBJECT_TYPE *)&objecta[0],
                                                         (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareLorentzDistance((OBJECT_TYPE *)&objecta.at(0, allocator),
                                                         (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorLorentzDistance(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareLorentzDistance((OBJECT_TYPE *)&objecta[0],
                                                         (OBJECT_TYPE *)&objectb[0], dimension);
    }
#endif
  };

  class ComparatorInnerProduct : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorInnerProduct(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return -PrimitiveComparator::compareDotProduct((OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb[0],
                                                     dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return -PrimitiveComparator::compareDotProduct((OBJECT_TYPE *)&objecta[0],
                                                     (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return -PrimitiveComparator::compareDotProduct((OBJECT_TYPE *)&objecta.at(0, allocator),
                                                     (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorInnerProduct(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      auto d = PrimitiveComparator::compareDotProduct((OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb[0],
                                                      dimension);
      return -d;
    }
#endif
  };
  class ComparatorInnerProductQsint8Quint8 : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorInnerProductQsint8Quint8(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::InnerProductQsint8::compare(&objecta[0], &objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::InnerProductQsint8::compare(&objecta[0], &objectb.at(0, allocator),
                                                              dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::InnerProductQsint8::compare(&objecta.at(0, allocator),
                                                              &objectb.at(0, allocator), dimension);
    }
#else
    ComparatorInnerProductQsint8Quint8(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::InnerProductQsint8::compare(&objecta[0], &objectb[0], dimension);
    }
#endif
  };
  class ComparatorL2Quint8Quint8 : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorL2Quint8Quint8(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareL2((quint8 *)&objecta[0], (quint8 *)&objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareL2((quint8 *)&objecta[0], (quint8 *)&objectb.at(0, allocator),
                                            dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return PrimitiveComparator::compareL2((quint8 *)&objecta.at(0, allocator),
                                            (quint8 *)&objectb.at(0, allocator), dimension);
    }
#else
    ComparatorL2Quint8Quint8(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return PrimitiveComparator::compareL2((quint8 *)&objecta[0], (quint8 *)&objectb[0], dimension);
    }
#endif
  };
  class ComparatorDotProduct : public Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ComparatorDotProduct(size_t d, SharedMemoryAllocator &a) : Comparator(d, a) {}
    double operator()(Object &objecta, Object &objectb) {
      return magnitude - PrimitiveComparator::compareDotProduct((OBJECT_TYPE *)&objecta[0],
                                                                (OBJECT_TYPE *)&objectb[0], dimension);
    }
    double operator()(Object &objecta, PersistentObject &objectb) {
      return magnitude - PrimitiveComparator::compareDotProduct(
                             (OBJECT_TYPE *)&objecta[0], (OBJECT_TYPE *)&objectb.at(0, allocator), dimension);
    }
    double operator()(PersistentObject &objecta, PersistentObject &objectb) {
      return magnitude - PrimitiveComparator::compareDotProduct((OBJECT_TYPE *)&objecta.at(0, allocator),
                                                                (OBJECT_TYPE *)&objectb.at(0, allocator),
                                                                dimension);
    }
#else
    ComparatorDotProduct(size_t d) : Comparator(d) {}
    double operator()(Object &objecta, Object &objectb) {
      return magnitude - PrimitiveComparator::compareDotProduct((OBJECT_TYPE *)&objecta[0],
                                                                (OBJECT_TYPE *)&objectb[0], dimension);
    }
#endif
    float magnitude;
  };

  ObjectSpaceRepository(size_t d, const std::type_info &ot, DistanceType t, float mag = -1)
      : ObjectSpace(ObjectSpace::getDimensionForType(ot, d)),
        ObjectRepository(ObjectSpace::getDimensionForType(ot, d), ot) {
    size_t objectSize = 0;
    if (ot == typeid(uint8_t)) {
      objectSize = sizeof(uint8_t);
    } else if (ot == typeid(float)) {
      objectSize = sizeof(float);
#ifdef NGT_HALF_FLOAT
    } else if (ot == typeid(float16)) {
      objectSize = sizeof(float16);
#endif
    } else if (ot == typeid(qsint8)) {
      objectSize = sizeof(qsint8);
#ifdef NGT_BFLOAT
    } else if (ot == typeid(bfloat16)) {
      objectSize = sizeof(bfloat16);
#endif
#ifdef NGT_PQ4
    } else if (ot == typeid(qint4)) {
      objectSize = sizeof(qint4);
#endif
    } else {
      std::stringstream msg;
      msg << "ObjectSpace::constructor: Not supported type. " << ot.name();
      NGTThrowException(msg);
    }
    setLength(objectSize * ObjectSpace::getDimension());
    setPaddedLength(objectSize * ObjectSpace::getPaddedDimension());
    magnitude = mag;
    setDistanceType(t);
  }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  void open(const std::string &f, size_t sharedMemorySize) { ObjectRepository::open(f, sharedMemorySize); }
  void copy(PersistentObject &objecta, PersistentObject &objectb) { objecta = objectb; }

  void show(std::ostream &os, PersistentObject &object) {
    const std::type_info &t = getObjectType();
    if (t == typeid(uint8_t)) {
      auto *optr = static_cast<unsigned char *>(&object.at(0, allocator));
      for (size_t i = 0; i < getDimension(); i++) {
        os << (int)optr[i] << " ";
      }
#ifdef NGT_HALF_FLOAT
    } else if (t == typeid(float16)) {
      auto *optr = reinterpret_cast<float16 *>(&object.at(0, allocator));
      for (size_t i = 0; i < getDimension(); i++) {
        os << optr[i] << " ";
      }
#endif
    } else if (t == typeid(float)) {
      auto *optr = reinterpret_cast<float *>(&object.at(0, allocator));
      for (size_t i = 0; i < getDimension(); i++) {
        os << optr[i] << " ";
      }
    } else {
      os << " not implement for the type.";
    }
  }

  Object *allocateObject(Object &o) {
    Object *po = new Object(getByteSizeOfObject());
    for (size_t i = 0; i < getByteSizeOfObject(); i++) {
      (*po)[i] = o[i];
    }
    return po;
  }
  Object *allocateObject(PersistentObject &o) {
    PersistentObject &spo = (PersistentObject &)o;
    Object *po            = new Object(getByteSizeOfObject());
    for (size_t i = 0; i < getByteSizeOfObject(); i++) {
      (*po)[i] = spo.at(i, ObjectRepository::allocator);
    }
    return (Object *)po;
  }
  void deleteObject(PersistentObject *po) { delete po; }
#endif // NGT_SHARED_MEMORY_ALLOCATOR

  void copy(Object &objecta, Object &objectb) { objecta.copy(objectb, getByteSizeOfObject()); }

  void setDistanceType(DistanceType t) {
    if (comparator != 0) {
      delete comparator;
      comparator = 0;
    }
    if (comparatorForSearch != 0) {
      delete comparatorForSearch;
      comparatorForSearch = 0;
    }
    assert(ObjectSpace::dimension != 0);
    distanceType = t;
#ifdef NGT_PQ4
    if (type == typeid(qint4)) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      NGTThrowException("Not supported qint4 for the shared memory mode.");
#endif
      switch (distanceType) {
      case DistanceTypeL2:
        comparator = new ObjectSpaceRepository::ComparatorPq4L2(ObjectSpace::getPaddedDimension(), *this);
        break;
      case DistanceTypeNormalizedCosine:
        comparator = new ObjectSpaceRepository::ComparatorPq4NormalizedCosineSimilarity(
            ObjectSpace::getPaddedDimension(), *this);
        normalization = true;
        break;
      default:
        std::stringstream msg;
        msg << "NGT::ObjectSpaceRepository: The distance type is invalid for qint4. " << distanceType;
        NGTThrowException(msg);
      }
      return;
    }
#endif
    switch (distanceType) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    case DistanceTypeL1:
      comparator = new ObjectSpaceRepository::ComparatorL1(ObjectSpace::getPaddedDimension(),
                                                           ObjectRepository::allocator);
      break;
    case DistanceTypeL2:
      comparator = new ObjectSpaceRepository::ComparatorL2(ObjectSpace::getPaddedDimension(),
                                                           ObjectRepository::allocator);
      break;
    case DistanceTypeNormalizedL2:
      comparator    = new ObjectSpaceRepository::ComparatorNormalizedL2(ObjectSpace::getPaddedDimension(),
                                                                        ObjectRepository::allocator);
      normalization = true;
      break;
    case DistanceTypeHamming:
      comparator = new ObjectSpaceRepository::ComparatorHammingDistance(ObjectSpace::getPaddedDimension(),
                                                                        ObjectRepository::allocator);
      break;
    case DistanceTypeJaccard:
      comparator = new ObjectSpaceRepository::ComparatorJaccardDistance(ObjectSpace::getPaddedDimension(),
                                                                        ObjectRepository::allocator);
      break;
    case DistanceTypeSparseJaccard:
      comparator = new ObjectSpaceRepository::ComparatorSparseJaccardDistance(
          ObjectSpace::getPaddedDimension(), ObjectRepository::allocator);
      setSparse();
      break;
    case DistanceTypeAngle:
      comparator = new ObjectSpaceRepository::ComparatorAngleDistance(ObjectSpace::getPaddedDimension(),
                                                                      ObjectRepository::allocator);
      break;
    case DistanceTypeCosine:
      comparator = new ObjectSpaceRepository::ComparatorCosineSimilarity(ObjectSpace::getPaddedDimension(),
                                                                         ObjectRepository::allocator);
      break;
    case DistanceTypePoincare: // added by Nyapicom
      comparator = new ObjectSpaceRepository::ComparatorPoincareDistance(ObjectSpace::getPaddedDimension(),
                                                                         ObjectRepository::allocator);
      break;
    case DistanceTypeLorentz: // added by Nyapicom
      comparator = new ObjectSpaceRepository::ComparatorLorentzDistance(ObjectSpace::getPaddedDimension(),
                                                                        ObjectRepository::allocator);
      break;
    case DistanceTypeNormalizedAngle:
      comparator = new ObjectSpaceRepository::ComparatorNormalizedAngleDistance(
          ObjectSpace::getPaddedDimension(), ObjectRepository::allocator);
      normalization = true;
      break;
    case DistanceTypeNormalizedCosine:
      if (typeid(OBJECT_TYPE) == typeid(qsint8)) {
        comparator = new ObjectSpaceRepository::ComparatorL2Quint8Quint8(ObjectSpace::getPaddedDimension(),
                                                                         ObjectRepository::allocator);
        comparatorForSearch = new ObjectSpaceRepository::ComparatorNormalizedCosineSimilarityQsint8Quint8(
            ObjectSpace::getPaddedDimension(), ObjectRepository::allocator);
      } else {
        comparator = new ObjectSpaceRepository::ComparatorNormalizedCosineSimilarity(
            ObjectSpace::getPaddedDimension(), ObjectRepository::allocator);
      }
      normalization = true;
      break;
    case DistanceTypeInnerProduct:
      if (typeid(OBJECT_TYPE) == typeid(qsint8)) {
        comparator = new ObjectSpaceRepository::ComparatorL2Quint8Quint8(ObjectSpace::getPaddedDimension(),
                                                                         ObjectRepository::allocator);
        comparatorForSearch = new ObjectSpaceRepository::ComparatorInnerProductQsint8Quint8(
            ObjectSpace::getPaddedDimension(), ObjectRepository::allocator);
      } else {
        comparator = new ObjectSpaceRepository::ComparatorL2(ObjectSpace::getPaddedDimension(),
                                                             ObjectRepository::allocator);
      }
      setInnerProduct();
      break;
    case DistanceTypeDotProduct: {
      auto *comp      = new ObjectSpaceRepository::ComparatorDotProduct(ObjectSpace::getPaddedDimension(),
                                                                        ObjectRepository::allocator);
      comp->magnitude = magnitude;
      comparator      = comp;
      setInnerProduct();
      break;
    }
#else
    case DistanceTypeL1:
      comparator = new ObjectSpaceRepository::ComparatorL1(ObjectSpace::getPaddedDimension());
      break;
    case DistanceTypeL2:
      comparator = new ObjectSpaceRepository::ComparatorL2(ObjectSpace::getPaddedDimension());
      break;
    case DistanceTypeNormalizedL2:
      comparator    = new ObjectSpaceRepository::ComparatorNormalizedL2(ObjectSpace::getPaddedDimension());
      normalization = true;
      break;
    case DistanceTypeHamming:
      comparator = new ObjectSpaceRepository::ComparatorHammingDistance(ObjectSpace::getPaddedDimension());
      break;
    case DistanceTypeJaccard:
      comparator = new ObjectSpaceRepository::ComparatorJaccardDistance(ObjectSpace::getPaddedDimension());
      break;
    case DistanceTypeSparseJaccard:
      comparator =
          new ObjectSpaceRepository::ComparatorSparseJaccardDistance(ObjectSpace::getPaddedDimension());
      setSparse();
      break;
    case DistanceTypeAngle:
      comparator = new ObjectSpaceRepository::ComparatorAngleDistance(ObjectSpace::getPaddedDimension());
      break;
    case DistanceTypeCosine:
      comparator = new ObjectSpaceRepository::ComparatorCosineSimilarity(ObjectSpace::getPaddedDimension());
      break;
    case DistanceTypePoincare: // added by Nyapicom
      comparator = new ObjectSpaceRepository::ComparatorPoincareDistance(ObjectSpace::getPaddedDimension());
      break;
    case DistanceTypeLorentz: // added by Nyapicom
      comparator = new ObjectSpaceRepository::ComparatorLorentzDistance(ObjectSpace::getPaddedDimension());
      break;
    case DistanceTypeNormalizedAngle:
      comparator =
          new ObjectSpaceRepository::ComparatorNormalizedAngleDistance(ObjectSpace::getPaddedDimension());
      normalization = true;
      break;
    case DistanceTypeNormalizedCosine:
      if (typeid(OBJECT_TYPE) == typeid(qsint8)) {
        comparator = new ObjectSpaceRepository::ComparatorL2Quint8Quint8(ObjectSpace::getPaddedDimension());
        comparatorForSearch = new ObjectSpaceRepository::ComparatorNormalizedCosineSimilarityQsint8Quint8(
            ObjectSpace::getPaddedDimension());
      } else {
        comparator = new ObjectSpaceRepository::ComparatorNormalizedCosineSimilarity(
            ObjectSpace::getPaddedDimension());
      }
      normalization = true;
      break;
    case DistanceTypeInnerProduct:
      if (typeid(OBJECT_TYPE) == typeid(qsint8)) {
        comparator = new ObjectSpaceRepository::ComparatorL2Quint8Quint8(ObjectSpace::getPaddedDimension());
        comparatorForSearch =
            new ObjectSpaceRepository::ComparatorInnerProductQsint8Quint8(ObjectSpace::getPaddedDimension());
      } else {
        comparator = new ObjectSpaceRepository::ComparatorL2(ObjectSpace::getPaddedDimension());
      }
      setInnerProduct();
      break;
    case DistanceTypeDotProduct: {
      auto *comp      = new ObjectSpaceRepository::ComparatorDotProduct(ObjectSpace::getPaddedDimension());
      comp->magnitude = magnitude;
      comparator      = comp;
      setInnerProduct();
      break;
    }
#endif
    default:
      std::stringstream msg;
      msg << "NGT::ObjectSpaceRepository: The distance type is invalid. " << distanceType;
      NGTThrowException(msg);
    }
  }

  void serialize(const std::string &ofile) { ObjectRepository::serialize(ofile, this); }
  void deserialize(const std::string &ifile) { ObjectRepository::deserialize(ifile, this); }
  void serializeAsText(const std::string &ofile) { ObjectRepository::serializeAsText(ofile, this); }
  void deserializeAsText(const std::string &ifile) { ObjectRepository::deserializeAsText(ifile, this); }
  void readText(std::istream &is, size_t dataSize) { ObjectRepository::readText(is, dataSize); }
  void appendText(std::istream &is, size_t dataSize) { ObjectRepository::appendText(is, dataSize); }

  void append(const float *data, size_t dataSize) { ObjectRepository::append(data, dataSize); }
  void append(const double *data, size_t dataSize) { ObjectRepository::append(data, dataSize); }
  void append(const uint8_t *data, size_t dataSize) { ObjectRepository::append(data, dataSize); }
#ifdef NGT_HALF_FLOAT
  void append(const float16 *data, size_t dataSize) { ObjectRepository::append(data, dataSize); }
#endif

  void deleteAll() { ObjectRepository::deleteAll(); }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  PersistentObject *allocatePersistentObject(Object &obj) {
    return ObjectRepository::allocatePersistentObject(obj);
  }
  size_t insert(PersistentObject *obj) { return ObjectRepository::insert(obj); }
#else
  size_t insert(Object *obj) { return ObjectRepository::insert(obj); }
#endif

  void remove(size_t id) { ObjectRepository::remove(id); }

  void linearSearch(Object &query, double radius, size_t size, ResultSet &results) {
    auto *comp    = comparator;
    bool allocate = false;
    if (typeid(OBJECT_TYPE) == typeid(qsint8)) {
      if (distanceType == DistanceTypeInnerProduct) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        comp = new ObjectSpaceRepository::ComparatorInnerProductQsint8Quint8(
            ObjectSpace::getPaddedDimension(), ObjectRepository::allocator);
#else
        comp =
            new ObjectSpaceRepository::ComparatorInnerProductQsint8Quint8(ObjectSpace::getPaddedDimension());
#endif
        allocate = true;
      } else if (distanceType == DistanceTypeNormalizedCosine) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        comp = new ObjectSpaceRepository::ComparatorNormalizedCosineSimilarityQsint8Quint8(
            ObjectSpace::getPaddedDimension(), ObjectRepository::allocator);
#else
        comp = new ObjectSpaceRepository::ComparatorNormalizedCosineSimilarityQsint8Quint8(
            ObjectSpace::getPaddedDimension());
#endif
        allocate = true;
      }
    }
    try {
      linearSearch(query, radius, size, results, *comp);
    } catch (Exception &err) {
      if (allocate) delete comp;
      throw err;
    }
    if (allocate) delete comp;
  }
  void linearSearch(Object &query, double radius, size_t size, ResultSet &results, Comparator &comparator) {
    if (!results.empty()) {
      NGTThrowException("lenearSearch: results is not empty");
    }
#ifndef NGT_PREFETCH_DISABLED
    size_t byteSizeOfObject     = getByteSizeOfObject();
    const size_t prefetchOffset = getPrefetchOffset();
#endif
    ObjectRepository &rep = *this;
    for (size_t idx = 1; idx < rep.size(); idx++) {
#ifndef NGT_PREFETCH_DISABLED
      if (idx + prefetchOffset < rep.size() && rep[idx + prefetchOffset] != 0) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        MemoryCache::prefetch(
            (unsigned char *)&(*static_cast<PersistentObject *>(ObjectRepository::get(idx + prefetchOffset))),
            byteSizeOfObject);
#else
        MemoryCache::prefetch(
            (unsigned char *)&(*static_cast<PersistentObject *>(rep[idx + prefetchOffset]))[0],
            byteSizeOfObject);
#endif
      }
#endif
      if (rep[idx] == 0) {
        continue;
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      Distance d = comparator(query, (PersistentObject &)*rep[idx]);
#else
      Distance d = comparator(query, (Object &)*rep[idx]);
#endif
      if (radius < 0.0 || d <= radius) {
        NGT::ObjectDistance obj(idx, d);
        results.push(obj);
        if (results.size() > size) {
          results.pop();
        }
      }
    }
    return;
  }

  float computeMaxMagnitude(NGT::ObjectID beginID = 1) {
    float maxMag          = 0.0;
    ObjectRepository &rep = *this;
    auto nOfThreads       = omp_get_max_threads();
    std::vector<float> maxm(nOfThreads, 0.0);
#pragma omp parallel for
    for (size_t idx = beginID; idx < rep.size(); idx++) {
      if (rep[idx] == 0) {
        continue;
      }
      auto thdID = omp_get_thread_num();
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      auto object = getObject(*rep[idx], allocator);
#else
      auto object = getObject(*rep[idx]);
#endif
      double mag = 0.0;
      for (size_t i = 0; i < object.size() - 1; i++) {
        mag += object[i] * object[i];
      }
      if (mag > maxm[thdID]) {
        maxm[thdID] = mag;
      }
    }
    for (int ti = 0; ti < nOfThreads; ti++) {
      if (maxm[ti] > maxMag) {
        maxMag = maxm[ti];
      }
    }
    return maxMag;
  }
  void setMagnitude(float maxMag,
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
                    NGT::PersistentRepository<void> &graphNodes,
#else
                    NGT::Repository<void> &graphNodes,
#endif
                    NGT::ObjectID beginID = 1) {
    ObjectRepository &rep = *this;
#pragma omp parallel for
    for (size_t idx = beginID; idx < rep.size(); idx++) {
      if (rep[idx] == 0) {
        continue;
      }
      if (idx < graphNodes.size() && graphNodes[idx] != 0) {
        continue;
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      auto object = getObject(*rep[idx], allocator);
#else
      auto object = getObject(*rep[idx]);
#endif
      double mag = 0.0;
      for (size_t i = 0; i < object.size() - 1; i++) {
        mag += object[i] * object[i];
      }
      auto v = maxMag - static_cast<float>(mag);
      if (v < 0.0) {
        std::cerr << "Warning! magnitude is larger than the current max magnitude. " << idx << ":" << v << ":"
                  << maxMag << ":" << static_cast<float>(mag) << std::endl;
        v = 0.0;
      }
      object.back() = sqrt(v);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      setObject(*rep[idx], object, allocator);
#else
      setObject(*rep[idx], object);
#endif
    }
  }

  std::pair<float, float> getMaxMin(float clippingRate = 0.02, size_t size = 0) {
    ObjectRepository &rep = *this;
    if (size == 0) {
      size = rep.size();
    } else {
      size = size > rep.size() ? size : rep.size();
    }
    auto dim          = getDimension();
    auto clippingSize = static_cast<float>(size) * clippingRate;
    clippingSize      = clippingSize == 0 ? 1 : clippingSize;
    std::priority_queue<float> min;
    std::priority_queue<float, std::vector<float>, std::greater<float>> max;
    std::cerr << "repo size=" << rep.size() << " " << clippingSize << std::endl;
    for (size_t idx = 1; idx < rep.size(); idx++) {
      try {
        OBJECT_TYPE *obj = static_cast<OBJECT_TYPE *>(getObject(idx));
        for (size_t i = 0; i < dim; i++) {
          float v = static_cast<float>(obj[i]);
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
      } catch (...) {
      }
    }
    auto ret = std::make_pair(max.top(), min.top());
    return ret;
  }

  void *getObject(size_t idx) {
    if (isEmpty(idx)) {
      std::stringstream msg;
      msg << "NGT::ObjectSpaceRepository: The specified ID is out of the range. The object ID should be "
             "greater than zero. "
          << idx << ":" << ObjectRepository::size() << ".";
      NGTThrowException(msg);
    }
    PersistentObject &obj = *(*this)[idx];
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    return reinterpret_cast<OBJECT_TYPE *>(&obj.at(0, allocator));
#else
    return reinterpret_cast<OBJECT_TYPE *>(&obj[0]);
#endif
  }

  void getObject(size_t idx, std::vector<float> &v) {
    OBJECT_TYPE *obj = static_cast<OBJECT_TYPE *>(getObject(idx));
    size_t dim       = getDimension();
    v.resize(dim);
    for (size_t i = 0; i < dim; i++) {
      v[i] = static_cast<float>(obj[i]);
    }
  }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  std::vector<float> getObject(PersistentObject &object, SharedMemoryAllocator &allocator) {
    std::vector<float> v;
    OBJECT_TYPE *obj = static_cast<OBJECT_TYPE *>(object.getPointer(allocator));
    size_t dim       = getDimension();
    v.resize(dim);
    for (size_t i = 0; i < dim; i++) {
      v[i] = static_cast<float>(obj[i]);
    }
    return v;
  }
#endif
  std::vector<float> getObject(Object &object) {
    std::vector<float> v;
    OBJECT_TYPE *obj = static_cast<OBJECT_TYPE *>(object.getPointer());
    size_t dim       = getDimension();
    v.resize(dim);
    for (size_t i = 0; i < dim; i++) {
      v[i] = static_cast<float>(obj[i]);
    }
    return v;
  }

  void getObjects(const std::vector<size_t> &idxs, std::vector<std::vector<float>> &vs) {
    vs.resize(idxs.size());
    auto v = vs.begin();
    for (auto idx = idxs.begin(); idx != idxs.end(); idx++, v++) {
      getObject(*idx, *v);
    }
  }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  void normalize(PersistentObject &object) {
    auto *obj = reinterpret_cast<OBJECT_TYPE *>(object.getPointer(getRepository().getAllocator()));
    ObjectSpace::normalize(obj, ObjectSpace::dimension);
  }
#endif
  void normalize(Object &object) {
    auto *obj = reinterpret_cast<OBJECT_TYPE *>(object.getPointer());
    ObjectSpace::normalize(obj, ObjectSpace::dimension);
  }

  Object *allocateObject() { return ObjectRepository::allocateObject(); }
  void deleteObject(Object *po) { ObjectRepository::deleteObject(po); }

  Object *allocateNormalizedObject(const std::string &textLine, const std::string &sep) {
    Object *allocatedObject = ObjectRepository::allocateObject(textLine, sep);
    if (normalization) {
      try {
        normalize(*allocatedObject);
      } catch (Exception &err) {
        ObjectRepository::deleteObject(allocatedObject);
        throw err;
      }
    }
    return allocatedObject;
  }

  Object *allocateNormalizedObject(const std::vector<double> &obj) {
    Object *allocatedObject = 0;
    if (quantizationIsEnabled()) {
      std::vector<float> qobj(obj.begin(), obj.end());
      if (normalization) {
        ObjectSpace::normalize(qobj);
      }
      quantizeToQint8(qobj);
      allocatedObject = ObjectRepository::allocateObject(qobj);
    } else {
      allocatedObject = ObjectRepository::allocateObject(obj);
      if (normalization) {
        try {
          normalize(*allocatedObject);
        } catch (Exception &err) {
          ObjectRepository::deleteObject(allocatedObject);
          throw err;
        }
      }
    }
    return allocatedObject;
  }
  Object *allocateNormalizedObject(const std::vector<float> &obj) {
    Object *allocatedObject = 0;
    if (quantizationIsEnabled()) {
      std::vector<float> qobj(obj.begin(), obj.end());
      if (normalization) {
        ObjectSpace::normalize(qobj);
      }
      if (scalarQuantizationIsEnabled()) {
        quantizeToQint8(qobj);
        allocatedObject = ObjectRepository::allocateObject(qobj);
#ifdef NGT_PQ4
      } else {
        std::vector<uint8_t> pq4obj;
        ObjectSpace::quantizer.quantizeToPq4(qobj, pq4obj);
        allocatedObject = ObjectRepository::allocateObject(pq4obj);
#endif
      }
    } else {
      allocatedObject = ObjectRepository::allocateObject(obj);
      if (normalization) {
        try {
          normalize(*allocatedObject);
        } catch (Exception &err) {
          ObjectRepository::deleteObject(allocatedObject);
          throw err;
        }
      }
    }
    return allocatedObject;
  }
#ifdef NGT_HALF_FLOAT
  Object *allocateNormalizedObject(const std::vector<float16> &obj) {
    Object *allocatedObject = 0;
    if (quantizationIsEnabled()) {
      std::vector<float> qobj(obj.begin(), obj.end());
      if (normalization) {
        ObjectSpace::normalize(qobj);
      }
      quantizeToQint8(qobj);
      allocatedObject = ObjectRepository::allocateObject(qobj);
    } else {
      allocatedObject = ObjectRepository::allocateObject(obj);
      if (normalization) {
        try {
          normalize(*allocatedObject);
        } catch (Exception &err) {
          ObjectRepository::deleteObject(allocatedObject);
          throw err;
        }
      }
    }
    return allocatedObject;
  }
#endif
  Object *allocateNormalizedObject(const std::vector<uint8_t> &obj) {
    Object *allocatedObject = 0;
    if (quantizationIsEnabled()) {
      std::vector<float> qobj(obj.begin(), obj.end());
      if (normalization) {
        ObjectSpace::normalize(qobj);
      }
      quantizeToQint8(qobj);
      allocatedObject = ObjectRepository::allocateObject(qobj);
    } else {
      allocatedObject = ObjectRepository::allocateObject(obj);
      if (normalization) {
        try {
          normalize(*allocatedObject);
        } catch (Exception &err) {
          ObjectRepository::deleteObject(allocatedObject);
          throw err;
        }
      }
    }
    return allocatedObject;
  }

  Object *allocateNormalizedObject(const float *obj, size_t size) {
    Object *allocatedObject = 0;
    try {
      if (quantizationIsEnabled()) {
        std::vector<float> qobj(obj, obj + size);
        if (normalization) {
          ObjectSpace::normalize(qobj);
        }
        quantizeToQint8(qobj);
        allocatedObject = ObjectRepository::allocateObject(qobj);
      } else {
        allocatedObject = ObjectRepository::allocateObject(obj, size);
        if (normalization) {
          try {
            normalize(*allocatedObject);
          } catch (Exception &err) {
            ObjectRepository::deleteObject(allocatedObject);
            throw err;
          }
        }
      }
    } catch (Exception &err) {
      std::stringstream msg;
      msg << err.what() << " quantization=" << (quantizationIsEnabled() ? "True" : "False");
      NGTThrowException(msg);
    }
    return allocatedObject;
  }

  PersistentObject *allocateNormalizedPersistentObject(const std::vector<double> &obj) {
    PersistentObject *allocatedObject = 0;
    if (quantizationIsEnabled()) {
      std::vector<float> qobj(obj.begin(), obj.end());
      if (normalization) {
        ObjectSpace::normalize(qobj);
      }
      auto shift = isQsint8Quint8();
      quantizeToQint8(qobj, shift);
      allocatedObject = ObjectRepository::allocatePersistentObject(qobj);
    } else {
      allocatedObject = ObjectRepository::allocatePersistentObject(obj);
      if (normalization) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        normalize(*allocatedObject);
#else
        try {
          normalize(*allocatedObject);
        } catch (Exception &err) {
          ObjectRepository::deleteObject(allocatedObject);
          throw err;
        }
#endif
      }
    }
    return allocatedObject;
  }
  PersistentObject *allocateNormalizedPersistentObject(const std::vector<float> &obj) {
    PersistentObject *allocatedObject = 0;
    try {
      if (quantizationIsEnabled()) {
        std::vector<float> qobj(obj.begin(), obj.end());
        if (normalization) {
          ObjectSpace::normalize(qobj);
        }
        if (scalarQuantizationIsEnabled()) {
          auto shift = isQsint8Quint8();
          quantizeToQint8(qobj, shift);
          allocatedObject = ObjectRepository::allocatePersistentObject(qobj);
#ifdef NGT_PQ4
        } else {
          std::vector<uint8_t> pq4obj;
          ObjectSpace::quantizer.quantizeToPq4(qobj, pq4obj);
          allocatedObject = ObjectRepository::allocatePersistentObject(pq4obj);
#endif
        }
      } else {
        allocatedObject = ObjectRepository::allocatePersistentObject(obj);
        if (normalization) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          normalize(*allocatedObject);
#else
          try {
            normalize(*allocatedObject);
          } catch (Exception &err) {
            ObjectRepository::deleteObject(allocatedObject);
            throw err;
          }
#endif
        }
      }
    } catch (Exception &err) {
      std::stringstream msg;
      msg << err.what() << " quantization=" << (quantizationIsEnabled() ? "True" : "False");
      NGTThrowException(msg);
    }
    return allocatedObject;
  }
#ifdef NGT_HALF_FLOAT
  PersistentObject *allocateNormalizedPersistentObject(const std::vector<float16> &obj) {
    PersistentObject *allocatedObject = 0;
    if (quantizationIsEnabled()) {
      std::vector<float> qobj(obj.begin(), obj.end());
      if (normalization) {
        ObjectSpace::normalize(qobj);
      }
      auto shift = isQsint8Quint8();
      quantizeToQint8(qobj, shift);
      allocatedObject = ObjectRepository::allocatePersistentObject(qobj);
    } else {
      allocatedObject = ObjectRepository::allocatePersistentObject(obj);
      if (normalization) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        normalize(*allocatedObject);
#else
        try {
          normalize(*allocatedObject);
        } catch (Exception &err) {
          ObjectRepository::deleteObject(allocatedObject);
          throw err;
        }
#endif
      }
    }
    return allocatedObject;
  }
#endif
  PersistentObject *allocateNormalizedPersistentObject(const std::vector<uint8_t> &obj) {
    PersistentObject *allocatedObject = 0;
    if (quantizationIsEnabled()) {
      std::vector<float> qobj(obj.begin(), obj.end());
      if (normalization) {
        ObjectSpace::normalize(qobj);
      }
      auto shift = isQsint8Quint8();
      quantizeToQint8(qobj, shift);
      allocatedObject = ObjectRepository::allocatePersistentObject(qobj);
    } else {
      allocatedObject = ObjectRepository::allocatePersistentObject(obj);
      if (normalization) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        normalize(*allocatedObject);
#else
        try {
          normalize(*allocatedObject);
        } catch (Exception &err) {
          ObjectRepository::deleteObject(allocatedObject);
          throw err;
        }
#endif
      }
    }
    return allocatedObject;
  }

  size_t getSize() { return ObjectRepository::size(); }
  size_t getSizeOfElement() { return sizeof(OBJECT_TYPE); }
  const std::type_info &getObjectType() { return typeid(OBJECT_TYPE); }
  size_t getByteSizeOfObject() { return getByteSize(); }

  ObjectRepository &getRepository() { return *this; }

  void show(std::ostream &os, Object &object) {
    const std::type_info &t = getObjectType();
    if (t == typeid(uint8_t)) {
      unsigned char *optr = static_cast<unsigned char *>(&object[0]);
      for (size_t i = 0; i < getDimension(); i++) {
        os << (int)optr[i] << " ";
      }
    } else if (t == typeid(float)) {
      float *optr = reinterpret_cast<float *>(&object[0]);
      for (size_t i = 0; i < getDimension(); i++) {
        os << optr[i] << " ";
      }
#ifdef NGT_HALF_FLOAT
    } else if (t == typeid(float16)) {
      float16 *optr = reinterpret_cast<float16 *>(&object[0]);
      for (size_t i = 0; i < getDimension(); i++) {
        os << optr[i] << " ";
      }
#endif
    } else {
      os << " not implement for the type.";
    }
  }

  bool isQsint8Quint8() {
    return ((distanceType == ObjectSpace::DistanceTypeInnerProduct) ||
            (distanceType == ObjectSpace::DistanceTypeNormalizedCosine)) &&
           (typeid(OBJECT_TYPE) == typeid(qsint8));
  }

}; // namespace NGT

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
// set v in objectspace to this object using allocator.
inline void PersistentObject::set(PersistentObject &po, ObjectSpace &objectspace) {
  SharedMemoryAllocator &allocator = objectspace.getRepository().getAllocator();
  uint8_t *src                     = (uint8_t *)&po.at(0, allocator);
  uint8_t *dst                     = (uint8_t *)&(*this).at(0, allocator);
  memcpy(dst, src, objectspace.getByteSizeOfObject());
}

inline off_t PersistentObject::allocate(ObjectSpace &objectspace) {
  SharedMemoryAllocator &allocator = objectspace.getRepository().getAllocator();
  return allocator.getOffset(new (allocator) PersistentObject(allocator, &objectspace));
}

inline void PersistentObject::serializeAsText(std::ostream &os, ObjectSpace *objectspace) {
  assert(objectspace != 0);
  SharedMemoryAllocator &allocator = objectspace->getRepository().getAllocator();
  const std::type_info &t          = objectspace->getObjectType();
  void *ref                        = &(*this).at(0, allocator);
  size_t dimension                 = objectspace->getDimension();
  if (t == typeid(uint8_t)) {
    NGT::Serializer::writeAsText(os, (uint8_t *)ref, dimension);
  } else if (t == typeid(qsint8)) {
    NGT::Serializer::writeAsText(os, (int8_t *)ref, dimension);
  } else if (t == typeid(float)) {
    NGT::Serializer::writeAsText(os, (float *)ref, dimension);
#ifdef NGT_HALF_FLOAT
  } else if (t == typeid(float16)) {
    NGT::Serializer::writeAsText(os, (float16 *)ref, dimension);
#endif
  } else if (t == typeid(double)) {
    NGT::Serializer::writeAsText(os, (double *)ref, dimension);
  } else if (t == typeid(uint16_t)) {
    NGT::Serializer::writeAsText(os, (uint16_t *)ref, dimension);
  } else if (t == typeid(uint32_t)) {
    NGT::Serializer::writeAsText(os, (uint32_t *)ref, dimension);
  } else {
    std::cerr << "ObjectT::serializeAsText: not supported data type. [" << t.name() << "]" << std::endl;
    assert(0);
  }
}

inline void PersistentObject::deserializeAsText(std::ifstream &is, ObjectSpace *objectspace) {
  assert(objectspace != 0);
  SharedMemoryAllocator &allocator = objectspace->getRepository().getAllocator();
  const std::type_info &t          = objectspace->getObjectType();
  size_t dimension                 = objectspace->getDimension();
  void *ref                        = &(*this).at(0, allocator);
  assert(ref != 0);
  if (t == typeid(uint8_t)) {
    NGT::Serializer::readAsText(is, (uint8_t *)ref, dimension);
  } else if (t == typeid(float)) {
    NGT::Serializer::readAsText(is, (float *)ref, dimension);
#ifdef NGT_HALF_FLOAT
  } else if (t == typeid(float16)) {
    NGT::Serializer::readAsText(is, (float16 *)ref, dimension);
#endif
  } else if (t == typeid(double)) {
    NGT::Serializer::readAsText(is, (double *)ref, dimension);
  } else if (t == typeid(uint16_t)) {
    NGT::Serializer::readAsText(is, (uint16_t *)ref, dimension);
  } else if (t == typeid(uint32_t)) {
    NGT::Serializer::readAsText(is, (uint32_t *)ref, dimension);
  } else {
    std::cerr << "Object::deserializeAsText: not supported data type. [" << t.name() << "]" << std::endl;
    assert(0);
  }
}

#endif
} // namespace NGT
