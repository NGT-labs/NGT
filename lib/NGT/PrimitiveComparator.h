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

#include "NGT/defines.h"

#if defined(NGT_NO_SIMD)
#include "NGT/PrimitiveComparatorNoArch.h"
#elif defined(__x86_64__)
#include "NGT/PrimitiveComparatorX86.h"
#elif defined(NGT_NEON)
#include "NGT/PrimitiveComparatorArm.h"
#else
#include "NGT/PrimitiveComparatorNoArch.h"
#endif

namespace NGT {

class PrimitiveComparator::L1Uint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareL1((const uint8_t *)a, (const uint8_t *)b, size);
  }
};

class PrimitiveComparator::L2Uint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareL2((const uint8_t *)a, (const uint8_t *)b, size);
  }
};

class PrimitiveComparator::HammingUint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareHammingDistance((const uint8_t *)a, (const uint8_t *)b, size);
  }
};

class PrimitiveComparator::JaccardUint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareJaccardDistance((const uint8_t *)a, (const uint8_t *)b, size);
  }
};

class PrimitiveComparator::SparseJaccardFloat {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareSparseJaccardDistance((const float *)a, (const float *)b, size);
  }
};

class PrimitiveComparator::L2Float {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareL2((const float *)a, (const float *)b, size);
  }
};

class PrimitiveComparator::NormalizedL2Float {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareNormalizedL2((const float *)a, (const float *)b, size);
  }
};

class PrimitiveComparator::L1Float {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareL1((const float *)a, (const float *)b, size);
  }
};

class PrimitiveComparator::CosineSimilarityFloat {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareCosineSimilarity((const float *)a, (const float *)b, size);
  }
};

class PrimitiveComparator::NormalizedCosineSimilarityFloat {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareNormalizedCosineSimilarity((const float *)a, (const float *)b, size);
  }
};

class PrimitiveComparator::AngleFloat {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareAngleDistance((const float *)a, (const float *)b, size);
  }
};

class PrimitiveComparator::NormalizedAngleFloat {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareNormalizedAngleDistance((const float *)a, (const float *)b, size);
  }
};

// added by Nyapicom
class PrimitiveComparator::PoincareFloat {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::comparePoincareDistance((const float *)a, (const float *)b, size);
  }
};

// added by Nyapicom
class PrimitiveComparator::LorentzFloat {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareLorentzDistance((const float *)a, (const float *)b, size);
  }
};

#ifdef NGT_HALF_FLOAT
class PrimitiveComparator::SparseJaccardFloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareSparseJaccardDistance((const float16 *)a, (const float16 *)b, size);
  }
};

class PrimitiveComparator::L2Float16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareL2((const float16 *)a, (const float16 *)b, size);
  }
};

class PrimitiveComparator::NormalizedL2Float16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareNormalizedL2((const float16 *)a, (const float16 *)b, size);
  }
};

class PrimitiveComparator::L1Float16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareL1((const float16 *)a, (const float16 *)b, size);
  }
};

class PrimitiveComparator::CosineSimilarityFloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareCosineSimilarity((const float16 *)a, (const float16 *)b, size);
  }
};

class PrimitiveComparator::NormalizedCosineSimilarityFloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareNormalizedCosineSimilarity((const float16 *)a, (const float16 *)b,
                                                                  size);
  }
};

class PrimitiveComparator::AngleFloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareAngleDistance((const float16 *)a, (const float16 *)b, size);
  }
};

class PrimitiveComparator::NormalizedAngleFloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareNormalizedAngleDistance((const float16 *)a, (const float16 *)b, size);
  }
};

// added by Nyapicom
class PrimitiveComparator::PoincareFloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::comparePoincareDistance((const float16 *)a, (const float16 *)b, size);
  }
};

// added by Nyapicom
class PrimitiveComparator::LorentzFloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareLorentzDistance((const float16 *)a, (const float16 *)b, size);
  }
};
#endif
#ifdef NGT_BFLOAT
class PrimitiveComparator::SparseJaccardBfloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};

class PrimitiveComparator::L2Bfloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareL2((const bfloat16 *)a, (const bfloat16 *)b, size);
  }
};

class PrimitiveComparator::NormalizedL2Bfloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareNormalizedL2((const bfloat16 *)a, (const bfloat16 *)b, size);
  }
};

class PrimitiveComparator::L1Bfloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};

class PrimitiveComparator::CosineSimilarityBfloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};

class PrimitiveComparator::NormalizedCosineSimilarityBfloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};

class PrimitiveComparator::AngleBfloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};

class PrimitiveComparator::NormalizedAngleBfloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};

// added by Nyapicom
class PrimitiveComparator::PoincareBfloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};

// added by Nyapicom
class PrimitiveComparator::LorentzBfloat16 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};
#endif

#ifdef NGT_PQ4
class PrimitiveComparator::L2Qint4 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareL2((const qint4 *)a, (const qint4 *)b, size);
  }
};

class PrimitiveComparator::CosineSimilarityQint4 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareCosineSimilarity((const qint4 *)a, (const qint4 *)b, size);
  }
};

class PrimitiveComparator::NormalizedCosineSimilarityQint4 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareNormalizedCosineSimilarity((const qint4 *)a, (const qint4 *)b, size);
  }
};

class PrimitiveComparator::InnerProductQint4 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};
#endif

class PrimitiveComparator::SparseJaccardQsint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};

class PrimitiveComparator::L2Qsint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareL2((const qsint8 *)a, (const qsint8 *)b, size);
  }
};

class PrimitiveComparator::NormalizedL2Qsint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareNormalizedL2((const qsint8 *)a, (const qsint8 *)b, size);
  }
};

class PrimitiveComparator::L1Qsint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareL1((const qsint8 *)a, (const qsint8 *)b, size);
  }
};

class PrimitiveComparator::CosineSimilarityQsint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    return PrimitiveComparator::compareCosineSimilarity((const qsint8 *)a, (const qsint8 *)b, size);
  }
};

class PrimitiveComparator::AngleQsint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};

class PrimitiveComparator::NormalizedAngleQsint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};

// added by Nyapicom
class PrimitiveComparator::PoincareQsint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};

// added by Nyapicom
class PrimitiveComparator::LorentzQsint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    NGTThrowException("Not supported.");
  }
};

class PrimitiveComparator::InnerProductQsint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    auto d = PrimitiveComparator::compareDotProduct((const qsint8 *)a, (const quint8 *)b, size);
    return 127.0 * 127.0 * size - d;
  }
};

class PrimitiveComparator::NormalizedCosineSimilarityQuint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    auto *aptr = reinterpret_cast<const uint8_t *>(a);
    auto *bptr = reinterpret_cast<const uint8_t *>(b);
    float suma = 0.0;
    float sumb = 0.0;
    for (size_t i = 0; i < size; i++) {
      suma += *aptr++;
      sumb += *bptr++;
    }
    float max = 255.0 * 255.0 * size;
    auto d    = PrimitiveComparator::compareDotProduct((const quint8 *)a, (const quint8 *)b, size);
    d -= 127.0 * (suma + sumb) + 127.0 * 127.0 * size;
    return max - d;
  }
};

class PrimitiveComparator::NormalizedCosineSimilarityQsint8 {
 public:
  inline static double compare(const void *a, const void *b, size_t size) {
    float max = 127.0 * 255.0 * size;
    auto d    = PrimitiveComparator::compareDotProduct((const qsint8 *)a, (const quint8 *)b, size);
    return max - d;
  }
};

} // namespace NGT
