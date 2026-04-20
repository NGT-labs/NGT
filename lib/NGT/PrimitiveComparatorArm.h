//
// Copyright (C) 2026 Yahoo Japan Corporation
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

#include "NGT/defines.h"
#include <arm_neon.h>
#include <cstddef>

namespace NGT {

class MemoryCache {
 public:
  inline static void prefetch(unsigned char *ptr, const size_t byteSizeOfObject) {
    switch ((byteSizeOfObject - 1) >> 6) {
    default:
    case 28: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 27: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 26: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 25: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 24: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 23: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 22: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 21: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 20: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 19: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 18: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 17: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 16: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 15: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 14: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 13: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 12: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 11: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 10: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 9: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 8: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 7: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 6: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 5: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 4: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 3: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 2: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 1: __builtin_prefetch(ptr, 0, 3); ptr += 64;
    case 0:
      __builtin_prefetch(ptr, 0, 3);
      ptr += 64;
      break;
    }
  }

  inline static void *alignedAlloc(const size_t allocSize) {
    const size_t alignment = 64;
    const uintptr_t mask   = ~(static_cast<uintptr_t>(alignment - 1));

    uint8_t *p_base = new uint8_t[allocSize + alignment];
    if (!p_base) {
      NGTThrowException("MemoryCache::alignedAlloc: Memory allocation failed.");
      return nullptr;
    }

    uint8_t *ptr_to_align_down = p_base + alignment;
    uint8_t *aligned_ptr =
        reinterpret_cast<uint8_t *>((reinterpret_cast<uintptr_t>(ptr_to_align_down)) & mask);

    uint8_t *filler = p_base;
    *filler++       = 0xAB;
    while (filler < aligned_ptr) {
      *filler++ = 0xCD;
    }

    return aligned_ptr;
  }

  inline static void alignedFree(void *ptr_to_free) {
    if (!ptr_to_free) {
      return;
    }
    uint8_t *p = static_cast<uint8_t *>(ptr_to_free);
    p--;
    while (*p == 0xCD) {
      p--;
    }
    if (*p != 0xAB) {
      NGTThrowException("MemoryCache::alignedFree: Fatal Error! Cannot find allocated address.");
    }
    delete[] p;
  }
};

class PrimitiveComparator {
 public:
  static double absolute(double v) { return fabs(v); }
  static int absolute(int v) { return abs(v); }

  inline static double horizontalAddF32(float32x4_t v) {
#if defined(__aarch64__)
    return static_cast<double>(vaddvq_f32(v));
#else
    float32x2_t temp = vadd_f32(vget_low_f32(v), vget_high_f32(v));
    temp             = vpadd_f32(temp, temp);
    return static_cast<double>(vget_lane_f32(temp, 0));
#endif
  }

  inline static uint64_t horizontalAddU32(uint32x4_t v) {
#if defined(__aarch64__)
    return static_cast<uint64_t>(vaddvq_u32(v));
#else
    uint64x2_t temp = vpaddlq_u32(v);
    return static_cast<uint64_t>(vgetq_lane_u64(temp, 0) + vgetq_lane_u64(temp, 1));
#endif
  }

  inline static uint64_t horizontalAddS32(int32x4_t v) {
#if defined(__aarch64__)
    return static_cast<uint64_t>(vaddvq_s32(v));
#else
    int64x2_t temp = vpaddlq_s32(v);
    return static_cast<uint64_t>(vgetq_lane_s64(temp, 0) + vgetq_lane_s64(temp, 1));
#endif
  }

  inline static uint32_t horizontalMaxU32(uint32x4_t v) {
#if defined(__aarch64__)
    return vmaxvq_u32(v);
#else
    uint32x2_t temp = vpmax_u32(vget_low_u32(v), vget_high_u32(v));
    temp            = vpmax_u32(temp, temp);
    return vget_lane_u32(temp, 0);
#endif
  }

  inline static bool anyZeroU32(uint32x4_t v) {
    const uint32x4_t zero = vdupq_n_u32(0);
    uint32x4_t eq         = vceqq_u32(v, zero);
#if defined(__aarch64__)
    return vmaxvq_u32(eq) != 0;
#else
    uint32x2_t temp = vpmax_u32(vget_low_u32(eq), vget_high_u32(eq));
    temp            = vpmax_u32(temp, temp);
    return vget_lane_u32(temp, 0) != 0;
#endif
  }

  inline static double compareL2(const float *a, const float *b, size_t size) {
    float32x4_t sum1 = vdupq_n_f32(0.0f);
    float32x4_t sum2 = vdupq_n_f32(0.0f);
    float32x4_t sum3 = vdupq_n_f32(0.0f);
    float32x4_t sum4 = vdupq_n_f32(0.0f);

    const float *last      = a + size;
    const float *lastgroup = last - 15;

    while (a < lastgroup) {
      float32x4_t a1 = vld1q_f32(a);
      float32x4_t b1 = vld1q_f32(b);
      float32x4_t d1 = vsubq_f32(a1, b1);
      sum1           = vmlaq_f32(sum1, d1, d1);

      float32x4_t a2 = vld1q_f32(a + 4);
      float32x4_t b2 = vld1q_f32(b + 4);
      float32x4_t d2 = vsubq_f32(a2, b2);
      sum2           = vmlaq_f32(sum2, d2, d2);

      float32x4_t a3 = vld1q_f32(a + 8);
      float32x4_t b3 = vld1q_f32(b + 8);
      float32x4_t d3 = vsubq_f32(a3, b3);
      sum3           = vmlaq_f32(sum3, d3, d3);

      float32x4_t a4 = vld1q_f32(a + 12);
      float32x4_t b4 = vld1q_f32(b + 12);
      float32x4_t d4 = vsubq_f32(a4, b4);
      sum4           = vmlaq_f32(sum4, d4, d4);

      a += 16;
      b += 16;
    }

    sum1 = vaddq_f32(sum1, sum2);
    sum3 = vaddq_f32(sum3, sum4);
    sum1 = vaddq_f32(sum1, sum3);

    const float *lastgroup_simd = last - 3;
    while (a < lastgroup_simd) {
      float32x4_t va   = vld1q_f32(a);
      float32x4_t vb   = vld1q_f32(b);
      float32x4_t diff = vsubq_f32(va, vb);
      sum1             = vmlaq_f32(sum1, diff, diff);
      a += 4;
      b += 4;
    }

    double sum = 0.0;
#if defined(__aarch64__)
    sum = static_cast<double>(vaddvq_f32(sum1));
#else
    float32x2_t temp_sum = vpadd_f32(vget_low_f32(sum1), vget_high_f32(sum1));
    temp_sum             = vpadd_f32(temp_sum, temp_sum);
    sum                  = static_cast<double>(vget_lane_f32(temp_sum, 0));
#endif

    while (a < last) {
      float diff = *a++ - *b++;
      sum += diff * diff;
    }

    return std::sqrt(sum);
  }

#ifdef NGT_HALF_FLOAT
  inline static double compareL2(const float16_t *a, const float16_t *b, size_t size) {
    float32x4_t sum1 = vdupq_n_f32(0.0f);
    float32x4_t sum2 = vdupq_n_f32(0.0f);
    float32x4_t sum3 = vdupq_n_f32(0.0f);
    float32x4_t sum4 = vdupq_n_f32(0.0f);

    const float16_t *last      = a + size;
    const float16_t *lastgroup = last - 15;

    while (a < lastgroup) {
      float32x4_t d1 = vsubq_f32(vcvt_f32_f16(vld1_f16(a)), vcvt_f32_f16(vld1_f16(b)));
      sum1           = vmlaq_f32(sum1, d1, d1);

      float32x4_t d2 = vsubq_f32(vcvt_f32_f16(vld1_f16(a + 4)), vcvt_f32_f16(vld1_f16(b + 4)));
      sum2           = vmlaq_f32(sum2, d2, d2);

      float32x4_t d3 = vsubq_f32(vcvt_f32_f16(vld1_f16(a + 8)), vcvt_f32_f16(vld1_f16(b + 8)));
      sum3           = vmlaq_f32(sum3, d3, d3);

      float32x4_t d4 = vsubq_f32(vcvt_f32_f16(vld1_f16(a + 12)), vcvt_f32_f16(vld1_f16(b + 12)));
      sum4           = vmlaq_f32(sum4, d4, d4);

      a += 16;
      b += 16;
    }

    sum1 = vaddq_f32(sum1, sum2);
    sum3 = vaddq_f32(sum3, sum4);
    sum1 = vaddq_f32(sum1, sum3);

    double sum = 0.0;
#if defined(__aarch64__)
    sum = static_cast<double>(vaddvq_f32(sum1));
#else
    float32x2_t temp_sum = vpadd_f32(vget_low_f32(sum1), vget_high_f32(sum1));
    temp_sum             = vpadd_f32(temp_sum, temp_sum);
    sum                  = static_cast<double>(vget_lane_f32(temp_sum, 0));
#endif

    const float16_t *lastgroup_simd = last - 3;
    while (a < lastgroup_simd) {
      float32x4_t diff = vsubq_f32(vcvt_f32_f16(vld1_f16(a)), vcvt_f32_f16(vld1_f16(b)));
      float sum_part   = vaddvq_f32(vmlaq_f32(vdupq_n_f32(0.0f), diff, diff));
      sum += sum_part;
      a += 4;
      b += 4;
    }

    while (a < last) {
      float diff = static_cast<float>(*a++) - static_cast<float>(*b++);
      sum += diff * diff;
    }

    return std::sqrt(sum);
  }
#endif

  inline static double compareL2(const unsigned char *a, const unsigned char *b, size_t size) {
    int32x4_t sum_vec1 = vdupq_n_s32(0);
    int32x4_t sum_vec2 = vdupq_n_s32(0);

    const unsigned char *last      = a + size;
    const unsigned char *lastgroup = last - 31;

    while (a < lastgroup) {
      uint8x16_t va1   = vld1q_u8(a);
      uint8x16_t vb1   = vld1q_u8(b);
      uint16x8_t va1_l = vmovl_u8(vget_low_u8(va1));
      uint16x8_t va1_h = vmovl_u8(vget_high_u8(va1));
      uint16x8_t vb1_l = vmovl_u8(vget_low_u8(vb1));
      uint16x8_t vb1_h = vmovl_u8(vget_high_u8(vb1));
      int16x8_t d1_l   = vreinterpretq_s16_u16(vsubq_u16(va1_l, vb1_l));
      int16x8_t d1_h   = vreinterpretq_s16_u16(vsubq_u16(va1_h, vb1_h));
      sum_vec1         = vmlal_s16(sum_vec1, vget_low_s16(d1_l), vget_low_s16(d1_l));
      sum_vec1         = vmlal_s16(sum_vec1, vget_high_s16(d1_l), vget_high_s16(d1_l));
      sum_vec1         = vmlal_s16(sum_vec1, vget_low_s16(d1_h), vget_low_s16(d1_h));
      sum_vec1         = vmlal_s16(sum_vec1, vget_high_s16(d1_h), vget_high_s16(d1_h));

      uint8x16_t va2   = vld1q_u8(a + 16);
      uint8x16_t vb2   = vld1q_u8(b + 16);
      uint16x8_t va2_l = vmovl_u8(vget_low_u8(va2));
      uint16x8_t va2_h = vmovl_u8(vget_high_u8(va2));
      uint16x8_t vb2_l = vmovl_u8(vget_low_u8(vb2));
      uint16x8_t vb2_h = vmovl_u8(vget_high_u8(vb2));
      int16x8_t d2_l   = vreinterpretq_s16_u16(vsubq_u16(va2_l, vb2_l));
      int16x8_t d2_h   = vreinterpretq_s16_u16(vsubq_u16(va2_h, vb2_h));
      sum_vec2         = vmlal_s16(sum_vec2, vget_low_s16(d2_l), vget_low_s16(d2_l));
      sum_vec2         = vmlal_s16(sum_vec2, vget_high_s16(d2_l), vget_high_s16(d2_l));
      sum_vec2         = vmlal_s16(sum_vec2, vget_low_s16(d2_h), vget_low_s16(d2_h));
      sum_vec2         = vmlal_s16(sum_vec2, vget_high_s16(d2_h), vget_high_s16(d2_h));

      a += 32;
      b += 32;
    }

    sum_vec1 = vaddq_s32(sum_vec1, sum_vec2);

    const unsigned char *lastgroup_16 = last - 15;
    while (a < lastgroup_16) {
      uint8x16_t va   = vld1q_u8(a);
      uint8x16_t vb   = vld1q_u8(b);
      uint16x8_t va_l = vmovl_u8(vget_low_u8(va));
      uint16x8_t va_h = vmovl_u8(vget_high_u8(va));
      uint16x8_t vb_l = vmovl_u8(vget_low_u8(vb));
      uint16x8_t vb_h = vmovl_u8(vget_high_u8(vb));
      int16x8_t d_l   = vreinterpretq_s16_u16(vsubq_u16(va_l, vb_l));
      int16x8_t d_h   = vreinterpretq_s16_u16(vsubq_u16(va_h, vb_h));
      sum_vec1        = vmlal_s16(sum_vec1, vget_low_s16(d_l), vget_low_s16(d_l));
      sum_vec1        = vmlal_s16(sum_vec1, vget_high_s16(d_l), vget_high_s16(d_l));
      sum_vec1        = vmlal_s16(sum_vec1, vget_low_s16(d_h), vget_low_s16(d_h));
      sum_vec1        = vmlal_s16(sum_vec1, vget_high_s16(d_h), vget_high_s16(d_h));
      a += 16;
      b += 16;
    }

    uint64_t sum = 0;
#if defined(__aarch64__)
    sum = static_cast<uint64_t>(vaddlvq_s32(sum_vec1));
#else
    int64x2_t temp_sum_pair = vpaddlq_s32(sum_vec1);
    sum = static_cast<uint64_t>(vgetq_lane_s64(temp_sum_pair, 0) + vgetq_lane_s64(temp_sum_pair, 1));
#endif

    while (a < last) {
      int diff = static_cast<int>(*a++) - static_cast<int>(*b++);
      sum += diff * diff;
    }
    return std::sqrt(static_cast<double>(sum));
  }

  inline static double compareL2(const quint8 *a, const quint8 *b, size_t size) {
    int64x2_t sum_vec = vdupq_n_s64(0);

    const quint8 *last      = a + size;
    const quint8 *lastgroup = last - 15;

    while (a < lastgroup) {
      uint8x16_t va = vld1q_u8(reinterpret_cast<const uint8_t *>(a));
      uint8x16_t vb = vld1q_u8(reinterpret_cast<const uint8_t *>(b));

      int16x8_t diff_low  = vreinterpretq_s16_u16(vsubl_u8(vget_low_u8(va), vget_low_u8(vb)));
      int16x8_t diff_high = vreinterpretq_s16_u16(vsubl_u8(vget_high_u8(va), vget_high_u8(vb)));

      int32x4_t sq_low_1  = vmull_s16(vget_low_s16(diff_low), vget_low_s16(diff_low));
      int32x4_t sq_low_2  = vmull_s16(vget_high_s16(diff_low), vget_high_s16(diff_low));
      int32x4_t sq_high_1 = vmull_s16(vget_low_s16(diff_high), vget_low_s16(diff_high));
      int32x4_t sq_high_2 = vmull_s16(vget_high_s16(diff_high), vget_high_s16(diff_high));

      sum_vec = vpadalq_s32(sum_vec, sq_low_1);
      sum_vec = vpadalq_s32(sum_vec, sq_low_2);
      sum_vec = vpadalq_s32(sum_vec, sq_high_1);
      sum_vec = vpadalq_s32(sum_vec, sq_high_2);

      a += 16;
      b += 16;
    }

    uint64_t sum = 0;
#if defined(__aarch64__)
    sum = static_cast<uint64_t>(vaddvq_s64(sum_vec));
#else
    sum = static_cast<uint64_t>(vgetq_lane_s64(sum_vec, 0) + vgetq_lane_s64(sum_vec, 1));
#endif

    while (a < last) {
      int diff = static_cast<int>(*a++) - static_cast<int>(*b++);
      sum += diff * diff;
    }

    return std::sqrt(static_cast<double>(sum));
  }

  inline static double compareL2(const qsint8 *a, const qsint8 *b, size_t size) {
    int32x4_t sum_vec = vdupq_n_s32(0);

    const qsint8 *last      = a + size;
    const qsint8 *lastgroup = last - 15;

    while (a < lastgroup) {
      int8x16_t va = vld1q_s8(reinterpret_cast<const int8_t *>(a));
      int8x16_t vb = vld1q_s8(reinterpret_cast<const int8_t *>(b));

      int16x8_t va_low  = vmovl_s8(vget_low_s8(va));
      int16x8_t va_high = vmovl_s8(vget_high_s8(va));
      int16x8_t vb_low  = vmovl_s8(vget_low_s8(vb));
      int16x8_t vb_high = vmovl_s8(vget_high_s8(vb));

      int16x8_t diff_low  = vsubq_s16(va_low, vb_low);
      int16x8_t diff_high = vsubq_s16(va_high, vb_high);

      sum_vec = vmlal_s16(sum_vec, vget_low_s16(diff_low), vget_low_s16(diff_low));
      sum_vec = vmlal_s16(sum_vec, vget_high_s16(diff_low), vget_high_s16(diff_low));
      sum_vec = vmlal_s16(sum_vec, vget_low_s16(diff_high), vget_low_s16(diff_high));
      sum_vec = vmlal_s16(sum_vec, vget_high_s16(diff_high), vget_high_s16(diff_high));

      a += 16;
      b += 16;
    }

    uint64_t sum = 0;
#if defined(__aarch64__)
    sum = vaddvq_s32(sum_vec);
#else
    uint64x2_t temp_sum_pair = vpaddlq_s32(sum_vec);
    sum                      = vgetq_lane_u64(temp_sum_pair, 0) + vgetq_lane_u64(temp_sum_pair, 1);
#endif

    while (a < last) {
      int diff = static_cast<int>(*a++) - static_cast<int>(*b++);
      sum += diff * diff;
    }

    return std::sqrt(static_cast<double>(sum));
  }

#ifdef NGT_PQ4
  static double compareL2(const qint4 *a, const qint4 *b, size_t size) {
    NGTThrowException("compareL2(qint4*, qint4*) is not supported.");
    return 0.0;
  }
#endif

  inline static double compareL2(const bfloat16_t *a, const bfloat16_t *b, size_t size) {
    float32x4_t sum_vec = vdupq_n_f32(0.0f);

    const bfloat16_t *last      = a + size;
    const bfloat16_t *lastgroup = last - 3;

    while (a < lastgroup) {
      float32x4_t va   = vcvt_f32_bf16(vld1_bf16(a));
      float32x4_t vb   = vcvt_f32_bf16(vld1_bf16(b));
      float32x4_t diff = vsubq_f32(va, vb);
      sum_vec          = vmlaq_f32(sum_vec, diff, diff);
      a += 4;
      b += 4;
    }

    float sum = vaddvq_f32(sum_vec);

    while (a < last) {
      float diff = static_cast<float>(*a++) - static_cast<float>(*b++);
      sum += diff * diff;
    }

    return std::sqrt(sum);
  }

  inline static double compareL2(const half_float::half *a, const half_float::half *b, size_t size) {
    double sum = 0.0;

    for (size_t i = 0; i < size; ++i) {
      float diff = static_cast<float>(a[i]) - static_cast<float>(b[i]);
      sum += diff * diff;
    }

    return std::sqrt(sum);
  }

  template <typename OBJECT_TYPE>
  inline static double compareNormalizedL2(const OBJECT_TYPE *a, const OBJECT_TYPE *b, size_t size) {
    double v = 2.0 - 2.0 * compareDotProduct(a, b, size);
    if (v < 0.0) {
      return 0.0;
    } else {
      return sqrt(v);
    }
  }

  template <typename OBJECT_TYPE, typename COMPARE_TYPE>
  static double compareL1(const OBJECT_TYPE *a, const OBJECT_TYPE *b, size_t size) {
    auto *last      = a + size;
    auto *lastgroup = last - 3;
    COMPARE_TYPE diff0, diff1, diff2, diff3;
    double d = 0.0;
    while (a < lastgroup) {
      diff0 = (COMPARE_TYPE)(a[0]) - b[0];
      diff1 = (COMPARE_TYPE)(a[1]) - b[1];
      diff2 = (COMPARE_TYPE)(a[2]) - b[2];
      diff3 = (COMPARE_TYPE)(a[3]) - b[3];
      d += absolute(diff0) + absolute(diff1) + absolute(diff2) + absolute(diff3);
      a += 4;
      b += 4;
    }
    while (a < last) {
      diff0 = (COMPARE_TYPE)*a++ - (COMPARE_TYPE)*b++;
      d += absolute(diff0);
    }
    return d;
  }

  inline static double compareL1(const uint8_t *a, const uint8_t *b, size_t size) {
    const uint8_t *last      = a + size;
    const uint8_t *lastgroup = last - 15;
    uint32x4_t sum_vec       = vdupq_n_u32(0);
    while (a < lastgroup) {
      uint8x16_t va     = vld1q_u8(a);
      uint8x16_t vb     = vld1q_u8(b);
      uint8x16_t diff   = vabdq_u8(va, vb);
      uint16x8_t diff16 = vpaddlq_u8(diff);
      uint32x4_t diff32 = vpaddlq_u16(diff16);
      sum_vec           = vaddq_u32(sum_vec, diff32);
      a += 16;
      b += 16;
    }
    uint64_t sum = horizontalAddU32(sum_vec);
    while (a < last) {
      int diff = static_cast<int>(*a++) - static_cast<int>(*b++);
      sum += static_cast<uint64_t>(diff < 0 ? -diff : diff);
    }
    return static_cast<double>(sum);
  }

  inline static double compareL1(const int8_t *a, const int8_t *b, size_t size) {
    const int8_t *last      = a + size;
    const int8_t *lastgroup = last - 15;
    int32x4_t sum_vec       = vdupq_n_s32(0);
    while (a < lastgroup) {
      int8x16_t va        = vld1q_s8(a);
      int8x16_t vb        = vld1q_s8(b);
      int16x8_t diff_low  = vsubl_s8(vget_low_s8(va), vget_low_s8(vb));
      int16x8_t diff_high = vsubl_s8(vget_high_s8(va), vget_high_s8(vb));
      int16x8_t abs_low   = vabsq_s16(diff_low);
      int16x8_t abs_high  = vabsq_s16(diff_high);
      sum_vec             = vpadalq_s16(sum_vec, abs_low);
      sum_vec             = vpadalq_s16(sum_vec, abs_high);
      a += 16;
      b += 16;
    }
    uint64_t sum = horizontalAddS32(sum_vec);
    while (a < last) {
      int diff = static_cast<int>(*a++) - static_cast<int>(*b++);
      sum += static_cast<uint64_t>(diff < 0 ? -diff : diff);
    }
    return static_cast<double>(sum);
  }
#ifdef NGT_PQ4
  inline static double compareL1(const qint4 *a, const qint4 *b, size_t size) {
    NGTThrowException("Not supported.");
  }
#endif

  inline static double compareL1(const float *a, const float *b, size_t size) {
    const float *last      = a + size;
    const float *lastgroup = last - 3;
    float32x4_t sum_vec    = vdupq_n_f32(0.0f);
    while (a < lastgroup) {
      float32x4_t va   = vld1q_f32(a);
      float32x4_t vb   = vld1q_f32(b);
      float32x4_t diff = vsubq_f32(va, vb);
      sum_vec          = vaddq_f32(sum_vec, vabsq_f32(diff));
      a += 4;
      b += 4;
    }
    double sum = horizontalAddF32(sum_vec);
    while (a < last) {
      sum += fabs(*a++ - *b++);
    }
    return sum;
  }
#ifdef NGT_HALF_FLOAT
  inline static double compareL1(const float16 *a, const float16 *b, size_t size) {
    const float16_t *pa        = reinterpret_cast<const float16_t *>(a);
    const float16_t *pb        = reinterpret_cast<const float16_t *>(b);
    const float16_t *last      = pa + size;
    const float16_t *lastgroup = last - 3;
    float32x4_t sum_vec        = vdupq_n_f32(0.0f);
    while (pa < lastgroup) {
      float32x4_t va   = vcvt_f32_f16(vld1_f16(pa));
      float32x4_t vb   = vcvt_f32_f16(vld1_f16(pb));
      float32x4_t diff = vsubq_f32(va, vb);
      sum_vec          = vaddq_f32(sum_vec, vabsq_f32(diff));
      pa += 4;
      pb += 4;
    }
    double sum = horizontalAddF32(sum_vec);
    while (pa < last) {
      float diff = static_cast<float>(*pa++) - static_cast<float>(*pb++);
      sum += fabs(diff);
    }
    return sum;
  }
#endif
#ifdef NGT_BFLOAT
  inline static double compareL1(const bfloat16 *a, const bfloat16 *b, size_t size) {
    return compareL1<bfloat16, float>(a, b, size);
  }
#endif
  inline static double compareL1(const quint8 *a, const quint8 *b, size_t size) {
    return compareL1(reinterpret_cast<const uint8_t *>(a), reinterpret_cast<const uint8_t *>(b), size);
  }
  inline static double compareL1(const qsint8 *a, const qsint8 *b, size_t size) {
    return compareL1(reinterpret_cast<const int8_t *>(a), reinterpret_cast<const int8_t *>(b), size);
  }

  inline static double popCount(uint32_t x) {
    x = (x & 0x55555555) + (x >> 1 & 0x55555555);
    x = (x & 0x33333333) + (x >> 2 & 0x33333333);
    x = (x & 0x0F0F0F0F) + (x >> 4 & 0x0F0F0F0F);
    x = (x & 0x00FF00FF) + (x >> 8 & 0x00FF00FF);
    x = (x & 0x0000FFFF) + (x >> 16 & 0x0000FFFF);
    return x;
  }

  template <typename OBJECT_TYPE>
  inline static double compareHammingDistance(const OBJECT_TYPE *a, const OBJECT_TYPE *b, size_t size) {
    const uint8_t *pa   = reinterpret_cast<const uint8_t *>(a);
    const uint8_t *pb   = reinterpret_cast<const uint8_t *>(b);
    size_t byteSize     = size * sizeof(OBJECT_TYPE);
    const uint8_t *last = pa + byteSize;

    uint32x4_t sum_vec = vdupq_n_u32(0);
    while (pa + 16 <= last) {
      uint8x16_t va    = vld1q_u8(pa);
      uint8x16_t vb    = vld1q_u8(pb);
      uint8x16_t vxor  = veorq_u8(va, vb);
      uint8x16_t cnt   = vcntq_u8(vxor);
      uint16x8_t cnt16 = vpaddlq_u8(cnt);
      uint32x4_t cnt32 = vpaddlq_u16(cnt16);
      sum_vec          = vaddq_u32(sum_vec, cnt32);
      pa += 16;
      pb += 16;
    }

    uint64_t count = horizontalAddU32(sum_vec);
    while (pa < last) {
      count += static_cast<uint64_t>(__builtin_popcount(static_cast<unsigned int>(*pa++ ^ *pb++)));
    }
    return static_cast<double>(count);
  }

  template <typename OBJECT_TYPE>
  inline static double compareJaccardDistance(const OBJECT_TYPE *a, const OBJECT_TYPE *b, size_t size) {
    const uint8_t *pa   = reinterpret_cast<const uint8_t *>(a);
    const uint8_t *pb   = reinterpret_cast<const uint8_t *>(b);
    size_t byteSize     = size * sizeof(OBJECT_TYPE);
    const uint8_t *last = pa + byteSize;

    uint32x4_t count_vec    = vdupq_n_u32(0);
    uint32x4_t count_de_vec = vdupq_n_u32(0);
    while (pa + 16 <= last) {
      uint8x16_t va        = vld1q_u8(pa);
      uint8x16_t vb        = vld1q_u8(pb);
      uint8x16_t vand      = vandq_u8(va, vb);
      uint8x16_t vor       = vorrq_u8(va, vb);
      uint8x16_t cnt_and   = vcntq_u8(vand);
      uint8x16_t cnt_or    = vcntq_u8(vor);
      uint16x8_t cnt_and16 = vpaddlq_u8(cnt_and);
      uint16x8_t cnt_or16  = vpaddlq_u8(cnt_or);
      uint32x4_t cnt_and32 = vpaddlq_u16(cnt_and16);
      uint32x4_t cnt_or32  = vpaddlq_u16(cnt_or16);
      count_vec            = vaddq_u32(count_vec, cnt_and32);
      count_de_vec         = vaddq_u32(count_de_vec, cnt_or32);
      pa += 16;
      pb += 16;
    }

    uint64_t count   = horizontalAddU32(count_vec);
    uint64_t countDe = horizontalAddU32(count_de_vec);
    while (pa < last) {
      count += static_cast<uint64_t>(__builtin_popcount(static_cast<unsigned int>(*pa & *pb)));
      countDe += static_cast<uint64_t>(__builtin_popcount(static_cast<unsigned int>(*pa | *pb)));
      ++pa;
      ++pb;
    }

    return 1.0 - static_cast<double>(count) / static_cast<double>(countDe);
  }

  inline static double compareSparseJaccardDistance(const unsigned char *a, const unsigned char *b,
                                                    size_t size) {
    std::cerr << "compareSparseJaccardDistance: Not implemented." << std::endl;
    abort();
  }

#ifdef NGT_HALF_FLOAT
  inline static double compareSparseJaccardDistance(const float16 *a, const float16 *b, size_t size) {
    std::cerr << "compareSparseJaccardDistance: Not implemented." << std::endl;
    abort();
  }
#endif

#ifdef NGT_BFLOAT
  inline static double compareSparseJaccardDistance(const bfloat16 *a, const bfloat16 *b, size_t size) {
    std::cerr << "compareSparseJaccardDistance: Not implemented." << std::endl;
    abort();
  }
#endif

  inline static double compareSparseJaccardDistance(const qsint8 *a, const qsint8 *b, size_t size) {
    NGTThrowException("Not supported.");
  }
#ifdef NGT_PQ4
  inline static double compareSparseJaccardDistance(const qint4 *a, const qint4 *b, size_t size) {
    NGTThrowException("Not supported.");
  }
#endif
  inline static double compareSparseJaccardDistance(const float *a, const float *b, size_t size) {
    size_t loca        = 0;
    size_t locb        = 0;
    const uint32_t *ai = reinterpret_cast<const uint32_t *>(a);
    const uint32_t *bi = reinterpret_cast<const uint32_t *>(b);
    size_t count       = 0;
    while (locb < size && ai[loca] != 0 && bi[loca] != 0) {
      if (loca + 3 < size && locb + 3 < size) {
        uint32x4_t ai_block = vld1q_u32(ai + loca);
        uint32x4_t bi_block = vld1q_u32(bi + locb);
        if (!anyZeroU32(ai_block) && !anyZeroU32(bi_block)) {
          uint32_t max_ai = horizontalMaxU32(ai_block);
          uint32_t max_bi = horizontalMaxU32(bi_block);
          if (max_ai < bi[locb]) {
            loca += 4;
            continue;
          }
          if (max_bi < ai[loca]) {
            locb += 4;
            continue;
          }
        }
      }
      int64_t sub = static_cast<int64_t>(ai[loca]) - static_cast<int64_t>(bi[locb]);
      count += sub == 0;
      loca += sub <= 0;
      locb += sub >= 0;
    }
    while (loca + 3 < size) {
      uint32x4_t block = vld1q_u32(ai + loca);
      if (anyZeroU32(block)) {
        break;
      }
      loca += 4;
    }
    while (ai[loca] != 0) {
      loca++;
    }
    while (locb + 3 < size) {
      uint32x4_t block = vld1q_u32(bi + locb);
      if (anyZeroU32(block)) {
        break;
      }
      locb += 4;
    }
    while (locb < size && bi[locb] != 0) {
      locb++;
    }
    return 1.0 - static_cast<double>(count) / static_cast<double>(loca + locb - count);
  }

  inline static double compareDotProduct(const float *a, const float *b, size_t size) {
    if (size == 0) return 0.0;
    double sum          = 0.0;
    size_t alignedSize  = size - (size % 4);
    float32x4_t sum_vec = vdupq_n_f32(0.0f);

    for (size_t i = 0; i < alignedSize; i += 4) {
      float32x4_t v1_vec = vld1q_f32(a + i);
      float32x4_t v2_vec = vld1q_f32(b + i);
      sum_vec            = vmlaq_f32(sum_vec, v1_vec, v2_vec);
    }

#if defined(__aarch64__)
    sum = static_cast<double>(vaddvq_f32(sum_vec));
#else
    float32x2_t temp_sum = vpadd_f32(vget_low_f32(sum_vec), vget_high_f32(sum_vec));
    temp_sum             = vpadd_f32(temp_sum, temp_sum);
    sum                  = static_cast<double>(vget_lane_f32(temp_sum, 0));
#endif

    for (size_t i = alignedSize; i < size; ++i) {
      sum += static_cast<double>(a[i]) * static_cast<double>(b[i]);
    }
    return sum;
  }

  inline static double compareDotProduct(const float16 *a, const float16 *b, size_t size) {
    if (size == 0) return 0.0;
    double sum              = 0.0;
    size_t alignedSize      = size - (size % 4);
    float32x4_t sum_vec_f32 = vdupq_n_f32(0.0f);

    const float16_t *v1_f16p = reinterpret_cast<const float16_t *>(a);
    const float16_t *v2_f16p = reinterpret_cast<const float16_t *>(b);

    for (size_t i = 0; i < alignedSize; i += 4) {
      float16x4_t v1_f16_vec = vld1_f16(v1_f16p + i);
      float16x4_t v2_f16_vec = vld1_f16(v2_f16p + i);

      float32x4_t v1_f32_vec = vcvt_f32_f16(v1_f16_vec);
      float32x4_t v2_f32_vec = vcvt_f32_f16(v2_f16_vec);

      sum_vec_f32 = vmlaq_f32(sum_vec_f32, v1_f32_vec, v2_f32_vec);
    }

#if defined(__aarch64__)
    sum = static_cast<double>(vaddvq_f32(sum_vec_f32));
#else
    float32x2_t temp_sum_pair = vpadd_f32(vget_low_f32(sum_vec_f32), vget_high_f32(sum_vec_f32));
    temp_sum_pair             = vpadd_f32(temp_sum_pair, temp_sum_pair);
    sum                       = static_cast<double>(vget_lane_f32(temp_sum_pair, 0));
#endif

    for (size_t i = alignedSize; i < size; ++i) {
      sum += static_cast<double>(static_cast<float>(a[i])) * static_cast<double>(static_cast<float>(b[i]));
    }
    return sum;
  }

#ifdef NGT_BFLOAT
  inline static double compareDotProduct(const bfloat16 *a, const bfloat16 *b, size_t size) { abort(); }
#endif

  inline static double compareDotProduct(const uint8_t *a, const uint8_t *b, size_t size) {
    if (size == 0) return 0.0;
    double sum_d           = 0.0;
    size_t alignedSize     = size - (size % 16);
    uint32x4_t sum_vec_u32 = vdupq_n_u32(0);

    for (size_t i = 0; i < alignedSize; i += 16) {
      uint8x16_t v1_u8x16 = vld1q_u8(a + i);
      uint8x16_t v2_u8x16 = vld1q_u8(b + i);

      uint16x8_t v1_u16_low  = vmovl_u8(vget_low_u8(v1_u8x16));
      uint16x8_t v1_u16_high = vmovl_u8(vget_high_u8(v1_u8x16));
      uint16x8_t v2_u16_low  = vmovl_u8(vget_low_u8(v2_u8x16));
      uint16x8_t v2_u16_high = vmovl_u8(vget_high_u8(v2_u8x16));

      sum_vec_u32 = vmlal_u16(sum_vec_u32, vget_low_u16(v1_u16_low), vget_low_u16(v2_u16_low));
      sum_vec_u32 = vmlal_u16(sum_vec_u32, vget_high_u16(v1_u16_low), vget_high_u16(v2_u16_low));
      sum_vec_u32 = vmlal_u16(sum_vec_u32, vget_low_u16(v1_u16_high), vget_low_u16(v2_u16_high));
      sum_vec_u32 = vmlal_u16(sum_vec_u32, vget_high_u16(v1_u16_high), vget_high_u16(v2_u16_high));
    }

    uint64_t scalar_sum_u64 = 0;
#if defined(__aarch64__)
    scalar_sum_u64 = vaddvq_u32(sum_vec_u32);
#else
    uint64x2_t temp_sum_pair = vpaddlq_u32(sum_vec_u32);
    scalar_sum_u64           = vgetq_lane_u64(temp_sum_pair, 0) + vgetq_lane_u64(temp_sum_pair, 1);
#endif
    sum_d = static_cast<double>(scalar_sum_u64);

    for (size_t i = alignedSize; i < size; ++i) {
      sum_d +=
          static_cast<double>(static_cast<uint32_t>(a[i])) * static_cast<double>(static_cast<uint32_t>(b[i]));
    }
    return sum_d;
  }

  inline static double compareDotProduct(const int8_t *a, const int8_t *b, size_t size) {
    if (size == 0) return 0.0;
    double sum_d          = 0.0;
    size_t alignedSize    = size - (size % 16);
    int32x4_t sum_vec_s32 = vdupq_n_s32(0);

    for (size_t i = 0; i < alignedSize; i += 16) {
      int8x16_t v1_s8x16 = vld1q_s8(a + i);
      int8x16_t v2_s8x16 = vld1q_s8(b + i);

      int16x8_t v1_s16_low  = vmovl_s8(vget_low_s8(v1_s8x16));
      int16x8_t v1_s16_high = vmovl_s8(vget_high_s8(v1_s8x16));
      int16x8_t v2_s16_low  = vmovl_s8(vget_low_s8(v2_s8x16));
      int16x8_t v2_s16_high = vmovl_s8(vget_high_s8(v2_s8x16));

      sum_vec_s32 = vmlal_s16(sum_vec_s32, vget_low_s16(v1_s16_low), vget_low_s16(v2_s16_low));
      sum_vec_s32 = vmlal_s16(sum_vec_s32, vget_high_s16(v1_s16_low), vget_high_s16(v2_s16_low));
      sum_vec_s32 = vmlal_s16(sum_vec_s32, vget_low_s16(v1_s16_high), vget_low_s16(v2_s16_high));
      sum_vec_s32 = vmlal_s16(sum_vec_s32, vget_high_s16(v1_s16_high), vget_high_s16(v2_s16_high));
    }

    int64_t scalar_sum_s64 = 0;
#if defined(__aarch64__)
    scalar_sum_s64 = vaddvq_s32(sum_vec_s32);
#else
    int64x2_t temp_sum_pair = vpaddlq_s32(sum_vec_s32);
    scalar_sum_s64          = vgetq_lane_s64(temp_sum_pair, 0) + vgetq_lane_s64(temp_sum_pair, 1);
#endif
    sum_d = static_cast<double>(scalar_sum_s64);

    for (size_t i = alignedSize; i < size; ++i) {
      sum_d +=
          static_cast<double>(static_cast<int32_t>(a[i])) * static_cast<double>(static_cast<int32_t>(b[i]));
    }
    return sum_d;
  }

  inline static double compareDotProduct(const int8_t *a, const uint8_t *b, size_t size) {
    if (size == 0) return 0.0;
    double sum_d          = 0.0;
    size_t alignedSize    = size - (size % 16);
    int32x4_t sum_vec_s32 = vdupq_n_s32(0);

    for (size_t i = 0; i < alignedSize; i += 16) {
      int8x16_t v1_s8x16  = vld1q_s8(a + i);
      uint8x16_t v2_u8x16 = vld1q_u8(b + i);

      int16x8_t v1_s16_low  = vmovl_s8(vget_low_s8(v1_s8x16));
      int16x8_t v1_s16_high = vmovl_s8(vget_high_s8(v1_s8x16));

      uint16x8_t v2_u16_low_temp  = vmovl_u8(vget_low_u8(v2_u8x16));
      uint16x8_t v2_u16_high_temp = vmovl_u8(vget_high_u8(v2_u8x16));
      int16x8_t v2_s16_low        = vreinterpretq_s16_u16(v2_u16_low_temp);
      int16x8_t v2_s16_high       = vreinterpretq_s16_u16(v2_u16_high_temp);

      sum_vec_s32 = vmlal_s16(sum_vec_s32, vget_low_s16(v1_s16_low), vget_low_s16(v2_s16_low));
      sum_vec_s32 = vmlal_s16(sum_vec_s32, vget_high_s16(v1_s16_low), vget_high_s16(v2_s16_low));
      sum_vec_s32 = vmlal_s16(sum_vec_s32, vget_low_s16(v1_s16_high), vget_low_s16(v2_s16_high));
      sum_vec_s32 = vmlal_s16(sum_vec_s32, vget_high_s16(v1_s16_high), vget_high_s16(v2_s16_high));
    }

    int64_t scalar_sum_s64 = 0;
#if defined(__aarch64__)
    scalar_sum_s64 = vaddvq_s32(sum_vec_s32);
#else
    int64x2_t temp_sum_pair = vpaddlq_s32(sum_vec_s32);
    scalar_sum_s64          = vgetq_lane_s64(temp_sum_pair, 0) + vgetq_lane_s64(temp_sum_pair, 1);
#endif
    sum_d = static_cast<double>(scalar_sum_s64);

    for (size_t i = alignedSize; i < size; ++i) {
      sum_d +=
          static_cast<double>(static_cast<int32_t>(a[i])) * static_cast<double>(static_cast<int32_t>(b[i]));
    }
    return sum_d;
  }

  inline static double compareDotProduct(const quint8 *a, const quint8 *b, size_t size) {
    return compareDotProduct(reinterpret_cast<const uint8_t *>(a), reinterpret_cast<const uint8_t *>(b),
                             size);
  }
  inline static double compareDotProduct(const qsint8 *a, const qsint8 *b, size_t size) {
    auto d =
        compareDotProduct(reinterpret_cast<const int8_t *>(a), reinterpret_cast<const int8_t *>(b), size);
    return d;
  }
  inline static double compareDotProduct(const qsint8 *a, const quint8 *b, size_t size) {
    return compareDotProduct(reinterpret_cast<const int8_t *>(a), reinterpret_cast<const uint8_t *>(b), size);
  }
#ifdef NGT_PQ4
  static double compareDotProduct(const qint4 *a, const qint4 *b, size_t size) {
    NGTThrowException("compareDotProduct(qint4*, qint4*) is not supported.");
    return 0.0;
  }
#endif

  inline static double compareCosine(const float *a, const float *b, size_t size) {
    const float *last      = a + size;
    const float *lastgroup = last - 3;
    float32x4_t normA      = vdupq_n_f32(0.0f);
    float32x4_t normB      = vdupq_n_f32(0.0f);
    float32x4_t sum        = vdupq_n_f32(0.0f);

    while (a < lastgroup) {
      float32x4_t va = vld1q_f32(a);
      float32x4_t vb = vld1q_f32(b);
      normA          = vmlaq_f32(normA, va, va);
      normB          = vmlaq_f32(normB, vb, vb);
      sum            = vmlaq_f32(sum, va, vb);
      a += 4;
      b += 4;
    }

    double na = horizontalAddF32(normA);
    double nb = horizontalAddF32(normB);
    double s  = horizontalAddF32(sum);

    while (a < last) {
      double va = static_cast<double>(*a++);
      double vb = static_cast<double>(*b++);
      na += va * va;
      nb += vb * vb;
      s += va * vb;
    }
    return s / sqrt(na * nb);
  }

#ifdef NGT_HALF_FLOAT
  inline static double compareCosine(const float16 *a, const float16 *b, size_t size) {
    const float16_t *pa        = reinterpret_cast<const float16_t *>(a);
    const float16_t *pb        = reinterpret_cast<const float16_t *>(b);
    const float16_t *last      = pa + size;
    const float16_t *lastgroup = last - 3;
    float32x4_t normA          = vdupq_n_f32(0.0f);
    float32x4_t normB          = vdupq_n_f32(0.0f);
    float32x4_t sum            = vdupq_n_f32(0.0f);

    while (pa < lastgroup) {
      float32x4_t va = vcvt_f32_f16(vld1_f16(pa));
      float32x4_t vb = vcvt_f32_f16(vld1_f16(pb));
      normA          = vmlaq_f32(normA, va, va);
      normB          = vmlaq_f32(normB, vb, vb);
      sum            = vmlaq_f32(sum, va, vb);
      pa += 4;
      pb += 4;
    }

    double na = horizontalAddF32(normA);
    double nb = horizontalAddF32(normB);
    double s  = horizontalAddF32(sum);

    while (pa < last) {
      double va = static_cast<double>(static_cast<float>(*pa++));
      double vb = static_cast<double>(static_cast<float>(*pb++));
      na += va * va;
      nb += vb * vb;
      s += va * vb;
    }
    return s / sqrt(na * nb);
  }
#endif

  inline static double compareCosine(const uint8_t *a, const uint8_t *b, size_t size) {
    double na = compareDotProduct(a, a, size);
    double nb = compareDotProduct(b, b, size);
    double s  = compareDotProduct(a, b, size);
    return s / sqrt(na * nb);
  }

  inline static double compareCosine(const int8_t *a, const int8_t *b, size_t size) {
    double na = compareDotProduct(a, a, size);
    double nb = compareDotProduct(b, b, size);
    double s  = compareDotProduct(a, b, size);
    return s / sqrt(na * nb);
  }

  inline static double compareCosine(const quint8 *a, const quint8 *b, size_t size) {
    double na = compareDotProduct(a, a, size);
    double nb = compareDotProduct(b, b, size);
    double s  = compareDotProduct(a, b, size);
    return s / sqrt(na * nb);
  }

  inline static double compareCosine(const qsint8 *a, const qsint8 *b, size_t size) {
    double na = compareDotProduct(a, a, size);
    double nb = compareDotProduct(b, b, size);
    double s  = compareDotProduct(a, b, size);
    return s / sqrt(na * nb);
  }

  template <typename OBJECT_TYPE>
  inline static double compareCosine(const OBJECT_TYPE *a, const OBJECT_TYPE *b, size_t size) {
    double normA = 0.0;
    double normB = 0.0;
    double sum   = 0.0;
    for (size_t loc = 0; loc < size; loc++) {
      normA += static_cast<double>(a[loc]) * static_cast<double>(a[loc]);
      normB += static_cast<double>(b[loc]) * static_cast<double>(b[loc]);
      sum += static_cast<double>(a[loc]) * static_cast<double>(b[loc]);
    }

    double cosine = sum / sqrt(normA * normB);

    return cosine;
  }

  inline static double compareNormalizedCosineSimilarity(const float *a, const float *b, size_t size) {
    auto v = 1.0 - compareDotProduct(a, b, size);
    return v < 0.0 ? -v : v;
  }
  inline static double compareNormalizedCosineSimilarity(const float16 *a, const float16 *b, size_t size) {
    auto v = 1.0 - compareDotProduct(a, b, size);
    return v < 0.0 ? -v : v;
  }
#ifdef NGT_BFLOAT
  inline static double compareNormalizedCosineSimilarity(const bfloat16 *a, const bfloat16 *b, size_t size) {
    auto v = 1.0 - compareDotProduct(a, b, size);
    return v < 0.0 ? -v : v;
  }
#endif
  inline static double compareNormalizedCosineSimilarity(const uint8_t *a, const uint8_t *b, size_t size) {
    auto v = 1.0 - compareDotProduct(a, b, size);
    return v < 0.0 ? -v : v;
  }
  inline static double compareNormalizedCosineSimilarity(const qsint8 *a, const qsint8 *b, size_t size) {
    float max = 127.0 * 255.0 * size;
    auto v    = max - compareDotProduct(a, b, size);
    return v;
  }
  inline static double compareNormalizedCosineSimilarity(const quint8 *a, const quint8 *b, size_t size) {
    float max = 255.0 * 255.0 * size;
    auto v    = max - compareDotProduct(a, b, size);
    return v;
  }
#ifdef NGT_PQ4
  inline static double compareNormalizedCosineSimilarity(const qint4 *a, const qint4 *b, size_t size) {
    auto v = 1.0 - compareDotProduct(a, b, size);
    v      = v < 0.0 ? 0 : v;
    return v;
  }
#endif

  template <typename OBJECT_TYPE>
  inline static double compareAngleDistance(const OBJECT_TYPE *a, const OBJECT_TYPE *b, size_t size) {
    double cosine = compareCosine(a, b, size);
    if (cosine >= 1.0) {
      return 0.0;
    } else if (cosine <= -1.0) {
      return acos(-1.0);
    } else {
      return acos(cosine);
    }
  }

  template <typename OBJECT_TYPE>
  inline static double compareNormalizedAngleDistance(const OBJECT_TYPE *a, const OBJECT_TYPE *b,
                                                      size_t size) {
    double cosine = compareDotProduct(a, b, size);
    if (cosine >= 1.0) {
      return 0.0;
    } else if (cosine <= -1.0) {
      return acos(-1.0);
    } else {
      return acos(cosine);
    }
  }

  // added by Nyapicom
  inline static double comparePoincareDistance(const float *a, const float *b, size_t size) {
    double c2 = compareL2(a, b, size);
    double a2 = compareDotProduct(a, a, size);
    double b2 = compareDotProduct(b, b, size);
    return std::acosh(1 + 2.0 * c2 * c2 / (1.0 - a2) / (1.0 - b2));
  }
#ifdef NGT_HALF_FLOAT
  inline static double comparePoincareDistance(const float16 *a, const float16 *b, size_t size) {
    double c2 = compareL2(a, b, size);
    double a2 = compareDotProduct(a, a, size);
    double b2 = compareDotProduct(b, b, size);
    return std::acosh(1 + 2.0 * c2 * c2 / (1.0 - a2) / (1.0 - b2));
  }
#endif
  template <typename OBJECT_TYPE>
  inline static double comparePoincareDistance(const OBJECT_TYPE *a, const OBJECT_TYPE *b, size_t size) {
    // Unlike the other distance functions, this is not optimized...
    double a2 = 0.0;
    double b2 = 0.0;
    double c2 = compareL2(a, b, size);
    for (size_t i = 0; i < size; i++) {
      a2 += static_cast<double>(a[i]) * static_cast<double>(a[i]);
      b2 += static_cast<double>(b[i]) * static_cast<double>(b[i]);
    }
    return std::acosh(1 + 2.0 * c2 * c2 / (1.0 - a2) / (1.0 - b2));
  }

  // added by Nyapicom
  inline static double compareLorentzDistance(const float *a, const float *b, size_t size) {
    double dot = compareDotProduct(a, b, size);
    double sum = 2.0 * static_cast<double>(a[0]) * static_cast<double>(b[0]) - dot;
    return std::acosh(sum);
  }
#ifdef NGT_HALF_FLOAT
  inline static double compareLorentzDistance(const float16 *a, const float16 *b, size_t size) {
    double dot = compareDotProduct(a, b, size);
    double sum =
        2.0 * static_cast<double>(static_cast<float>(a[0])) * static_cast<double>(static_cast<float>(b[0])) -
        dot;
    return std::acosh(sum);
  }
#endif
  template <typename OBJECT_TYPE>
  inline static double compareLorentzDistance(const OBJECT_TYPE *a, const OBJECT_TYPE *b, size_t size) {
    double sum = static_cast<double>(a[0]) * static_cast<double>(b[0]);
    for (size_t i = 1; i < size; i++) {
      sum -= static_cast<double>(a[i]) * static_cast<double>(b[i]);
    }
    return std::acosh(sum);
  }

  template <typename OBJECT_TYPE>
  inline static double compareCosineSimilarity(const OBJECT_TYPE *a, const OBJECT_TYPE *b, size_t size) {
    auto v = 1.0 - compareCosine(a, b, size);
    return v < 0.0 ? -v : v;
  }

#ifdef NGT_PQ4
  static double compareCosineSimilarity(const qint4 *a, const qint4 *b, size_t size) {
    NGTThrowException("Cosine similarity is not supported.");
  }
#endif

  class L1Uint8;
  class L2Uint8;
  class HammingUint8;
  class JaccardUint8;
  class SparseJaccardFloat;
  class L2Float;
  class NormalizedL2Float;
  class L1Float;
  class CosineSimilarityFloat;
  class NormalizedCosineSimilarityFloat;
  class AngleFloat;
  class NormalizedAngleFloat;
  class PoincareFloat;
  class LorentzFloat;
#ifdef NGT_HALF_FLOAT
  class SparseJaccardFloat16;
  class L2Float16;
  class NormalizedL2Float16;
  class L1Float16;
  class CosineSimilarityFloat16;
  class NormalizedCosineSimilarityFloat16;
  class AngleFloat16;
  class NormalizedAngleFloat16;
  class PoincareFloat16;
  class LorentzFloat16;
#endif
#ifdef NGT_BFLOAT
  class SparseJaccardBfloat16;
  class L2Bfloat16;
  class NormalizedL2Bfloat16;
  class L1Bfloat16;
  class CosineSimilarityBfloat16;
  class NormalizedCosineSimilarityBfloat16;
  class AngleBfloat16;
  class NormalizedAngleBfloat16;
  class PoincareBfloat16;
  class LorentzBfloat16;
#endif
#ifdef NGT_PQ4
  class L2Qint4;
  class CosineSimilarityQint4;
  class NormalizedCosineSimilarityQint4;
  class InnerProductQint4;
#endif
  class SparseJaccardQsint8;
  class L2Qsint8;
  class NormalizedL2Qsint8;
  class L1Qsint8;
  class CosineSimilarityQsint8;
  class AngleQsint8;
  class NormalizedAngleQsint8;
  class PoincareQsint8;
  class LorentzQsint8;
  class InnerProductQsint8;
  class NormalizedCosineSimilarityQuint8;
  class NormalizedCosineSimilarityQsint8;
};

} // namespace NGT
