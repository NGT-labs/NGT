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

#include <immintrin.h>

namespace NGT {

class MemoryCache {
 public:
  inline static void prefetch(unsigned char *ptr, const size_t byteSizeOfObject) {
    switch ((byteSizeOfObject - 1) >> 6) {
    default:
    case 28: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 27: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 26: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 25: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 24: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 23: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 22: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 21: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 20: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 19: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 18: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 17: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 16: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 15: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 14: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 13: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 12: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 11: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 10: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 9: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 8: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 7: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 6: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 5: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 4: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 3: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 2: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 1: _mm_prefetch(ptr, _MM_HINT_T0); ptr += 64;
    case 0:
      _mm_prefetch(ptr, _MM_HINT_T0);
      ptr += 64;
      break;
    }
  }
  inline static void *alignedAlloc(const size_t allocSize) {
#if defined(NGT_AVX512)
    size_t alignment = 64;
    uint64_t mask    = 0xFFFFFFFFFFFFFFC0;
#elif defined(NGT_AVX2)
    size_t alignment = 32;
    uint64_t mask    = 0xFFFFFFFFFFFFFFE0;
#else
    size_t alignment = 16;
    uint64_t mask    = 0xFFFFFFFFFFFFFFF0;
#endif
    uint8_t *p   = new uint8_t[allocSize + alignment];
    uint8_t *ptr = p + alignment;
    ptr          = reinterpret_cast<uint8_t *>((reinterpret_cast<uint64_t>(ptr) & mask));
    *p++         = 0xAB;
    while (p != ptr)
      *p++ = 0xCD;
    return ptr;
  }
  inline static void alignedFree(void *ptr) {
    uint8_t *p = static_cast<uint8_t *>(ptr);
    p--;
    while (*p == 0xCD)
      p--;
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

  inline static double compareL2(const float *a, const float *b, size_t size) {
    const float *last = a + size;
#if defined(NGT_AVX512)
    __m512 sum512 = _mm512_setzero_ps();
    while (a < last) {
      __m512 v = _mm512_sub_ps(_mm512_loadu_ps(a), _mm512_loadu_ps(b));
      sum512   = _mm512_fmadd_ps(v, v, sum512);
      a += 16;
      b += 16;
    }

    return sqrt(static_cast<double>(_mm512_reduce_add_ps(sum512)));
#else
#if defined(NGT_AVX2)
    __m256 sum256 = _mm256_setzero_ps();
    __m256 v;
    while (a < last) {
      v      = _mm256_sub_ps(_mm256_loadu_ps(a), _mm256_loadu_ps(b));
      sum256 = _mm256_fmadd_ps(v, v, sum256);
      a += 8;
      b += 8;
      v      = _mm256_sub_ps(_mm256_loadu_ps(a), _mm256_loadu_ps(b));
      sum256 = _mm256_fmadd_ps(v, v, sum256);
      a += 8;
      b += 8;
    }
    __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#else
    __m128 sum128 = _mm_setzero_ps();
    __m128 v;
    while (a < last) {
      v      = _mm_sub_ps(_mm_loadu_ps(a), _mm_loadu_ps(b));
      sum128 = _mm_add_ps(sum128, _mm_mul_ps(v, v));
      a += 4;
      b += 4;
      v      = _mm_sub_ps(_mm_loadu_ps(a), _mm_loadu_ps(b));
      sum128 = _mm_add_ps(sum128, _mm_mul_ps(v, v));
      a += 4;
      b += 4;
      v      = _mm_sub_ps(_mm_loadu_ps(a), _mm_loadu_ps(b));
      sum128 = _mm_add_ps(sum128, _mm_mul_ps(v, v));
      a += 4;
      b += 4;
      v      = _mm_sub_ps(_mm_loadu_ps(a), _mm_loadu_ps(b));
      sum128 = _mm_add_ps(sum128, _mm_mul_ps(v, v));
      a += 4;
      b += 4;
    }
#endif

    __attribute__((aligned(32))) float f[4];
    _mm_store_ps(f, sum128);

    double s = f[0] + f[1] + f[2] + f[3];
    return sqrt(s);
#endif
  }

#ifdef NGT_HALF_FLOAT
  inline static double compareL2(const float16 *a, const float16 *b, size_t size) {
    const float16 *last = a + size;
#if defined(NGT_AVX512)
    __m512 sum512 = _mm512_setzero_ps();
    while (a < last) {
      __m512 v = _mm512_sub_ps(_mm512_cvtph_ps(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(a))),
                               _mm512_cvtph_ps(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(b))));
      sum512   = _mm512_add_ps(sum512, _mm512_mul_ps(v, v));
      a += 16;
      b += 16;
    }

    __m256 sum256 = _mm256_add_ps(_mm512_extractf32x8_ps(sum512, 0), _mm512_extractf32x8_ps(sum512, 1));
    __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#elif defined(NGT_AVX2)
    __m256 sum256 = _mm256_setzero_ps();
    __m256 v;
    while (a < last) {
      v      = _mm256_sub_ps(_mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i *>(a))),
                             _mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i *>(b))));
      sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v, v));
      a += 8;
      b += 8;
      v      = _mm256_sub_ps(_mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i *>(a))),
                             _mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i *>(b))));
      sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v, v));
      a += 8;
      b += 8;
    }
    __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#else
    __m128 sum128 = _mm_setzero_ps();
    __m128 v;
    while (a < last) {
      __m128i va = _mm_load_si128(reinterpret_cast<const __m128i *>(a));
      __m128i vb = _mm_load_si128(reinterpret_cast<const __m128i *>(b));
      v          = _mm_sub_ps(_mm_cvtph_ps(va), _mm_cvtph_ps(vb));
      sum128     = _mm_add_ps(sum128, _mm_mul_ps(v, v));
      va         = _mm_srli_si128(va, 8);
      vb         = _mm_srli_si128(vb, 8);
      v          = _mm_sub_ps(_mm_cvtph_ps(va), _mm_cvtph_ps(vb));
      sum128     = _mm_add_ps(sum128, _mm_mul_ps(v, v));
      a += 8;
      b += 8;
    }
#endif
    __m128 tmp = _mm_hadd_ps(sum128, _mm_set1_ps(0));
    double s   = _mm_cvtss_f32(_mm_shuffle_ps(tmp, tmp, _MM_SHUFFLE(0, 0, 0, 0))) +
               _mm_cvtss_f32(_mm_shuffle_ps(tmp, tmp, _MM_SHUFFLE(0, 0, 0, 1)));
    return sqrt(s);
  }
#endif

#ifdef NGT_BFLOAT
  inline static double compareL2(const bfloat16 *a, const bfloat16 *b, size_t size) {
    const bfloat16 *last = a + size;
    __m512 sum512        = _mm512_setzero_ps();
    while (a < last) {
      __m512i av = _mm512_slli_epi32(
          _mm512_cvtepu16_epi32(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(a))), 16);
      __m512i bv = _mm512_slli_epi32(
          _mm512_cvtepu16_epi32(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(b))), 16);
      __m512 sub = _mm512_sub_ps(reinterpret_cast<__m512>(av), reinterpret_cast<__m512>(bv));
      sum512     = _mm512_fmadd_ps(sub, sub, sum512);
      a += 16;
      b += 16;
    }
    __m256 sum256 = _mm256_add_ps(_mm512_extractf32x8_ps(sum512, 0), _mm512_extractf32x8_ps(sum512, 1));
    __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
    __m128 tmp    = _mm_hadd_ps(sum128, _mm_set1_ps(0));
    double d      = _mm_cvtss_f32(_mm_shuffle_ps(tmp, tmp, _MM_SHUFFLE(0, 0, 0, 0))) +
               _mm_cvtss_f32(_mm_shuffle_ps(tmp, tmp, _MM_SHUFFLE(0, 0, 0, 1)));
    return d;
  }
#endif

  inline static double compareL2(const unsigned char *a, const unsigned char *b, size_t size) {
#define COMPARATOR_L2_AVX512_UINT8
#if defined(NGT_AVX512) && defined(COMPARATOR_L2_AVX512_UINT8)
    // AVX-512 optimized version: 64 bytes per iteration, integer arithmetic only
    const unsigned char *last = a + size;
    __m512i sum512            = _mm512_setzero_si512();
    const __m512i mask        = _mm512_set1_epi16(0x00FF);

    // 64-byte loop
    while (a + 64 <= last) {
      __m512i raw_a = _mm512_loadu_si512(a);
      __m512i raw_b = _mm512_loadu_si512(b);

      __m512i a_lo = _mm512_and_si512(raw_a, mask);
      __m512i b_lo = _mm512_and_si512(raw_b, mask);
      __m512i a_hi = _mm512_srli_epi16(raw_a, 8);
      __m512i b_hi = _mm512_srli_epi16(raw_b, 8);

      __m512i diff_lo = _mm512_sub_epi16(a_lo, b_lo);
      __m512i diff_hi = _mm512_sub_epi16(a_hi, b_hi);

      __m512i sq_lo = _mm512_madd_epi16(diff_lo, diff_lo);
      __m512i sq_hi = _mm512_madd_epi16(diff_hi, diff_hi);

      sum512 = _mm512_add_epi32(sum512, sq_lo);
      sum512 = _mm512_add_epi32(sum512, sq_hi);

      a += 64;
      b += 64;
    }

    // 32-byte processing for remainder
    if (a + 32 <= last) {
      __m256i raw_a         = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a));
      __m256i raw_b         = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(b));
      const __m256i mask256 = _mm256_set1_epi16(0x00FF);

      __m256i a_lo = _mm256_and_si256(raw_a, mask256);
      __m256i b_lo = _mm256_and_si256(raw_b, mask256);
      __m256i a_hi = _mm256_srli_epi16(raw_a, 8);
      __m256i b_hi = _mm256_srli_epi16(raw_b, 8);

      __m256i diff_lo = _mm256_sub_epi16(a_lo, b_lo);
      __m256i diff_hi = _mm256_sub_epi16(a_hi, b_hi);
      __m256i sq_lo   = _mm256_madd_epi16(diff_lo, diff_lo);
      __m256i sq_hi   = _mm256_madd_epi16(diff_hi, diff_hi);

      __m256i sq_sum = _mm256_add_epi32(sq_lo, sq_hi);
      sum512         = _mm512_add_epi32(sum512, _mm512_zextsi256_si512(sq_sum));

      a += 32;
      b += 32;
    }

    int32_t sum = _mm512_reduce_add_epi32(sum512);

    while (a < last) {
      int d = (int)*a++ - (int)*b++;
      sum += d * d;
    }

    double s = (double)sum;
#else
    __m128 sum                     = _mm_setzero_ps();
    const unsigned char *last      = a + size;
    const unsigned char *lastgroup = last - 7;
    const __m128i zero             = _mm_setzero_si128();
    while (a < lastgroup) {
      __m128i x1 = _mm_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)a));
      __m128i x2 = _mm_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)b));
      x1         = _mm_subs_epi16(x1, x2);
      __m128i v  = _mm_mullo_epi16(x1, x1);
      sum        = _mm_add_ps(sum, _mm_cvtepi32_ps(_mm_unpacklo_epi16(v, zero)));
      sum        = _mm_add_ps(sum, _mm_cvtepi32_ps(_mm_unpackhi_epi16(v, zero)));
      a += 8;
      b += 8;
    }
    __attribute__((aligned(32))) float f[4];
    _mm_store_ps(f, sum);
    double s = f[0] + f[1] + f[2] + f[3];
    while (a < last) {
      int d = (int)*a++ - (int)*b++;
      s += d * d;
    }
#endif
    return sqrt(s);
  }

  inline static double compareL2(const quint8 *a, const quint8 *b, size_t size) {

    auto *u8a = reinterpret_cast<const uint8_t *>(a);
    auto *u8b = reinterpret_cast<const uint8_t *>(b);

    const unsigned char *last = u8a + size;
#if defined(NGT_AVX512)
    __m512i sum512 = _mm512_setzero_si512();
    {
      const unsigned char *lastgroup = last - 63;
      while (u8a < lastgroup) {
        __m512i mu8a = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(u8a));
        __m512i mu8b = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(u8b));
        __mmask64 m  = _mm512_cmplt_epu8_mask(mu8a, mu8b);
        __m512i x =
            _mm512_add_epi8(_mm512_maskz_subs_epu8(m, mu8b, mu8a), _mm512_maskz_subs_epu8(~m, mu8a, mu8b));
        __m512i xi16 = _mm512_cvtepu8_epi16(_mm512_extracti32x8_epi32(x, 0));
        sum512       = _mm512_add_epi32(sum512, _mm512_madd_epi16(xi16, xi16));
        xi16         = _mm512_cvtepu8_epi16(_mm512_extracti32x8_epi32(x, 1));
        sum512       = _mm512_add_epi32(sum512, _mm512_madd_epi16(xi16, xi16));
        u8a += 64;
        u8b += 64;
      }
    }
    {
      const unsigned char *lastgroup = last - 31;
      while (u8a < lastgroup) {
        __m256i mu8a = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(u8a));
        __m256i mu8b = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(u8b));
        __mmask32 m  = _mm256_cmplt_epu8_mask(mu8a, mu8b);
        __m256i x =
            _mm256_add_epi8(_mm256_maskz_subs_epu8(m, mu8b, mu8a), _mm256_maskz_subs_epu8(~m, mu8a, mu8b));
        __m512i xi16 = _mm512_cvtepu8_epi16(x);
        sum512       = _mm512_add_epi32(sum512, _mm512_madd_epi16(xi16, xi16));
        u8a += 32;
        u8b += 32;
      }
    }
    __m256i sum256 =
        _mm256_add_epi32(_mm512_extracti32x8_epi32(sum512, 0), _mm512_extracti32x8_epi32(sum512, 1));
#elif defined(NGT_AVX2)
    __m256i sum256 = _mm256_setzero_si256();
    {
      const unsigned char *lastgroup = last - 31;
      while (u8a < lastgroup) {
        __m256i x1   = _mm256_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)u8a));
        __m256i x2   = _mm256_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)u8b));
        __m256i xi16 = _mm256_subs_epi16(x1, x2);
        sum256       = _mm256_add_epi32(sum256, _mm256_madd_epi16(xi16, xi16));
        u8a += 16;
        u8b += 16;
        x1     = _mm256_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)u8a));
        x2     = _mm256_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)u8b));
        xi16   = _mm256_subs_epi16(x1, x2);
        sum256 = _mm256_add_epi32(sum256, _mm256_madd_epi16(xi16, xi16));
        u8a += 16;
        u8b += 16;
      }
    }
#else
    __m256i sum256 = _mm256_setzero_si256();
#endif
    {
      const unsigned char *lastgroup = last - 15;

      while (u8a < lastgroup) {
        __m256i x1   = _mm256_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)u8a));
        __m256i x2   = _mm256_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)u8b));
        __m256i xi16 = _mm256_subs_epi16(x1, x2);
        sum256       = _mm256_add_epi32(sum256, _mm256_madd_epi16(xi16, xi16));
        u8a += 16;
        u8b += 16;
      }
    }

    const __m256i value0 = _mm256_set1_epi32(0);
    __m256i tmp1         = _mm256_hadd_epi32(sum256, value0);
    __m256i tmp2         = _mm256_hadd_epi32(tmp1, value0);
    double s             = _mm256_extract_epi32(tmp2, 0) + _mm256_extract_epi32(tmp2, 4);
    return s;
  }

  inline static double compareL2(const qsint8 *a, const qsint8 *b, size_t size) {

    auto *i8a        = reinterpret_cast<const int8_t *>(a);
    auto *i8b        = reinterpret_cast<const int8_t *>(b);
    const auto *last = i8a + size;

#if defined(__AVX512BW__)
    __m512i sum512 = _mm512_setzero_si512();
    {
      const auto *lastgroup = last - 63;
      while (i8a < lastgroup) {
        __m512i mi8a = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(i8a));
        __m512i mi8b = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(i8b));
        __mmask64 m  = _mm512_cmplt_epi8_mask(mi8a, mi8b);
        __m512i x =
            _mm512_add_epi8(_mm512_maskz_subs_epi8(m, mi8b, mi8a), _mm512_maskz_sub_epi8(~m, mi8a, mi8b));
        __m512i xi16 = _mm512_cvtepu8_epi16(_mm512_extracti32x8_epi32(x, 0));
        sum512       = _mm512_add_epi32(sum512, _mm512_madd_epi16(xi16, xi16));
        xi16         = _mm512_cvtepu8_epi16(_mm512_extracti32x8_epi32(x, 1));
        sum512       = _mm512_add_epi32(sum512, _mm512_madd_epi16(xi16, xi16));
        i8a += 64;
        i8b += 64;
      }
    }
    {
      const auto *lastgroup = last - 31;
      while (i8a < lastgroup) {
#if defined(__AVX512VL__)
        __m256i mi8a = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(i8a));
        __m256i mi8b = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(i8b));
        __mmask32 m  = _mm256_cmplt_epi8_mask(mi8a, mi8b);
        __m256i x =
            _mm256_add_epi8(_mm256_maskz_sub_epi8(m, mi8b, mi8a), _mm256_maskz_sub_epi8(~m, mi8a, mi8b));
        __m512i xi16 = _mm512_cvtepu8_epi16(x);
#else
        __m512i x1   = _mm512_cvtepi8_epi16(_mm256_loadu_si256(reinterpret_cast<__m256i const *>(i8a)));
        __m512i x2   = _mm512_cvtepi8_epi16(_mm256_loadu_si256(reinterpret_cast<__m256i const *>(i8b)));
        __m512i xi16 = _mm512_sub_epi16(x1, x2);
#endif
        sum512 = _mm512_add_epi32(sum512, _mm512_madd_epi16(xi16, xi16));
        i8a += 32;
        i8b += 32;
      }
    }
    __m256i sum256 =
        _mm256_add_epi32(_mm512_extracti32x8_epi32(sum512, 0), _mm512_extracti32x8_epi32(sum512, 1));
#else
    __m256i sum256 = _mm256_setzero_si256();
    {
      const auto *lastgroup = last - 31;
      while (i8a < lastgroup) {
        __m256i x1   = _mm256_cvtepi8_epi16(_mm_loadu_si128((__m128i const *)i8a));
        __m256i x2   = _mm256_cvtepi8_epi16(_mm_loadu_si128((__m128i const *)i8b));
        __m256i xi16 = _mm256_sub_epi16(x1, x2);
        sum256       = _mm256_add_epi32(sum256, _mm256_madd_epi16(xi16, xi16));
        i8a += 16;
        i8b += 16;
        x1     = _mm256_cvtepi8_epi16(_mm_loadu_si128((__m128i const *)i8a));
        x2     = _mm256_cvtepi8_epi16(_mm_loadu_si128((__m128i const *)i8b));
        xi16   = _mm256_subs_epi16(x1, x2);
        sum256 = _mm256_add_epi32(sum256, _mm256_madd_epi16(xi16, xi16));
        i8a += 16;
        i8b += 16;
      }
    }
#endif
    {
      const auto *lastgroup = last - 15;

      while (i8a < lastgroup) {
#if defined(__AVX512BW__) && defined(__AVX512VL__)
        __m128i mi8a = _mm_loadu_si128(reinterpret_cast<const __m128i *>(i8a));
        __m128i mi8b = _mm_loadu_si128(reinterpret_cast<const __m128i *>(i8b));
        __mmask16 m  = _mm_cmplt_epi8_mask(mi8a, mi8b);
        __m128i x    = _mm_add_epi8(_mm_maskz_sub_epi8(m, mi8b, mi8a), _mm_maskz_sub_epi8(~m, mi8a, mi8b));
        __m256i xi16 = _mm256_cvtepu8_epi16(x);
#else
        __m256i x1   = _mm256_cvtepi8_epi16(_mm_loadu_si128((__m128i const *)i8a));
        __m256i x2   = _mm256_cvtepi8_epi16(_mm_loadu_si128((__m128i const *)i8b));
        __m256i xi16 = _mm256_sub_epi16(x1, x2);
#endif
        sum256 = _mm256_add_epi32(sum256, _mm256_madd_epi16(xi16, xi16));
        i8a += 16;
        i8b += 16;
      }
    }

    const __m256i value0 = _mm256_set1_epi32(0);
    __m256i tmp1         = _mm256_hadd_epi32(sum256, value0);
    __m256i tmp2         = _mm256_hadd_epi32(tmp1, value0);
    double s             = _mm256_extract_epi32(tmp2, 0) + _mm256_extract_epi32(tmp2, 4);
    return s;
  }

  inline static double compareL2(const qsint8 *a, const quint8 *b, size_t size) {
    NGTThrowException("Not supported.");
    return 0.0;
  }
#ifdef NGT_PQ4
  static double compareL2(const qint4 *a, const qint4 *b, size_t size);
#endif

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

  inline static double compareL1(const float *a, const float *b, size_t size) {
    __m256 sum             = _mm256_setzero_ps();
    const float *last      = a + size;
    const float *lastgroup = last - 7;
    while (a < lastgroup) {
      __m256 x1         = _mm256_sub_ps(_mm256_loadu_ps(a), _mm256_loadu_ps(b));
      const __m256 mask = _mm256_set1_ps(-0.0f);
      __m256 v          = _mm256_andnot_ps(mask, x1);
      sum               = _mm256_add_ps(sum, v);
      a += 8;
      b += 8;
    }
    __attribute__((aligned(32))) float f[8];
    _mm256_store_ps(f, sum);
    double s = f[0] + f[1] + f[2] + f[3] + f[4] + f[5] + f[6] + f[7];
    while (a < last) {
      double d = fabs(*a++ - *b++);
      s += d;
    }
    return s;
  }
#ifdef NGT_HALF_FLOAT
  inline static double compareL1(const float16 *a, const float16 *b, size_t size) {
    __m256 sum               = _mm256_setzero_ps();
    const float16 *last      = a + size;
    const float16 *lastgroup = last - 7;
    while (a < lastgroup) {
      __m256 x1 = _mm256_sub_ps(_mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i *>(a))),
                                _mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i *>(b))));
      const __m256 mask = _mm256_set1_ps(-0.0f);
      __m256 v          = _mm256_andnot_ps(mask, x1);
      sum               = _mm256_add_ps(sum, v);
      a += 8;
      b += 8;
    }
    __attribute__((aligned(32))) float f[8];
    _mm256_store_ps(f, sum);
    double s = f[0] + f[1] + f[2] + f[3] + f[4] + f[5] + f[6] + f[7];
    while (a < last) {
      double d = fabs(*a++ - *b++);
      s += d;
    }
    return s;
  }
#endif
#ifdef NGT_BFLOAT
  inline static double compareL1(const bfloat16 *a, const bfloat16 *b, size_t size) { abort(); }
#endif
  inline static double compareL1(const unsigned char *a, const unsigned char *b, size_t size) {
    __m128 sum                     = _mm_setzero_ps();
    const unsigned char *last      = a + size;
    const unsigned char *lastgroup = last - 7;
    const __m128i zero             = _mm_setzero_si128();
    while (a < lastgroup) {
      __m128i x1 = _mm_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)a));
      __m128i x2 = _mm_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)b));
      x1         = _mm_subs_epi16(x1, x2);
      x1         = _mm_sign_epi16(x1, x1);
      sum        = _mm_add_ps(sum, _mm_cvtepi32_ps(_mm_unpacklo_epi16(x1, zero)));
      sum        = _mm_add_ps(sum, _mm_cvtepi32_ps(_mm_unpackhi_epi16(x1, zero)));
      a += 8;
      b += 8;
    }
    __attribute__((aligned(32))) float f[4];
    _mm_store_ps(f, sum);
    double s = f[0] + f[1] + f[2] + f[3];
    while (a < last) {
      double d = fabs(static_cast<double>(*a++) - static_cast<double>(*b++));
      s += d;
    }
    return s;
  }
  inline static double compareL1(const int8_t *a, const int8_t *b, size_t size) {
    return compareL1<int8_t, int>(a, b, size);
  }
  inline static double compareL1(const qsint8 *a, const qsint8 *b, size_t size) {
    return compareL1(reinterpret_cast<const int8_t *>(a), reinterpret_cast<const int8_t *>(b), size);
  }
#ifdef NGT_PQ4
  inline static double compareL1(const qint4 *a, const qint4 *b, size_t size) {
    NGTThrowException("Not supported.");
  }
#endif

  template <typename OBJECT_TYPE>
  inline static double compareHammingDistance(const OBJECT_TYPE *a, const OBJECT_TYPE *b, size_t size) {
    size_t count            = 0;
    const OBJECT_TYPE *last = a + size;
#if defined(__AVX512F__)
    while (a + 64 <= last) {
      __m512i vxor = _mm512_xor_si512(_mm512_loadu_si512(reinterpret_cast<const __m512i *>(a)),
                                      _mm512_loadu_si512(reinterpret_cast<const __m512i *>(b)));

#if defined(__AVX512VPOPCNTDQ__)
      count += _mm512_reduce_add_epi64(_mm512_popcnt_epi64(vxor));
#else
      __m256i lower = _mm512_castsi512_si256(vxor);
      count += _mm_popcnt_u64(_mm256_extract_epi64(lower, 0));
      count += _mm_popcnt_u64(_mm256_extract_epi64(lower, 1));
      count += _mm_popcnt_u64(_mm256_extract_epi64(lower, 2));
      count += _mm_popcnt_u64(_mm256_extract_epi64(lower, 3));

      __m256i upper = _mm512_extracti64x4_epi64(vxor, 1);
      count += _mm_popcnt_u64(_mm256_extract_epi64(upper, 0));
      count += _mm_popcnt_u64(_mm256_extract_epi64(upper, 1));
      count += _mm_popcnt_u64(_mm256_extract_epi64(upper, 2));
      count += _mm_popcnt_u64(_mm256_extract_epi64(upper, 3));
#endif

      a += 64;
      b += 64;
    }
#endif

#if defined(__AVX512F__) || defined(__AVX2__)
    while (a + 32 <= last) {
      __m256i vxor = _mm256_xor_si256(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(a)),
                                      _mm256_loadu_si256(reinterpret_cast<const __m256i *>(b)));

#if defined(__AVX512VPOPCNTDQ__) && defined(__AVX512VL__)
      __m256i popcnt = _mm256_popcnt_epi64(vxor);
      count += _mm256_extract_epi64(popcnt, 0);
      count += _mm256_extract_epi64(popcnt, 1);
      count += _mm256_extract_epi64(popcnt, 2);
      count += _mm256_extract_epi64(popcnt, 3);
#else
      count += _mm_popcnt_u64(_mm256_extract_epi64(vxor, 0));
      count += _mm_popcnt_u64(_mm256_extract_epi64(vxor, 1));
      count += _mm_popcnt_u64(_mm256_extract_epi64(vxor, 2));
      count += _mm_popcnt_u64(_mm256_extract_epi64(vxor, 3));
#endif
      a += 32;
      b += 32;
    }
#endif
    while (a < last) {
      __m128i vxor = _mm_xor_si128(_mm_loadu_si128(reinterpret_cast<const __m128i *>(a)),
                                   _mm_loadu_si128(reinterpret_cast<const __m128i *>(b)));
#if defined(__AVX512VPOPCNTDQ__) && defined(__AVX512VL__)
      __m128i popcnt = _mm_popcnt_epi64(vxor);
      count += _mm_extract_epi64(popcnt, 0);
      count += _mm_extract_epi64(popcnt, 1);
#else
      count += _mm_popcnt_u64(_mm_extract_epi64(vxor, 0));
      count += _mm_popcnt_u64(_mm_extract_epi64(vxor, 1));
#endif
      a += 16;
      b += 16;
    }
    return static_cast<double>(count);
  }

  template <typename OBJECT_TYPE>
  inline static double compareJaccardDistance(const OBJECT_TYPE *a, const OBJECT_TYPE *b, size_t size) {
    const uint64_t *last = reinterpret_cast<const uint64_t *>(a + size);

    const uint64_t *uinta = reinterpret_cast<const uint64_t *>(a);
    const uint64_t *uintb = reinterpret_cast<const uint64_t *>(b);
    size_t count          = 0;
    size_t countDe        = 0;
    while (uinta < last) {
      count += _mm_popcnt_u64(*uinta & *uintb);
      countDe += _mm_popcnt_u64(*uinta++ | *uintb++);
      count += _mm_popcnt_u64(*uinta & *uintb);
      countDe += _mm_popcnt_u64(*uinta++ | *uintb++);
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
      int64_t sub = static_cast<int64_t>(ai[loca]) - static_cast<int64_t>(bi[locb]);
      count += sub == 0;
      loca += sub <= 0;
      locb += sub >= 0;
    }
    while (ai[loca] != 0) {
      loca++;
    }
    while (locb < size && bi[locb] != 0) {
      locb++;
    }
    return 1.0 - static_cast<double>(count) / static_cast<double>(loca + locb - count);
  }

  inline static double compareDotProduct(const float *a, const float *b, size_t size) {
    const float *last = a + size;
#if defined(NGT_AVX512)
    __m512 sum512 = _mm512_setzero_ps();
    while (a < last) {
      sum512 = _mm512_add_ps(sum512, _mm512_mul_ps(_mm512_loadu_ps(a), _mm512_loadu_ps(b)));
      a += 16;
      b += 16;
    }
    __m256 sum256 = _mm256_add_ps(_mm512_extractf32x8_ps(sum512, 0), _mm512_extractf32x8_ps(sum512, 1));
    __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#elif defined(NGT_AVX2)
    __m256 sum256 = _mm256_setzero_ps();
    while (a < last) {
      sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(_mm256_loadu_ps(a), _mm256_loadu_ps(b)));
      a += 8;
      b += 8;
    }
    __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#else
    __m128 sum128 = _mm_setzero_ps();
    while (a < last) {
      sum128 = _mm_add_ps(sum128, _mm_mul_ps(_mm_loadu_ps(a), _mm_loadu_ps(b)));
      a += 4;
      b += 4;
    }
#endif
    __attribute__((aligned(32))) float f[4];
    _mm_store_ps(f, sum128);
    double s = static_cast<double>(f[0]) + static_cast<double>(f[1]) + static_cast<double>(f[2]) +
               static_cast<double>(f[3]);
    return s;
  }

#ifdef NGT_HALF_FLOAT
  inline static double compareDotProduct(const float16 *a, const float16 *b, size_t size) {
    const float16 *last = a + size;
#if defined(NGT_AVX512)
    __m512 sum512 = _mm512_setzero_ps();
    while (a < last) {
      sum512 = _mm512_add_ps(
          sum512, _mm512_mul_ps(_mm512_cvtph_ps(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(a))),
                                _mm512_cvtph_ps(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(b)))));

      a += 16;
      b += 16;
    }
    __m256 sum256 = _mm256_add_ps(_mm512_extractf32x8_ps(sum512, 0), _mm512_extractf32x8_ps(sum512, 1));
    __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#elif defined(NGT_AVX2)
    __m256 sum256 = _mm256_setzero_ps();
    while (a < last) {
      sum256 = _mm256_add_ps(
          sum256, _mm256_mul_ps(_mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i *>(a))),
                                _mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i *>(b)))));
      a += 8;
      b += 8;
    }
    __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#else
    __m128 sum128 = _mm_setzero_ps();
    while (a < last) {
      __m128i va = _mm_load_si128(reinterpret_cast<const __m128i *>(a));
      __m128i vb = _mm_load_si128(reinterpret_cast<const __m128i *>(b));
      sum128     = _mm_add_ps(sum128, _mm_mul_ps(_mm_cvtph_ps(va), _mm_cvtph_ps(vb)));
      va         = _mm_srli_si128(va, 8);
      vb         = _mm_srli_si128(vb, 8);
      sum128     = _mm_add_ps(sum128, _mm_mul_ps(_mm_cvtph_ps(va), _mm_cvtph_ps(vb)));
      a += 8;
      b += 8;
    }
#endif
    __attribute__((aligned(32))) float f[4];
    _mm_store_ps(f, sum128);
    double s = static_cast<double>(f[0]) + static_cast<double>(f[1]) + static_cast<double>(f[2]) +
               static_cast<double>(f[3]);
    return s;
  }
#endif

#ifdef NGT_BFLOAT
  inline static double compareDotProduct(const bfloat16 *a, const bfloat16 *b, size_t size) { abort(); }
#endif

  inline static double compareDotProduct(const uint8_t *a, const uint8_t *b, size_t size) {
    double sum = 0.0;
    for (size_t loc = 0; loc < size; loc++) {
      sum += static_cast<double>(a[loc]) * static_cast<double>(b[loc]);
    }
    return sum;
  }
  inline static double compareDotProduct(const int8_t *a, const int8_t *b, size_t size) {
    const auto *last = a + size;
#if defined(NGT_AVX512) || defined(NGT_AVX2)
#if defined(NGT_AVX512)
    __m512i sum512 = _mm512_setzero_si512();
    {
      const auto *lastgroup = last - 63;
      while (a < lastgroup) {
        __m512i ma   = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(a));
        __m512i mb   = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(b));
        __m512i malo = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(ma, 0));
        __m512i mahi = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(ma, 1));
        __m512i mblo = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(mb, 0));
        __m512i mbhi = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(mb, 1));
        sum512       = _mm512_add_epi32(sum512, _mm512_madd_epi16(malo, mblo));
        sum512       = _mm512_add_epi32(sum512, _mm512_madd_epi16(mahi, mbhi));
        a += 64;
        b += 64;
      }
    }
    __m256i sum256 =
        _mm256_add_epi32(_mm512_extracti64x4_epi64(sum512, 0), _mm512_extracti64x4_epi64(sum512, 1));
#else
    __m256i sum256 = _mm256_setzero_si256();
#endif
    {
      const auto *lastgroup = last - 31;
      while (a < lastgroup) {
        __m256i ma   = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a));
        __m256i mb   = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(b));
        __m256i malo = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(ma, 0));
        __m256i mahi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(ma, 1));
        __m256i mblo = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(mb, 0));
        __m256i mbhi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(mb, 1));
        sum256       = _mm256_add_epi32(sum256, _mm256_madd_epi16(malo, mblo));
        sum256       = _mm256_add_epi32(sum256, _mm256_madd_epi16(mahi, mbhi));
        a += 32;
        b += 32;
      }
    }
    __m128i sum128 = _mm_add_epi32(_mm256_extracti128_si256(sum256, 0), _mm256_extracti128_si256(sum256, 1));
#endif
    {
      const auto *lastgroup = last - 15;
      while (a < lastgroup) {
        __m128i ma   = _mm_loadu_si128(reinterpret_cast<const __m128i *>(a));
        __m128i mb   = _mm_loadu_si128(reinterpret_cast<const __m128i *>(b));
        __m128i malo = _mm_cvtepi8_epi16(ma);
        __m128i mahi = _mm_cvtepi8_epi16(_mm_bsrli_si128(ma, 8));
        __m128i mblo = _mm_cvtepi8_epi16(mb);
        __m128i mbhi = _mm_cvtepi8_epi16(_mm_bsrli_si128(mb, 8));
        sum128       = _mm_add_epi32(sum128, _mm_madd_epi16(malo, mblo));
        sum128       = _mm_add_epi32(sum128, _mm_madd_epi16(mahi, mbhi));
        a += 16;
        b += 16;
      }
    }
    __m128i tmp = _mm_hadd_epi32(sum128, _mm_set1_epi32(0));
    double sum  = _mm_extract_epi32(tmp, 0) + _mm_extract_epi32(tmp, 1);
    return sum;
  }
  inline static double compareDotProduct(const int8_t *a, const uint8_t *b, size_t size) {
#if defined(__AVX512VNNI__)
    const auto *last = a + size;
#if defined(NGT_AVX512)
    __m512i sum512 = _mm512_setzero_si512();
    {
      const auto *lastgroup = last - 63;
      while (a < lastgroup) {
        __m512i ma = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(a));
        __m512i mb = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(b));
        sum512     = _mm512_dpbusd_epi32(sum512, mb, ma);
        a += 64;
        b += 64;
      }
    }
    __m256i sum256 =
        _mm256_add_epi32(_mm512_extracti32x8_epi32(sum512, 0), _mm512_extracti32x8_epi32(sum512, 1));
    {
      const auto *lastgroup = last - 31;
      while (a < lastgroup) {
        __m256i ma = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a));
        __m256i mb = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(b));
        sum256     = _mm256_dpbusd_epi32(sum256, mb, ma);
        a += 32;
        b += 32;
      }
    }
    __m128i sum128 =
        _mm_add_epi32(_mm256_extracti32x4_epi32(sum256, 0), _mm256_extracti32x4_epi32(sum256, 1));
#elif defined(NGT_AVX2)
    __m256i sum256 = _mm256_setzero_si256();
    {
      const auto *lastgroup = last - 31;
      while (a < lastgroup) {
        __m256i ma = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a));
        __m256i mb = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(b));
        sum256     = _mm256_dpbusd_epi32(sum256, mb, ma);
        a += 32;
        b += 32;
      }
    }
    __m128i sum128 =
        _mm_add_epi32(_mm256_extracti32x4_epi32(sum256, 0), _mm256_extracti32x4_epi32(sum256, 1));
#else
    __m128i sum128 = _mm_setzero_si128();
#endif
    {
      const auto *lastgroup = last - 15;
      while (a < lastgroup) {
        __m128i ma = _mm_loadu_si128(reinterpret_cast<const __m128i *>(a));
        __m128i mb = _mm_loadu_si128(reinterpret_cast<const __m128i *>(b));
        sum128     = _mm_dpbusd_epi32(sum128, mb, ma);
        a += 16;
        b += 16;
      }
    }
    __m128i tmp = _mm_hadd_epi32(sum128, _mm_set1_epi32(0));
    double sum  = _mm_extract_epi32(tmp, 0) + _mm_extract_epi32(tmp, 1);
#else
    double sum = 0.0;
    for (size_t loc = 0; loc < size; loc++) {
      sum += static_cast<double>(a[loc]) * static_cast<double>(b[loc]);
    }
#endif
    return sum;
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
  static double compareDotProduct(const qint4 *a, const qint4 *b, size_t size);
#endif
  inline static double compareCosine(const float *a, const float *b, size_t size) {

    const float *last = a + size;
#if defined(NGT_AVX512)
    __m512 normA = _mm512_setzero_ps();
    __m512 normB = _mm512_setzero_ps();
    __m512 sum   = _mm512_setzero_ps();
    while (a < last) {
      __m512 am = _mm512_loadu_ps(a);
      __m512 bm = _mm512_loadu_ps(b);
      normA     = _mm512_add_ps(normA, _mm512_mul_ps(am, am));
      normB     = _mm512_add_ps(normB, _mm512_mul_ps(bm, bm));
      sum       = _mm512_add_ps(sum, _mm512_mul_ps(am, bm));
      a += 16;
      b += 16;
    }
    __m256 am256 = _mm256_add_ps(_mm512_extractf32x8_ps(normA, 0), _mm512_extractf32x8_ps(normA, 1));
    __m256 bm256 = _mm256_add_ps(_mm512_extractf32x8_ps(normB, 0), _mm512_extractf32x8_ps(normB, 1));
    __m256 s256  = _mm256_add_ps(_mm512_extractf32x8_ps(sum, 0), _mm512_extractf32x8_ps(sum, 1));
    __m128 am128 = _mm_add_ps(_mm256_extractf128_ps(am256, 0), _mm256_extractf128_ps(am256, 1));
    __m128 bm128 = _mm_add_ps(_mm256_extractf128_ps(bm256, 0), _mm256_extractf128_ps(bm256, 1));
    __m128 s128  = _mm_add_ps(_mm256_extractf128_ps(s256, 0), _mm256_extractf128_ps(s256, 1));
#elif defined(NGT_AVX2)
    __m256 normA = _mm256_setzero_ps();
    __m256 normB = _mm256_setzero_ps();
    __m256 sum   = _mm256_setzero_ps();
    __m256 am, bm;
    while (a < last) {
      am    = _mm256_loadu_ps(a);
      bm    = _mm256_loadu_ps(b);
      normA = _mm256_add_ps(normA, _mm256_mul_ps(am, am));
      normB = _mm256_add_ps(normB, _mm256_mul_ps(bm, bm));
      sum   = _mm256_add_ps(sum, _mm256_mul_ps(am, bm));
      a += 8;
      b += 8;
    }
    __m128 am128 = _mm_add_ps(_mm256_extractf128_ps(normA, 0), _mm256_extractf128_ps(normA, 1));
    __m128 bm128 = _mm_add_ps(_mm256_extractf128_ps(normB, 0), _mm256_extractf128_ps(normB, 1));
    __m128 s128  = _mm_add_ps(_mm256_extractf128_ps(sum, 0), _mm256_extractf128_ps(sum, 1));
#else
    __m128 am128 = _mm_setzero_ps();
    __m128 bm128 = _mm_setzero_ps();
    __m128 s128  = _mm_setzero_ps();
    __m128 am, bm;
    while (a < last) {
      am    = _mm_loadu_ps(a);
      bm    = _mm_loadu_ps(b);
      am128 = _mm_add_ps(am128, _mm_mul_ps(am, am));
      bm128 = _mm_add_ps(bm128, _mm_mul_ps(bm, bm));
      s128  = _mm_add_ps(s128, _mm_mul_ps(am, bm));
      a += 4;
      b += 4;
    }

#endif

    __attribute__((aligned(32))) float f[4];
    _mm_store_ps(f, am128);
    double na = f[0] + f[1] + f[2] + f[3];
    _mm_store_ps(f, bm128);
    double nb = f[0] + f[1] + f[2] + f[3];
    _mm_store_ps(f, s128);
    double s = f[0] + f[1] + f[2] + f[3];

    double cosine = s / sqrt(na * nb);
    return cosine;
  }

#ifdef NGT_HALF_FLOAT
  inline static double compareCosine(const float16 *a, const float16 *b, size_t size) {

    const float16 *last = a + size;
#if defined(NGT_AVX512)
    __m512 normA = _mm512_setzero_ps();
    __m512 normB = _mm512_setzero_ps();
    __m512 sum   = _mm512_setzero_ps();
    while (a < last) {
      __m512 am = _mm512_cvtph_ps(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(a)));
      __m512 bm = _mm512_cvtph_ps(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(b)));
      normA     = _mm512_add_ps(normA, _mm512_mul_ps(am, am));
      normB     = _mm512_add_ps(normB, _mm512_mul_ps(bm, bm));
      sum       = _mm512_add_ps(sum, _mm512_mul_ps(am, bm));
      a += 16;
      b += 16;
    }
    __m256 am256 = _mm256_add_ps(_mm512_extractf32x8_ps(normA, 0), _mm512_extractf32x8_ps(normA, 1));
    __m256 bm256 = _mm256_add_ps(_mm512_extractf32x8_ps(normB, 0), _mm512_extractf32x8_ps(normB, 1));
    __m256 s256  = _mm256_add_ps(_mm512_extractf32x8_ps(sum, 0), _mm512_extractf32x8_ps(sum, 1));
    __m128 am128 = _mm_add_ps(_mm256_extractf128_ps(am256, 0), _mm256_extractf128_ps(am256, 1));
    __m128 bm128 = _mm_add_ps(_mm256_extractf128_ps(bm256, 0), _mm256_extractf128_ps(bm256, 1));
    __m128 s128  = _mm_add_ps(_mm256_extractf128_ps(s256, 0), _mm256_extractf128_ps(s256, 1));
#elif defined(NGT_AVX2)
    __m256 normA = _mm256_setzero_ps();
    __m256 normB = _mm256_setzero_ps();
    __m256 sum   = _mm256_setzero_ps();
    __m256 am, bm;
    while (a < last) {
      am    = _mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i *>(a)));
      bm    = _mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i *>(b)));
      normA = _mm256_add_ps(normA, _mm256_mul_ps(am, am));
      normB = _mm256_add_ps(normB, _mm256_mul_ps(bm, bm));
      sum   = _mm256_add_ps(sum, _mm256_mul_ps(am, bm));
      a += 8;
      b += 8;
    }
    __m128 am128 = _mm_add_ps(_mm256_extractf128_ps(normA, 0), _mm256_extractf128_ps(normA, 1));
    __m128 bm128 = _mm_add_ps(_mm256_extractf128_ps(normB, 0), _mm256_extractf128_ps(normB, 1));
    __m128 s128  = _mm_add_ps(_mm256_extractf128_ps(sum, 0), _mm256_extractf128_ps(sum, 1));
#else
    __m128 am128 = _mm_setzero_ps();
    __m128 bm128 = _mm_setzero_ps();
    __m128 s128  = _mm_setzero_ps();
    __m128 am, bm;
    while (a < last) {
      __m128i va = _mm_load_si128(reinterpret_cast<const __m128i *>(a));
      __m128i vb = _mm_load_si128(reinterpret_cast<const __m128i *>(b));
      am         = _mm_cvtph_ps(va);
      bm         = _mm_cvtph_ps(vb);
      am128      = _mm_add_ps(am128, _mm_mul_ps(am, am));
      bm128      = _mm_add_ps(bm128, _mm_mul_ps(bm, bm));
      s128       = _mm_add_ps(s128, _mm_mul_ps(am, bm));
      va         = _mm_srli_si128(va, 8);
      vb         = _mm_srli_si128(vb, 8);
      am         = _mm_cvtph_ps(va);
      bm         = _mm_cvtph_ps(vb);
      am128      = _mm_add_ps(am128, _mm_mul_ps(am, am));
      bm128      = _mm_add_ps(bm128, _mm_mul_ps(bm, bm));
      s128       = _mm_add_ps(s128, _mm_mul_ps(am, bm));
      a += 8;
      b += 8;
    }

#endif

    __attribute__((aligned(32))) float f[4];
    _mm_store_ps(f, am128);
    double na = f[0] + f[1] + f[2] + f[3];
    _mm_store_ps(f, bm128);
    double nb = f[0] + f[1] + f[2] + f[3];
    _mm_store_ps(f, s128);
    double s = f[0] + f[1] + f[2] + f[3];

    double cosine = s / sqrt(na * nb);
    return cosine;
  }
#endif

#ifdef NGT_BFLOAT
  inline static double compareCosine(const bfloat16 *a, const bfloat16 *b, size_t size) { abort(); }
#endif

  inline static double compareCosine(const unsigned char *a, const unsigned char *b, size_t size) {
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

  inline static double compareCosine(const qsint8 *a, const qsint8 *b, size_t size) {
    return compareCosine(reinterpret_cast<const uint8_t *>(a), reinterpret_cast<const uint8_t *>(b), size);
  }

#ifdef NGT_PQ4
  inline static double compareCosine(const qint4 *a, const qint4 *b, size_t size) {
    NGTThrowException("Not supported.");
  }
#endif

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
  template <typename OBJECT_TYPE>
  inline static double compareLorentzDistance(const OBJECT_TYPE *a, const OBJECT_TYPE *b, size_t size) {
    // Unlike the other distance functions, this is not optimized...
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
