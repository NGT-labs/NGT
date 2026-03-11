//
// Copyright (C) 2025 Yahoo Japan Corporation
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

#include "NGT/defines.h"

#if defined(__x86_64__) && !defined(NGT_NO_AVX) && defined(NGT_PQ4)

#include "NGT/Common.h"
#include "NGT/PrimitiveComparator.h"
#include "NGT/ObjectSpace.h"

double NGT::PrimitiveComparator::compareL2(const qint4 *a, const qint4 *b, size_t size) {
  auto &query         = *reinterpret_cast<const NGT::Quantizer::Query *>(a);
  auto &lut           = query.lut;
  auto scale          = query.scale;
  auto *s8a           = static_cast<const int8_t *>(query.quantizedQuery.data());
  auto *u8b           = reinterpret_cast<const uint8_t *>(b);
  const uint8_t *last = u8b + size;
#if defined(NGT_AVX512)
  const __m512i mask512x0F          = _mm512_set1_epi16(0x000f);
  const __m512i mask512xF0          = _mm512_set1_epi16(0x00f0);
  __m512i lookupTable512            = _mm512_loadu_si512((__m512i const *)lut.data());
  const unsigned char *lastgroup512 = last - 31;
  __m512i sum512                    = _mm512_setzero_si512();
  while (u8b < lastgroup512) {
    __m512i packedobj = _mm512_cvtepu8_epi16(_mm256_loadu_si256((__m256i const *)u8b));
    __m512i lo        = _mm512_and_si512(packedobj, mask512x0F);
    __m512i hi        = _mm512_slli_epi16(_mm512_and_si512(packedobj, mask512xF0), 4);
    __m512i hilo      = _mm512_or_si512(lo, hi);
    __m512i bobj      = _mm512_shuffle_epi8(lookupTable512, hilo);
    __m512i aobj      = _mm512_loadu_si512((__m512i const *)s8a);
    __mmask64 m       = _mm512_cmplt_epu8_mask(aobj, bobj);
    __m512i x =
        _mm512_add_epi8(_mm512_maskz_subs_epu8(m, bobj, aobj), _mm512_maskz_subs_epu8(~m, aobj, bobj));
    __m512i xi16 = _mm512_cvtepu8_epi16(_mm512_extracti32x8_epi32(x, 0));
    sum512       = _mm512_add_epi32(sum512, _mm512_madd_epi16(xi16, xi16));
    xi16         = _mm512_cvtepu8_epi16(_mm512_extracti32x8_epi32(x, 1));
    sum512       = _mm512_add_epi32(sum512, _mm512_madd_epi16(xi16, xi16));
    s8a += 64;
    u8b += 32;
  }

  __m256i sum256 =
      _mm256_add_epi32(_mm512_extracti32x8_epi32(sum512, 0), _mm512_extracti32x8_epi32(sum512, 1));

  const __m256i mask256x0F          = _mm256_set1_epi16(0x000f);
  const __m256i mask256xF0          = _mm256_set1_epi16(0x00f0);
  __m256i lookupTable256            = _mm256_loadu_si256((__m256i const *)lut.data());
  const unsigned char *lastgroup256 = last - 15;
  while (u8b < lastgroup256) {
    __m256i packedobj = _mm256_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)u8b));
    __m256i lo        = _mm256_and_si256(packedobj, mask256x0F);
    __m256i hi        = _mm256_slli_epi16(_mm256_and_si256(packedobj, mask256xF0), 4);
    __m256i hilo      = _mm256_or_si256(lo, hi);
    __m256i bobj      = _mm256_shuffle_epi8(lookupTable256, hilo);
    __m256i aobj      = _mm256_loadu_si256((__m256i const *)s8a);
    __mmask32 m       = _mm256_cmplt_epu8_mask(aobj, bobj);
    __m256i x =
        _mm256_add_epi8(_mm256_maskz_subs_epu8(m, bobj, aobj), _mm256_maskz_subs_epu8(~m, aobj, bobj));
    __m256i xi16 = _mm256_cvtepu8_epi16(_mm256_extracti32x4_epi32(x, 0));
    sum256       = _mm256_add_epi32(sum256, _mm256_madd_epi16(xi16, xi16));
    xi16         = _mm256_cvtepu8_epi16(_mm256_extracti32x4_epi32(x, 1));
    sum256       = _mm256_add_epi32(sum256, _mm256_madd_epi16(xi16, xi16));
    s8a += 32;
    u8b += 16;
  }
#else
  __m256i sum256              = _mm256_setzero_si256();
  const __m256i mask256x0F    = _mm256_set1_epi16(0x000f);
  const __m256i mask256xF0    = _mm256_set1_epi16(0x00f0);
  __m256i lookupTable256      = _mm256_loadu_si256((__m256i const *)lut.data());
  const uint8_t *lastgroup256 = last - 15;
  while (u8b < lastgroup256) {
    __m256i packedobj = _mm256_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)u8b));
    __m256i lo        = _mm256_and_si256(packedobj, mask256x0F);
    __m256i hi        = _mm256_slli_epi16(_mm256_and_si256(packedobj, mask256xF0), 4);
    __m256i hilo      = _mm256_or_si256(lo, hi);
    __m256i bobj      = _mm256_shuffle_epi8(lookupTable256, hilo);
    __m256i bobjhi    = _mm256_cvtepu8_epi16(_mm256_extracti128_si256(bobj, 0));
    __m256i aobj      = _mm256_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)s8a));
    __m256i xi16      = _mm256_subs_epi16(aobj, bobjhi);
    sum256            = _mm256_add_epi32(sum256, _mm256_madd_epi16(xi16, xi16));
    s8a += 16;
    __m256i bobjlo = _mm256_cvtepu8_epi16(_mm256_extracti128_si256(bobj, 1));
    aobj           = _mm256_cvtepu8_epi16(_mm_loadu_si128((__m128i const *)s8a));
    xi16           = _mm256_subs_epi16(aobj, bobjlo);
    sum256         = _mm256_add_epi32(sum256, _mm256_madd_epi16(xi16, xi16));
    s8a += 16;
    u8b += 16;
  }
#endif

  const __m256i value0 = _mm256_set1_epi32(0);
  __m256i tmp1         = _mm256_hadd_epi32(sum256, value0);
  __m256i tmp2         = _mm256_hadd_epi32(tmp1, value0);
  double s             = _mm256_extract_epi32(tmp2, 0) + _mm256_extract_epi32(tmp2, 4);
  s                    = sqrt(s) / 255.0 * scale;
  return s;
}

double NGT::PrimitiveComparator::compareDotProduct(const qint4 *a, const qint4 *b, size_t size) {
  auto &query        = *reinterpret_cast<const NGT::Quantizer::Query *>(a);
  auto &lut          = query.lut;
  auto scale         = query.scale;
  auto *s8a          = static_cast<const int8_t *>(query.quantizedQuery.data());
  auto *s8b          = reinterpret_cast<const int8_t *>(b);
  const int8_t *last = s8b + query.genuineSize;
#if defined(__AVX512VNNI__)
  const __m512i mask512x0F   = _mm512_set1_epi16(0x000f);
  const __m512i mask512xF0   = _mm512_set1_epi16(0x00f0);
  __m512i lookupTable512     = _mm512_loadu_si512((__m512i const *)lut.data());
  __m512i sum512             = _mm512_setzero_si512();
  const int8_t *lastgroup512 = last - 31;
  while (s8b < lastgroup512) {
    __m512i packedobj = _mm512_cvtepi8_epi16(_mm256_loadu_si256((__m256i const *)s8b));
    __m512i lo        = _mm512_and_si512(packedobj, mask512x0F);
    __m512i hi        = _mm512_slli_epi16(_mm512_and_si512(packedobj, mask512xF0), 4);
    __m512i hilo      = _mm512_or_si512(lo, hi);
    __m512i bobj      = _mm512_shuffle_epi8(lookupTable512, hilo);
    __m512i aobj      = _mm512_loadu_si512((__m512i const *)s8a);
    sum512            = _mm512_dpbusd_epi32(sum512, bobj, aobj);
    s8a += 64;
    s8b += 32;
  }

  __m256i sum256 =
      _mm256_add_epi32(_mm512_extracti32x8_epi32(sum512, 0), _mm512_extracti32x8_epi32(sum512, 1));

  const __m256i mask256x0F   = _mm256_set1_epi16(0x000f);
  const __m256i mask256xF0   = _mm256_set1_epi16(0x00f0);
  __m256i lookupTable256     = _mm256_loadu_si256((__m256i const *)lut.data());
  const int8_t *lastgroup256 = last - 15;
  while (s8b < lastgroup256) {
    __m256i packedobj = _mm256_cvtepi8_epi16(_mm_loadu_si128((__m128i const *)s8b));
    __m256i lo        = _mm256_and_si256(packedobj, mask256x0F);
    __m256i hi        = _mm256_slli_epi16(_mm256_and_si256(packedobj, mask256xF0), 4);
    __m256i hilo      = _mm256_or_si256(lo, hi);
    __m256i bobj      = _mm256_shuffle_epi8(lookupTable256, hilo);
    __m256i aobj      = _mm256_loadu_si256((__m256i const *)s8a);
    sum256            = _mm256_dpbusd_epi32(sum256, bobj, aobj);
    s8a += 32;
    s8b += 16;
  }
#elif defined(NGT_AVX2) || defined(NGT_AVX512)
  const __m256i mask256x0F   = _mm256_set1_epi16(0x000f);
  const __m256i mask256xF0   = _mm256_set1_epi16(0x00f0);
  __m256i lookupTable256     = _mm256_loadu_si256((__m256i const *)lut.data());
  __m256i sum256             = _mm256_setzero_si256();
  const int8_t *lastgroup256 = last - 15;
  while (s8b < lastgroup256) {
    __m256i packedobj = _mm256_cvtepi8_epi16(_mm_loadu_si128((__m128i const *)s8b));
    __m256i lo        = _mm256_and_si256(packedobj, mask256x0F);
    __m256i hi        = _mm256_slli_epi16(_mm256_and_si256(packedobj, mask256xF0), 4);
    __m256i hilo      = _mm256_or_si256(lo, hi);
    __m256i bobj      = _mm256_shuffle_epi8(lookupTable256, hilo);
    __m256i aobj      = _mm256_loadu_si256((__m256i const *)s8a);
    __m256i a16lo     = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(aobj, 0));
    __m256i b16lo     = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(bobj, 0));
    __m256i prodlo    = _mm256_mullo_epi16(a16lo, b16lo);
    sum256            = _mm256_add_epi32(sum256, _mm256_cvtepi16_epi32(_mm256_extracti128_si256(prodlo, 0)));
    sum256            = _mm256_add_epi32(sum256, _mm256_cvtepi16_epi32(_mm256_extracti128_si256(prodlo, 1)));

    __m256i a16hi  = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(aobj, 1));
    __m256i b16hi  = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(bobj, 1));
    __m256i prodhi = _mm256_mullo_epi16(a16hi, b16hi);
    sum256         = _mm256_add_epi32(sum256, _mm256_cvtepi16_epi32(_mm256_extracti128_si256(prodhi, 0)));
    sum256         = _mm256_add_epi32(sum256, _mm256_cvtepi16_epi32(_mm256_extracti128_si256(prodhi, 1)));
    s8a += 32;
    s8b += 16;
  }
#endif

  const __m256i value0 = _mm256_set1_epi32(0);
  __m256i tmp1         = _mm256_hadd_epi32(sum256, value0);
  __m256i tmp2         = _mm256_hadd_epi32(tmp1, value0);
  double s             = _mm256_extract_epi32(tmp2, 0) + _mm256_extract_epi32(tmp2, 4);

  s -= query.shiftValue;
  s = s / 127.0 / 127.0 * scale * scale;
  return s;
}

#endif
