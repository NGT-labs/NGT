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

#if defined(NGT_NO_SIMD) && defined(NGT_PQ4)

#include "NGT/Common.h"
#include "NGT/PrimitiveComparator.h"
#include "NGT/ObjectSpace.h"

double NGT::PrimitiveComparator::compareL2(const qint4 *a, const qint4 *b, size_t size) {
  auto &query          = *reinterpret_cast<const NGT::Quantizer::Query *>(a);
  auto &quantizedQuery = query.quantizedQuery;
  auto &lut            = query.lut;
  auto scale           = query.scale;
  float d              = 0.0;
  for (size_t i = 0; i < quantizedQuery.size(); i++) {
    float f = static_cast<float>(quantizedQuery[i]);
    float fo;
    if ((i & 0x01) == 0) fo = static_cast<float>(lut[b[i >> 1].lower()]);
    else
      fo = static_cast<float>(lut[b[i >> 1].upper()]);
    f -= fo;
    d += f * f;
  }
  d = sqrt(d) / 127.0 * scale;
  return d;
}

double NGT::PrimitiveComparator::compareNormalizedCosineSimilarity(const qint4 *a, const qint4 *b,
                                                                   size_t size) {
  auto &query          = *reinterpret_cast<const NGT::Quantizer::Query *>(a);
  auto &quantizedQuery = query.quantizedQuery;
  auto &lut            = query.lut;
  float dp             = 0.0;
  for (size_t i = 0; i < quantizedQuery.size(); i++) {
    float fa = static_cast<float>(quantizedQuery[i]);
    float fb;
    if ((i & 0x01) == 0) fb = static_cast<float>(lut[b[i >> 1].lower()]);
    else
      fb = static_cast<float>(lut[b[i >> 1].upper()]);
    dp += fa * fb;
  }
  float d = 1.0 - dp / 127.0 / 127.0 * query.scale * query.scale;
  return d;
}

double NGT::PrimitiveComparator::compareCosineSimilarity(const qint4 *a, const qint4 *b, size_t size) {
  auto &query          = *reinterpret_cast<const NGT::Quantizer::Query *>(a);
  auto &quantizedQuery = query.quantizedQuery;
  auto &lut            = query.lut;
  float normA          = 0.0;
  float normB          = 0.0;
  float sum            = 0.0;
  for (size_t i = 0; i < quantizedQuery.size(); i++) {
    float fa = static_cast<float>(quantizedQuery[i]);
    float fb;
    if ((i & 0x01) == 0) fb = static_cast<float>(lut[b[i >> 1].lower()]);
    else
      fb = static_cast<float>(lut[b[i >> 1].upper()]);
    normA += fa * fa;
    normB += fb * fb;
    sum += fa * fb;
  }
  float d = 1.0 - sum / sqrt(normA * normB);
  return d;
}
#endif
