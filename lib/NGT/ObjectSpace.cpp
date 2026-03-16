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
#include "NGT/Common.h"
#include "NGT/ObjectSpace.h"
#include "NGT/ObjectRepository.h"
#include "NGT/NGTQ/QuantizedBlobGraph.h"

NGT::Distance NGT::ObjectSpace::compareWithL1(NGT::Object &o1, NGT::Object &o2) {
  auto dim = getPaddedDimension();
  NGT::Distance d;
  if (getObjectType() == typeid(uint8_t) || getObjectType() == typeid(quint8) ||
      getObjectType() == typeid(qsint8)
#ifdef NGT_PQ4
      || getObjectType() == typeid(qint4)
#endif
  ) {
    d = PrimitiveComparator::compareL1(reinterpret_cast<uint8_t *>(o1.getPointer()),
                                       reinterpret_cast<uint8_t *>(o2.getPointer()), dim);
#ifdef NGT_HALF_FLOAT
  } else if (getObjectType() == typeid(float16)) {
    d = PrimitiveComparator::compareL1(reinterpret_cast<float16 *>(o1.getPointer()),
                                       reinterpret_cast<float16 *>(o2.getPointer()), dim);
#endif
  } else if (getObjectType() == typeid(float)) {
    d = PrimitiveComparator::compareL1(reinterpret_cast<float *>(o1.getPointer()),
                                       reinterpret_cast<float *>(o2.getPointer()), dim);
  } else {
    std::stringstream msg;
    msg << "ObjectSpace::compareWithL1: Fatal Inner Error! Unexpected object type. "
        << getObjectType().name();
    NGTThrowException(msg);
  }
  return d;
}

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
NGT::Distance NGT::ObjectSpace::compareWithL1(NGT::Object &o1, NGT::PersistentObject &o2) {
  auto dim = getPaddedDimension();
  NGT::Distance d;
  if (getObjectType() == typeid(uint8_t)) {
    d = PrimitiveComparator::compareL1(
        reinterpret_cast<uint8_t *>(o1.getPointer()),
        reinterpret_cast<uint8_t *>(o2.getPointer(getRepository().getAllocator())), dim);
#ifdef NGT_HALF_FLOAT
  } else if (getObjectType() == typeid(float16)) {
    d = PrimitiveComparator::compareL1(
        reinterpret_cast<float16 *>(o1.getPointer()),
        reinterpret_cast<float16 *>(o2.getPointer(getRepository().getAllocator())), dim);
#endif
  } else if (getObjectType() == typeid(float)) {
    d = PrimitiveComparator::compareL1(
        reinterpret_cast<float *>(o1.getPointer()),
        reinterpret_cast<float *>(o2.getPointer(getRepository().getAllocator())), dim);
  } else {
    std::stringstream msg;
    msg << "ObjectSpace::compareWithL1: Fatal Inner Error! Unexpected object type.";
    NGTThrowException(msg);
  }
  return d;
}
#endif

#ifdef NGT_PQ4

NGT::Object *NGT::ObjectSpace::allocateQuantizedQuery(std::vector<float> &object) {
  auto *queryObject = getQuantizer().constructQueryObject(object, *this);
  auto *query       = new NGT::Object(reinterpret_cast<NGT::Object *>(queryObject));
  return query;
}

NGT::Quantizer::~Quantizer() {
  auto *qbgindex = reinterpret_cast<QBG::Index *>(index);
  delete qbgindex;
}

void NGT::Quantizer::open() {
  try {
    index = new QBG::Index(qbgIndexPath);
    setCentroids();
  } catch (NGT::Exception &err) {
    index = 0;
    std::stringstream msg;
    msg << "Cannot open the QBG index (" << qbgIndexPath << "). " << err.what();
    NGTThrowException(msg);
  }
}

void NGT::Quantizer::open(const std::string &path) {
  qbgIndexPath = path + "/" + getQbgIndex();
  open();
}

void NGT::Quantizer::build(std::string &indexPath, bool verbose) {
  NGT::Property prop;

  if (verbose) std::cerr << "ngt: Opening the NGT..." << std::endl;
  NGT::Index index(indexPath);
  index.getProperty(prop);

  if (verbose) std::cerr << "ngt: Creating..." << std::endl;
  index.getObjectSpace().getQuantizer().create(indexPath, prop);
  if (verbose) std::cerr << "ngt: Building..." << std::endl;
  index.getObjectSpace().getQuantizer().build(index.getRefinementObjectSpace(), verbose);
}

void NGT::Quantizer::quantizeToPq4(std::vector<float> &vector, std::vector<uint8_t> &qvector) {
  if (index == 0) {
    std::stringstream msg;
    msg << "QBG is unavailable!";
    NGTThrowException(msg);
  }
  auto &qbgindex = *reinterpret_cast<QBG::Index *>(index);
  auto &q        = qbgindex.getQuantizer();
  if (vector.size() != q.property.genuineDimension) {
    std::stringstream msg;
    msg << "Fatal inner error! Invalid dimension." << vector.size() << ":" << q.property.genuineDimension;
    NGTThrowException(msg);
  }
  size_t dim = vector.size();
  NGTQ::Object object(vector);
  if (vector.size() != q.property.dimension) {
    object.resize(q.property.dimension);
  }
  NGTQ::QuantizedObject qobject;
  size_t subspaceID = 0;
  q.encode(subspaceID, object, qobject);
  size_t qsize = (dim + 1) / 2;
  qvector.resize(qsize);
  for (size_t i = 0; i < qobject.object.size(); i++) {
    auto v = static_cast<uint8_t>(qobject.object[i]);
    v--;
    if ((i & 0x01) == 0) {
      qvector[i >> 1] = v;
    } else {
      qvector[i >> 1] |= v << 4;
    }
  }
}

void *NGT::Quantizer::constructQueryObject(std::vector<float> &query, ObjectSpace &objectSpace) {
  auto *quantizedQuery = new NGT::Quantizer::Query;
  setQuantizedQuery(query, *quantizedQuery, objectSpace);
  return quantizedQuery;
}

void *NGT::Quantizer::constructQueryObject() {
  auto *quantizedQuery = new NGT::Quantizer::Query;
  setQuantizedQuery(*quantizedQuery);
  return quantizedQuery;
}

void NGT::Quantizer::setCentroids() {
  if (index == 0) {
    std::cerr << "ObjectSpace::Quantizer: Warning! index is null" << std::endl;
    return;
  }
  auto &qbgindex                = *reinterpret_cast<QBG::Index *>(index);
  auto &quantizer               = qbgindex.getQuantizer();
  auto &quantizedObjectDistance = quantizer.getQuantizedObjectDistance();
  auto *localCentroids          = static_cast<float *>(&quantizedObjectDistance.localCentroidsForSIMD[0]);
  if (quantizedObjectDistance.localCodebookCentroidNoSIMD == 0) {
    centroids.clear();
    boundaries.clear();
    return;
  }
  centroids.resize(quantizedObjectDistance.localCodebookCentroidNoSIMD);
  float min = std::numeric_limits<float>::max();
  float max = 0.0;
  for (size_t i = 0; i < centroids.size(); i++) {
    float v      = localCentroids[i];
    centroids[i] = v;
    if (min > v) min = v;
    if (max < v) max = v;
  }
  centroidMin = min;
  centroidMax = max;

  auto &bs = boundaries;
  bs.reserve(centroids.size());
  for (size_t i = 0; i < centroids.size(); i++) {
    bs.push_back(make_pair(centroids[i], i));
  }
  std::sort(bs.begin(), bs.end());
  for (size_t i = 0; i < bs.size() - 1; i++) {
    bs[i].first = (bs[i].first + bs[i + 1].first) / 2.0;
  }
  bs.back().first = std::numeric_limits<float>::max();
}

void NGT::Quantizer::setQuantizedQuery(std::vector<float> &query, NGT::Quantizer::Query &qQuery,
                                       ObjectSpace &objectSpace) {
  if (centroids.size() == 0) {
    NGTThrowException("Fatal error! No centroid for the quantizer.");
  }
  if (objectSpace.isNormalizedDistance()) {
    objectSpace.normalize(query);
  }
  float min = centroidMin;
  float max = centroidMax;
  for (auto &v : query) {
    if (min > v) min = v;
    if (max < v) max = v;
  }
  qQuery.offset = 0;
  qQuery.scale  = max - min;
#if defined(__AVX512VNNI__)
  qQuery.shift = objectSpace.getDistanceType() == ObjectSpace::DistanceTypeNormalizedCosine;
#else
  qQuery.shift = false;
#endif
  size_t repeat = 4;
  qQuery.lut.reserve(centroids.size() * repeat);
  qQuery.lut.resize(centroids.size());
  if (qQuery.shift) {
    for (size_t i = 0; i < qQuery.lut.size(); i++) {
      float fv      = std::round((centroids[i] - qQuery.offset) / qQuery.scale * 127.5 + 128.0);
      fv            = fv < 0.0 ? 0.0 : fv;
      fv            = fv >= 256.0 ? 255 : fv;
      qQuery.lut[i] = static_cast<uint8_t>(fv);
    }
  } else {
    for (size_t i = 0; i < qQuery.lut.size(); i++) {
      float fv      = std::round((centroids[i] - qQuery.offset) / qQuery.scale * 127.5);
      fv            = fv < -128.0 ? -128.0 : fv;
      fv            = fv > 127.0 ? 127.0 : fv;
      qQuery.lut[i] = static_cast<uint8_t>(fv);
    }
  }
  for (size_t i = 0; i < repeat - 1; ++i) {
    qQuery.lut.insert(qQuery.lut.end(), qQuery.lut.begin(), qQuery.lut.begin() + centroids.size());
  }

  if (objectSpace.getPaddedDimension() * 2 < query.size()) {
    std::stringstream msg;
    msg << "Too long query. " << objectSpace.getPaddedDimension() * 2 << ":" << query.size();
    NGTThrowException(msg);
  }
  qQuery.genuineSize = objectSpace.getDimension();
  qQuery.quantizedQuery.resize(objectSpace.getPaddedDimension() * 2, 0);
  for (size_t i = 0; i < query.size(); i++) {
    float fv                 = std::round((query[i] - qQuery.offset) / qQuery.scale * 127.5);
    fv                       = fv < -128.0 ? -128.0 : fv;
    fv                       = fv > 127.0 ? 127.0 : fv;
    qQuery.quantizedQuery[i] = static_cast<int8_t>(fv);
  }
  qQuery.shiftValue = 0.0;
  if (qQuery.shift) {
    for (size_t i = 0; i < query.size(); i++) {
      qQuery.shiftValue += qQuery.quantizedQuery[i];
    }
    qQuery.shiftValue *= 128.0;
  }
}

void NGT::Quantizer::setQuantizedQuery(NGT::Quantizer::Query &qQuery) {
  if (centroids.size() == 0) {
    NGTThrowException("Fatal error! Something wrong.");
  }
  float min     = centroidMin;
  float max     = centroidMax;
  qQuery.offset = min;
  qQuery.scale  = max - min;
  qQuery.lut.resize(centroids.size());
  for (size_t i = 0; i < qQuery.lut.size(); i++) {
    float fv      = std::round((centroids[i] - qQuery.offset) / qQuery.scale * 255.5);
    fv            = fv < 0.0 ? 0.0 : fv;
    fv            = fv >= 256.0 ? 255 : fv;
    qQuery.lut[i] = static_cast<uint8_t>(fv);
  }
}

void NGT::Quantizer::destructQueryObject(void *c) {
  if (c == 0) {
    return;
  }
  auto *queryObject = static_cast<NGT::Quantizer::Query *>(c);
  delete queryObject;
}

NGT::Distance NGT::Quantizer::compareL2(void *a, void *b, size_t s) {
  auto *ao = reinterpret_cast<qint4 *>(a);
  auto *bo = reinterpret_cast<qint4 *>(b);
  float d  = 0.0;
  for (size_t i = 0; i < s; i++) {
    float fa = centroids[ao[i].lower()];
    float fb = centroids[bo[i].lower()];
    fa -= fb;
    d += fa * fa;
    fa = centroids[ao[i].upper()];
    fb = centroids[bo[i].upper()];
    fa -= fb;
    d += fa * fa;
  }
  d = sqrt(d);
  return d;
}

NGT::Distance NGT::Quantizer::compareCosineSimilarity(void *a, void *b, size_t s) {
  auto *ao    = reinterpret_cast<qint4 *>(a);
  auto *bo    = reinterpret_cast<qint4 *>(b);
  float normA = 0.0;
  float normB = 0.0;
  float sum   = 0.0;
  for (size_t i = 0; i < s; i++) {
    float fa = centroids[ao[i].lower()];
    float fb = centroids[bo[i].lower()];
    normA += fa * fa;
    normB += fb * fb;
    sum += fa * fb;
    fa = centroids[ao[i].upper()];
    fb = centroids[bo[i].upper()];
    normA += fa * fa;
    normB += fb * fb;
    sum += fa * fb;
  }
  float d = 1.0 - sum / sqrt(normA * normB);
  return d < 0.0 ? -d : d;
}

NGT::Distance NGT::Quantizer::compareNormalizedCosineSimilarity(void *a, void *b, size_t s) {
  auto *ao  = reinterpret_cast<qint4 *>(a);
  auto *bo  = reinterpret_cast<qint4 *>(b);
  float sum = 0.0;
  for (size_t i = 0; i < s; i++) {
    float fa = centroids[ao[i].lower()];
    float fb = centroids[bo[i].lower()];
    sum += fa * fb;
    fa = centroids[ao[i].upper()];
    fb = centroids[bo[i].upper()];
    sum += fa * fb;
  }
  float d = 1.0 - sum;
  return d < 0.0 ? -d : d;
}

void NGT::Quantizer::create(const std::string &indexPath, NGT::Property &prop) {

  if (prop.objectType != NGT::ObjectSpace::Qint4) {
    std::stringstream msg;
    msg << "Not qint4 object type. " << prop.objectType << std::endl;
    NGTThrowException(msg);
  }

  QBG::BuildParameters buildParameters;
  buildParameters.creation.localClusterDataType = NGTQ::ClusterDataTypePQ4;
  buildParameters.creation.genuineDimension     = prop.dimension;
  buildParameters.creation.dimension       = ((buildParameters.creation.genuineDimension + 15) / 16) * 16;
  buildParameters.creation.numOfSubvectors = buildParameters.creation.dimension;
  switch (prop.distanceType) {
  case NGT::ObjectSpace::DistanceTypeL2:
    buildParameters.creation.distanceType = NGTQ::DistanceType::DistanceTypeL2;
    break;
  case NGT::ObjectSpace::DistanceTypeL1:
    buildParameters.creation.distanceType = NGTQ::DistanceType::DistanceTypeL1;
    break;
  case NGT::ObjectSpace::DistanceTypeNormalizedCosine:
    buildParameters.creation.distanceType = NGTQ::DistanceType::DistanceTypeNormalizedCosine;
    break;
  case NGT::ObjectSpace::DistanceTypeCosine:
    buildParameters.creation.distanceType = NGTQ::DistanceType::DistanceTypeCosine;
    break;
  case NGT::ObjectSpace::DistanceTypeInnerProduct:
    buildParameters.creation.distanceType = NGTQ::DistanceType::DistanceTypeInnerProduct;
    break;
  default:
    std::stringstream msg;
    msg << "The specified data type is unavailable. " << prop.distanceType << std::endl;
    NGTThrowException(msg);
  }
  buildParameters.creation.genuineDataType  = ObjectFile::DataTypeFloat;
  buildParameters.creation.globalObjectType = NGT::ObjectSpace::ObjectType::Float;

  qbgIndexPath = indexPath + "/" + NGT::Quantizer::getQbgIndex();

  QBG::Index::create(qbgIndexPath, buildParameters);
}

void NGT::Quantizer::build(ObjectSpace &os, bool verbose) {
  if (verbose) std::cerr << "appending..." << std::endl;
  QBG::Index::append(qbgIndexPath, os);
  QBG::Optimizer optimizer;

  optimizer.unifiedPQ     = true;
  optimizer.rotation      = false;
  optimizer.repositioning = false;
  optimizer.globalType    = QBG::Optimizer::GlobalTypeZero;
  optimizer.verbose       = verbose;

  if (verbose) std::cerr << "optimizing..." << std::endl;
  optimizer.optimize(qbgIndexPath);
  if (verbose) std::cerr << "building NGTQ..." << std::endl;
  QBG::Index::buildNGTQ(qbgIndexPath, verbose);

  if (verbose) std::cerr << "opening..." << std::endl;
}

#endif

NGT::ObjectSpace *NGT::ObjectSpace::convertObjectSpace(NGT::ObjectSpace &srcObjectSpace,
                                                       ObjectType targetDataType, float maxMagnitude) {
  DistanceType distanceType = srcObjectSpace.getDistanceType();
  size_t dimension          = srcObjectSpace.getDimension();
  size_t size               = srcObjectSpace.getRepository().size();

  NGT::ObjectSpace *dstObjectSpace = nullptr;

  switch (targetDataType) {
  case ObjectType::Float:
    dstObjectSpace =
        new NGT::ObjectSpaceRepository<float, double>(dimension, typeid(float), distanceType, maxMagnitude);
    break;
  case ObjectType::Float16:
    dstObjectSpace = new NGT::ObjectSpaceRepository<NGT::float16, double>(dimension, typeid(NGT::float16),
                                                                          distanceType, maxMagnitude);
    break;
  case ObjectType::Uint8:
    dstObjectSpace = new NGT::ObjectSpaceRepository<uint8_t, double>(dimension, typeid(uint8_t), distanceType,
                                                                     maxMagnitude);
    break;
  default:
    std::stringstream msg;
    msg << "Invalid target data type. " << targetDataType;
    NGTThrowException(msg);
  }

  auto &dstRepo = dstObjectSpace->getRepository();
  dstRepo.initialize();

  auto &srcRepo                 = srcObjectSpace.getRepository();
  const std::type_info &srcType = srcObjectSpace.getObjectType();

  for (size_t id = 1; id < size; id++) {
    if (srcRepo[id] == nullptr) {
      dstRepo.push_back(nullptr);
      continue;
    }

    std::vector<float> floatVector(dimension);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    void *srcPtr = srcRepo[id]->getPointer(srcRepo.getAllocator());
#else
    void *srcPtr = srcRepo[id]->getPointer();
#endif

    if (srcType == typeid(float)) {
      float *src = static_cast<float *>(srcPtr);
      for (size_t i = 0; i < dimension; i++) {
        floatVector[i] = src[i];
      }
    } else if (srcType == typeid(uint8_t)) {
      uint8_t *src = static_cast<uint8_t *>(srcPtr);
      for (size_t i = 0; i < dimension; i++) {
        floatVector[i] = static_cast<float>(src[i]);
      }
    } else if (srcType == typeid(NGT::float16)) {
      NGT::float16 *src = static_cast<NGT::float16 *>(srcPtr);
      for (size_t i = 0; i < dimension; i++) {
        floatVector[i] = static_cast<float>(src[i]);
      }
    }

    PersistentObject *obj = 0;
    try {
      obj = dstRepo.allocateNormalizedPersistentObject(floatVector);
    } catch (Exception &err) {
      std::cerr << err.what() << " continue..." << std::endl;
      obj = dstRepo.allocatePersistentObject(floatVector);
    }
    dstRepo.push_back(obj);
  }
  return dstObjectSpace;
}

NGT::ObjectSpace::ObjectType NGT::ObjectSpace::getEstimatedObjectType() {
  auto &repo                    = getRepository();
  size_t size                   = repo.size();
  size_t dim                    = getDimension();
  const std::type_info &srcType = getObjectType();

  if (srcType == typeid(uint8_t)) {
    return ObjectType::Uint8;
  }

  NGT::ObjectSpace::ObjectType otype = dim < 100 ? ObjectType::Float : ObjectType::Float16;

  for (size_t id = 1; id < size; id++) {
    if (repo[id] == nullptr) {
      continue;
    }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    void *ptr = repo[id]->getPointer(repo.getAllocator());
#else
    void *ptr = repo[id]->getPointer();
#endif

    if (srcType == typeid(float)) {
      float *src = static_cast<float *>(ptr);
      for (size_t i = 0; i < dim; i++) {
        float val = src[i];
        if (val < 0.0f || val > 255.0f || val != std::floor(val)) {
          return otype;
          ;
        }
      }
    }
  }

  return ObjectType::Uint8;
}
