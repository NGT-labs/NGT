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

#include "PrimitiveComparator.h"

class ObjectSpace;

namespace NGT {

#ifdef NGT_PQ4
class Property;
class Quantizer {
 public:
  class Query {
   public:
    Query() : insideObject(0) {}
    Query(NGT::Object *o) : insideObject(o) {}
    std::vector<int8_t> lut;
    std::vector<int8_t> quantizedQuery;
    NGT::Object *insideObject;
    float offset;
    float scale;
    bool shift;
    float shiftValue;
    size_t genuineSize;
  };
  Quantizer() : index(0) {}
  ~Quantizer();
  void open();
  void open(const std::string &path);
  void quantizeToPq4(std::vector<float> &vector, std::vector<uint8_t> &qvector);
  static void build(std::string &indexPath, bool verbose);
  void setCentroids();
  void *constructQueryObject(std::vector<float> &object, ObjectSpace &objectSpace);
  void *constructQueryObject();
  void setQuantizedQuery(std::vector<float> &query, Query &qQuery, ObjectSpace &objectSpace);
  void setQuantizedQuery(Query &qQuery);
  void destructQueryObject(void *comparator);
  void quantize(std::vector<float> &object, std::vector<qint4> &quantizedObject);
  NGT::Distance compareL2(void *a, void *b, size_t s);
  NGT::Distance compareCosineSimilarity(void *a, void *b, size_t s);
  NGT::Distance compareNormalizedCosineSimilarity(void *a, void *b, size_t s);
  static const std::string getQbgIndex() { return "qbg"; }
  void create(const std::string &indexPath, Property &prop);
  void build(ObjectSpace &os, bool verbose = false);
  bool isAvailable() { return index != 0; }
  std::string qbgIndexPath;
  void *index;
  float centroidMax;
  float centroidMin;
  std::vector<float> centroids;
  std::vector<std::pair<float, uint32_t>> boundaries;
};
#endif

class PersistentObjectDistances;
class ObjectDistances : public std::vector<ObjectDistance> {
 public:
  ObjectDistances(NGT::ObjectSpace *os = 0) {}
  ObjectDistances(ObjectDistance *begin, ObjectDistance *end) : std::vector<ObjectDistance>(begin, end) {}
  void serialize(std::ofstream &os, ObjectSpace *objspace = 0) {
    NGT::Serializer::write(os, (std::vector<ObjectDistance> &)*this);
  }
  void deserialize(std::ifstream &is, ObjectSpace *objspace = 0) {
    NGT::Serializer::read(is, (std::vector<ObjectDistance> &)*this);
  }

  void serializeAsText(std::ofstream &os, ObjectSpace *objspace = 0) {
    NGT::Serializer::writeAsText(os, size());
    os << " ";
    for (size_t i = 0; i < size(); i++) {
      (*this)[i].serializeAsText(os);
      os << " ";
    }
  }
  void deserializeAsText(std::ifstream &is, ObjectSpace *objspace = 0) {
    size_t s;
    NGT::Serializer::readAsText(is, s);
    resize(s);
    for (size_t i = 0; i < size(); i++) {
      (*this)[i].deserializeAsText(is);
    }
  }

  void moveFrom(ResultSet &pq) {
    this->clear();
    this->resize(pq.size());
    for (int i = pq.size() - 1; i >= 0; i--) {
      (*this)[i] = pq.top();
      pq.pop();
    }
    assert(pq.size() == 0);
  }

  void moveFrom(ResultSet &pq, double (&f)(double)) {
    this->clear();
    this->resize(pq.size());
    for (int i = pq.size() - 1; i >= 0; i--) {
      (*this)[i]          = pq.top();
      (*this)[i].distance = f((*this)[i].distance);
      pq.pop();
    }
    assert(pq.size() == 0);
  }

  void moveFrom(ResultSet &pq, unsigned int id) {
    this->clear();
    if (pq.size() == 0) {
      return;
    }
    this->resize(id == 0 ? pq.size() : pq.size() - 1);
    int i = this->size() - 1;
    while (pq.size() != 0 && i >= 0) {
      if (pq.top().id != id) {
        (*this)[i] = pq.top();
        i--;
      }
      pq.pop();
    }
    if (pq.size() != 0 && pq.top().id != id) {
      std::cerr << "moveFrom: Fatal error: somethig wrong! " << pq.size() << ":" << this->size() << ":" << id
                << ":" << pq.top().id << std::endl;
      assert(pq.size() == 0 || pq.top().id == id);
    }
  }

  ObjectDistances &operator=(PersistentObjectDistances &objs);
};

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
class PersistentObjectDistances : public Vector<ObjectDistance> {
 public:
  PersistentObjectDistances(SharedMemoryAllocator &allocator, NGT::ObjectSpace *os = 0) {}
  void serialize(std::ofstream &os, ObjectSpace *objectspace = 0) {
    NGT::Serializer::write(os, (Vector<ObjectDistance> &)*this);
  }
  void deserialize(std::ifstream &is, ObjectSpace *objectspace = 0) {
    NGT::Serializer::read(is, (Vector<ObjectDistance> &)*this);
  }
  void serializeAsText(std::ofstream &os, SharedMemoryAllocator &allocator, ObjectSpace *objspace = 0) {
    NGT::Serializer::writeAsText(os, size());
    os << " ";
    for (size_t i = 0; i < size(); i++) {
      (*this).at(i, allocator).serializeAsText(os);
      os << " ";
    }
  }
  void deserializeAsText(std::ifstream &is, SharedMemoryAllocator &allocator, ObjectSpace *objspace = 0) {
    size_t s;
    is >> s;
    resize(s, allocator);
    for (size_t i = 0; i < size(); i++) {
      (*this).at(i, allocator).deserializeAsText(is);
    }
  }
  PersistentObjectDistances &copy(ObjectDistances &objs, SharedMemoryAllocator &allocator) {
    clear(allocator);
    reserve(objs.size(), allocator);
    for (ObjectDistances::iterator i = objs.begin(); i != objs.end(); i++) {
      push_back(*i, allocator);
    }
    return *this;
  }
};
typedef PersistentObjectDistances GraphNode;

inline ObjectDistances &ObjectDistances::operator=(PersistentObjectDistances &objs) {
  clear();
  reserve(objs.size());
  std::cerr << "not implemented" << std::endl;
  assert(0);
  return *this;
}
#else  // NGT_SHARED_MEMORY_ALLOCATOR
typedef ObjectDistances GraphNode;
#endif // NGT_SHARED_MEMORY_ALLOCATOR

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
class PersistentObject;
#else
typedef Object PersistentObject;
#endif

class ObjectRepository;

class ObjectSpace {
 public:
  class Comparator {
   public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    Comparator(size_t d, SharedMemoryAllocator &a) : dimension(d), allocator(a) {}
#else
    Comparator(size_t d) : dimension(d) {}
#endif
    virtual double operator()(Object &objecta, Object &objectb) = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    virtual double operator()(Object &objecta, PersistentObject &objectb)           = 0;
    virtual double operator()(PersistentObject &objecta, PersistentObject &objectb) = 0;
#endif
    size_t dimension;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    SharedMemoryAllocator &allocator;
#endif
#ifdef NGT_PQ4
    virtual void setQuery(std::vector<float> &query) {}
#endif
    virtual ~Comparator() {}
  };
  enum DistanceType {
    DistanceTypeNone             = -1,
    DistanceTypeL1               = 0,
    DistanceTypeL2               = 1,
    DistanceTypeHamming          = 2,
    DistanceTypeAngle            = 3,
    DistanceTypeCosine           = 4,
    DistanceTypeNormalizedAngle  = 5,
    DistanceTypeNormalizedCosine = 6,
    DistanceTypeJaccard          = 7,
    DistanceTypeSparseJaccard    = 8,
    DistanceTypeNormalizedL2     = 9,
    DistanceTypeInnerProduct     = 10,
    DistanceTypeDotProduct       = 11,
    DistanceTypePoincare         = 100, // added by Nyapicom
    DistanceTypeLorentz          = 101  // added by Nyapicom
  };

  enum ObjectType {
    ObjectTypeNone = 0,
    Uint8          = 1,
    Float          = 2,
#ifdef NGT_HALF_FLOAT
    Float16 = 3,
#endif
    Qsuint8 = 7,
#ifdef NGT_BFLOAT
    Bfloat16 = 5,
#endif
#ifdef NGT_PQ4
    Qint4 = 10,
#endif
    ObjectTypeUnset = 127
  };

  ObjectSpace(size_t d)
      : dimension(d), distanceType(DistanceTypeNone), comparator(0), comparatorForSearch(0),
        normalization(false), prefetchOffset(-1), prefetchSize(-1), quantizationScale(0.0),
        quantizationOffset(0.0), magnitude(-1) {}
  virtual ~ObjectSpace() {
    if (comparator != 0) {
      delete comparator;
    }
    if (comparatorForSearch != 0) {
      delete comparatorForSearch;
    }
  }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  virtual void open(const std::string &f, size_t shareMemorySize)         = 0;
  virtual Object *allocateObject(Object &o)                               = 0;
  virtual Object *allocateObject(PersistentObject &o)                     = 0;
  virtual PersistentObject *allocatePersistentObject(Object &obj)         = 0;
  virtual void deleteObject(PersistentObject *)                           = 0;
  virtual void copy(PersistentObject &objecta, PersistentObject &objectb) = 0;
  virtual void show(std::ostream &os, PersistentObject &object)           = 0;
  virtual size_t insert(PersistentObject *obj)                            = 0;
#else
  virtual size_t insert(Object *obj) = 0;
  virtual void deleteAll()           = 0;
#endif

  Comparator &getComparator() { return *comparator; }
  Comparator &getComparatorForSearch() {
    if (comparatorForSearch != 0) {
      return *comparatorForSearch;
    } else {
      return *comparator;
    }
  }

  virtual void serialize(const std::string &of)              = 0;
  virtual void deserialize(const std::string &ifile)         = 0;
  virtual void serializeAsText(const std::string &of)        = 0;
  virtual void deserializeAsText(const std::string &of)      = 0;
  virtual void readText(std::istream &is, size_t dataSize)   = 0;
  virtual void appendText(std::istream &is, size_t dataSize) = 0;
  virtual void append(const float *data, size_t dataSize)    = 0;
  virtual void append(const double *data, size_t dataSize)   = 0;
  virtual void append(const uint8_t *data, size_t dataSize)  = 0;
#ifdef NGT_HALF_FLOAT
  virtual void append(const float16 *data, size_t dataSize) = 0;
#endif

  virtual void copy(Object &objecta, Object &objectb) = 0;

  virtual void linearSearch(Object &query, double radius, size_t size, ResultSet &results) = 0;

  virtual std::pair<float, float> getMaxMin(float cut = 0.01, size_t size = 0)                  = 0;
  virtual const std::type_info &getObjectType()                                                 = 0;
  virtual void show(std::ostream &os, Object &object)                                           = 0;
  virtual size_t getSize()                                                                      = 0;
  virtual size_t getSizeOfElement()                                                             = 0;
  virtual size_t getByteSizeOfObject()                                                          = 0;
  virtual Object *allocateNormalizedObject(const std::string &textLine, const std::string &sep) = 0;
  virtual Object *allocateNormalizedObject(const std::vector<double> &obj)                      = 0;
  virtual Object *allocateNormalizedObject(const std::vector<float> &obj)                       = 0;
#ifdef NGT_HALF_FLOAT
  virtual Object *allocateNormalizedObject(const std::vector<float16> &obj) = 0;
#endif
  virtual Object *allocateNormalizedObject(const std::vector<uint8_t> &obj)                    = 0;
  virtual Object *allocateNormalizedObject(const float *obj, size_t size)                      = 0;
  virtual PersistentObject *allocateNormalizedPersistentObject(const std::vector<double> &obj) = 0;
  virtual PersistentObject *allocateNormalizedPersistentObject(const std::vector<float> &obj)  = 0;
  virtual void deleteObject(Object *po)                                                        = 0;
  virtual Object *allocateObject()                                                             = 0;
  virtual void remove(size_t id)                                                               = 0;

  virtual ObjectRepository &getRepository() = 0;

  virtual void setDistanceType(DistanceType t) = 0;

  virtual void *getObject(size_t idx)                       = 0;
  virtual void getObject(size_t idx, std::vector<float> &v) = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  virtual std::vector<float> getObject(PersistentObject &object, SharedMemoryAllocator &allocator) = 0;
#endif
  virtual std::vector<float> getObject(Object &object)                                          = 0;
  virtual void getObjects(const std::vector<size_t> &idxs, std::vector<std::vector<float>> &vs) = 0;
  virtual float computeMaxMagnitude(ObjectID beginId)                                           = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  virtual void setMagnitude(float maxMag, NGT::PersistentRepository<void> &graphNodes,
                            NGT::ObjectID beginID) = 0;
#else
  virtual void setMagnitude(float maxMag, NGT::Repository<void> &graphNodes, ObjectID beginId) = 0;
#endif
  DistanceType getDistanceType() { return distanceType; }
  size_t getDimension() { return dimension; }
  size_t getPaddedDimension() { return ((dimension - 1) / 16 + 1) * 16; }

  template <typename T> static void normalize(T *data, size_t dim) {
    float sum = 0.0;
    for (size_t i = 0; i < dim; i++) {
      sum += static_cast<float>(data[i]) * static_cast<float>(data[i]);
    }
    if (sum == 0.0) {
      for (size_t i = 0; i < dim; i++) {
        if (static_cast<float>(data[i]) != 0.0) {
          std::stringstream msg;
          msg << "ObjectSpace::normalize: Error! the sum of the object is zero for the cosine similarity, "
                 "but not a zero vector. "
              << i << ":" << static_cast<float>(data[i]);
          NGTThrowException(msg);
        }
      }
      std::stringstream msg;
#ifdef NGT_DISABLE_NORMALIZATION_ERROR_CHECK
      return;
#else
      msg << "ObjectSpace::normalize: Error! the object is an invalid zero vector for the cosine similarity. "
          << "type=" << typeid(T).name() << ". ";
      NGTThrowException(msg);
#endif
    }
    sum = sqrt(sum);
    for (size_t i = 0; i < dim; i++) {
      data[i] = static_cast<float>(data[i]) / sum;
    }
  }

  template <typename OTYPE> static void normalize(std::vector<OTYPE> &object) {
    ObjectSpace::normalize(object.data(), object.size());
  }

  int32_t getPrefetchOffset() { return prefetchOffset; }
  int32_t setPrefetchOffset(int offset) {
    if (offset > 0) {
      prefetchOffset = offset;
    }
    if (prefetchOffset <= 0) {
      prefetchOffset = floor(300.0 / (static_cast<float>(getPaddedDimension()) + 30.0) + 1.0);
    }
    return prefetchOffset;
  }
  int32_t getPrefetchSize() { return prefetchSize; }
  int32_t setPrefetchSize(int size) {
    if (size > 0) {
      prefetchSize = size;
    }
    if (prefetchSize <= 0) {
      prefetchSize = getByteSizeOfObject();
    }
    return prefetchSize;
  }

  bool scalarQuantizationIsEnabled() { return quantizationScale != 0.0; }
  bool pq4IsEnabled() {
#ifdef NGT_PQ4
    return getObjectType() == typeid(qint4);
#else
    return false;
#endif
  }
  bool quantizationIsEnabled() {
    return scalarQuantizationIsEnabled()
#ifdef NGT_PQ4
           || pq4IsEnabled()
#endif
        ;
  }
  void setQuantization(float scale, float offset) {
    quantizationScale  = scale;
    quantizationOffset = offset;
  }
  std::pair<float, float> getQuantization() { return std::make_pair(quantizationScale, quantizationOffset); }

  template <typename T> static void quantizeSymmetrically(T *vector, size_t dim, float max, float scale) {
    auto fmax = max + 0.5;
    for (size_t i = 0; i < dim; i++) {
      float fv  = static_cast<float>(vector[i]);
      fv        = std::round(fv / scale * fmax);
      fv        = fv < -max ? -max : fv;
      fv        = fv > max ? max : fv;
      vector[i] = static_cast<T>(fv);
    }
  }

  template <typename T> static void quantizeSymmetrically(std::vector<T> &vector, float max, float scale) {
    quantizeSymmetrically(vector.data(), vector.size(), max, scale);
  }

  template <typename T>
  static void dequantizeSymmetrically(T *vector, int8_t *cvector, size_t dimension, float max, float scale) {
    auto fmax = max + 0.5;
    for (size_t i = 0; i < dimension; i++) {
      float fv  = static_cast<float>(cvector[i]);
      fv        = (fv / fmax) * scale;
      vector[i] = static_cast<T>(fv);
    }
  }

  template <typename T>
  static void dequantizeSymmetrically(std::vector<T> &vector, int8_t *cvector, size_t dimension, float max,
                                      float scale) {
    vector.resize(dimension);
    dequantizeSymmetrically(vector.data(), cvector, dimension, max, scale);
  }

  template <typename T> static void quantize(T *vector, size_t dim, float max, float offset, float scale) {
    auto fmax = max + 1.0;
    for (size_t i = 0; i < dim; i++) {
      float fv  = static_cast<float>(vector[i]);
      fv        = floorf((fv - offset) / scale * fmax);
      fv        = fv < 0 ? 0 : fv;
      fv        = fv > max ? max : fv;
      vector[i] = static_cast<T>(fv);
    }
  }

  template <typename T> static void quantize(std::vector<T> &vector, float max, float offset, float scale) {
    quantize(vector.data(), vector.size(), max, offset, scale);
  }

  template <typename T>
  static void dequantize(T *vector, uint8_t *cvector, size_t dimension, float max, float offset,
                         float scale) {
    auto fmax = max + 1.0;
    for (size_t i = 0; i < dimension; i++) {
      float fv  = static_cast<float>(cvector[i]) + 0.5;
      fv        = (fv / fmax) * scale + offset;
      vector[i] = static_cast<T>(fv);
    }
  }

  template <typename T>
  static void dequantize(std::vector<T> &vector, uint8_t *cvector, size_t dimension, float max, float offset,
                         float scale) {
    vector.resize(dimension);
    dequantize(vector.data(), cvector, vector.size(), max, offset, scale);
  }

  template <typename T>
  void quantizeToQint8(std::vector<T> &vector, float offset, float scale, bool shift = false) {
    quantizeToQint8(vector, getObjectType(), getDimension(), offset, scale, shift);
  }

  template <typename T>
  static void quantizeToQint8(std::vector<T> &vector, const std::type_info &t, size_t dimension, float offset,
                              float scale, bool shift = false) {
    if (t == typeid(qsint8)) {
      quantizeSymmetrically(vector, 127.0, scale);
      if (shift) {
        for (size_t i = 0; i < dimension; i++) {
          vector[i] += 127;
        }
      }
    } else {
      std::stringstream msg;
      msg << "not supported type. " << t.name();
      NGTThrowException(msg);
    }
  }
  template <typename T> void quantizeToQint8(std::vector<T> &vector, bool shift = false) {
    if (quantizationOffset == 0.0 && quantizationScale == 0.0) {
      NGTThrowException("Error. Quantization parameters are not set yet.");
    }
    quantizeToQint8(vector, quantizationOffset, quantizationScale, shift);
  }
  static void quantizeToQint8(float *vector, size_t dimension, uint8_t *cvector, ObjectType type,
                              float offset, float scale, bool shift = false) {
    if (type == Qsuint8) {
      quantizeSymmetrically(vector, dimension, 127.0, scale);
      if (shift) {
        auto *cv = reinterpret_cast<uint8_t *>(cvector);
        for (size_t i = 0; i < dimension; i++) {
          cv[i] = static_cast<uint8_t>(vector[i] + 127);
        }
      } else {
        auto *cv = reinterpret_cast<int8_t *>(cvector);
        for (size_t i = 0; i < dimension; i++) {
          cv[i] = static_cast<int8_t>(vector[i]);
        }
      }
    } else {
      std::stringstream msg;
      msg << "not supported type. " << type;
      NGTThrowException(msg);
    }
  }
  template <typename T>
  void dequantizeFromQint8(std::vector<T> &vector, uint8_t *cvector, bool shift = false) {
    dequantizeFromQint8(vector, cvector, dimension, getObjectType(), quantizationOffset, quantizationScale,
                        shift);
  }
  template <typename T>
  static void dequantizeFromQint8(std::vector<T> &vector, uint8_t *cvector, size_t dimension,
                                  const std::type_info &t, float offset, float scale, bool shift = false) {
    if (t == typeid(qsint8)) {
      dequantizeSymmetrically(vector, reinterpret_cast<int8_t *>(cvector), dimension, 127.0, scale);
      if (shift) {
        auto *cv = reinterpret_cast<uint8_t *>(cvector);
        for (size_t i = 0; i < dimension; i++) {
          cv[i] = static_cast<uint8_t>(vector[i] + 127);
        }
      } else {
        auto *cv = reinterpret_cast<int8_t *>(cvector);
        for (size_t i = 0; i < dimension; i++) {
          cv[i] = static_cast<int8_t>(vector[i]);
        }
      }
    } else {
      std::stringstream msg;
      msg << "not supported type. " << t.name();
      NGTThrowException(msg);
    }
  }
  bool isQintObjectType() {
    const std::type_info &t = getObjectType();
    if (t == typeid(qsint8)) return true;
    return false;
  }
  bool isNormalizedDistance() { return normalization; }
  NGT::Distance compareWithL1(NGT::Object &o1, NGT::Object &o2);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  NGT::Distance compareWithL1(NGT::Object &o1, NGT::PersistentObject &o2);
#endif
  static size_t getDimensionForType(const std::type_info &ot, size_t d) {
#ifdef NGT_PQ4
    return ot == typeid(qint4) ? (d + 1) / 2 : d;
#else
    return d;
#endif
  }

  static NGT::ObjectSpace *convertObjectSpace(ObjectSpace &srcObjectSpace, ObjectType targetDataType,
                                              float maxMagnitude = 0.0);
  ObjectType getEstimatedObjectType();

#ifdef NGT_PQ4
  Quantizer &getQuantizer() { return quantizer; }
  NGT::Object *allocateQuantizedQuery(std::vector<float> &object);
  void openQuantizer(const std::string &path) {
    if (pq4IsEnabled()) {
      try {
        quantizer.open(path);
      } catch (NGT::Exception &err) {
      }
    }
  }
#endif
 protected:
  const size_t dimension;
  DistanceType distanceType;
  Comparator *comparator;
  Comparator *comparatorForSearch;
  bool normalization;
  int32_t prefetchOffset;
  int32_t prefetchSize;
  float quantizationScale;
  float quantizationOffset;
  float magnitude;
#ifdef NGT_PQ4
  Quantizer quantizer;
#endif
};

class BaseObject {
 public:
  virtual uint8_t &operator[](size_t idx) const = 0;
  void serialize(std::ostream &os, ObjectSpace *objectspace = 0) {
    if (objectspace == 0) {
      NGTThrowException("Object: objectspace is null");
    }
    size_t byteSize = objectspace->getByteSizeOfObject();
    NGT::Serializer::write(os, (uint8_t *)&(*this)[0], byteSize);
  }
  void deserialize(std::istream &is, ObjectSpace *objectspace = 0) {
    if (objectspace == 0) {
      NGTThrowException("Object: objectspace is null");
    }
    size_t byteSize = objectspace->getByteSizeOfObject();
    assert(&(*this)[0] != 0);
    NGT::Serializer::read(is, (uint8_t *)&(*this)[0], byteSize);
    if (is.eof()) {
      std::stringstream msg;
      msg << "ObjectSpace::BaseObject: Fatal Error! Read beyond the end of the object file. The object file "
             "is corrupted? "
          << byteSize;
      NGTThrowException(msg);
    }
  }
  void serializeAsText(std::ostream &os, ObjectSpace *objectspace = 0) {
    if (objectspace == 0) {
      NGTThrowException("Object: objectspace is null");
    }
    const std::type_info &t = objectspace->getObjectType();
    size_t dimension        = objectspace->getDimension();
    void *ref               = (void *)&(*this)[0];
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
      std::cerr << "Object::serializeAsText: not supported data type. [" << t.name() << "]" << std::endl;
      assert(0);
    }
  }
  void deserializeAsText(std::ifstream &is, ObjectSpace *objectspace = 0) {
    if (objectspace == 0) {
      NGTThrowException("Object: objectspace is null");
    }
    const std::type_info &t = objectspace->getObjectType();
    size_t dimension        = objectspace->getDimension();
    void *ref               = (void *)&(*this)[0];
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
  template <typename T> void set(std::vector<T> &v, ObjectSpace &objectspace) {
    const std::type_info &t = objectspace.getObjectType();
    size_t dimension        = objectspace.getDimension();
    void *ref               = (void *)&(*this)[0];
    if (ref == 0) {
      NGTThrowException("BaseObject::set: vector is null");
    }
    if (t == typeid(uint8_t)) {
      for (size_t d = 0; d < dimension; d++) {
        *(static_cast<uint8_t *>(ref) + d) = v[d];
      }
    } else if (t == typeid(qsint8)) {
      for (size_t d = 0; d < dimension; d++) {
        *(static_cast<int8_t *>(ref) + d) = v[d];
      }
    } else if (t == typeid(float)) {
      for (size_t d = 0; d < dimension; d++) {
        *(static_cast<float *>(ref) + d) = v[d];
      }
#ifdef NGT_HALF_FLOAT
    } else if (t == typeid(float16)) {
      for (size_t d = 0; d < dimension; d++) {
        *(static_cast<float16 *>(ref) + d) = v[d];
      }
#endif
    } else if (t == typeid(double)) {
      for (size_t d = 0; d < dimension; d++) {
        *(static_cast<double *>(ref) + d) = v[d];
      }
    } else if (t == typeid(uint16_t)) {
      for (size_t d = 0; d < dimension; d++) {
        *(static_cast<uint16_t *>(ref) + d) = v[d];
      }
    } else if (t == typeid(uint32_t)) {
      for (size_t d = 0; d < dimension; d++) {
        *(static_cast<uint32_t *>(ref) + d) = v[d];
      }
    } else {
      std::stringstream msg;
      msg << "BaseObject::set: not supported data type. [" << t.name() << "]";
      NGTThrowException(msg);
    }
  }
};

class Object : public BaseObject {
 public:
  Object(NGT::ObjectSpace *os = 0) : vector(0) {
    if (os == 0) {
      return;
    }
    size_t s = os->getByteSizeOfObject();
    construct(s);
  }

  template <typename T> Object(std::vector<T> v, NGT::ObjectSpace &os) : vector(0) {
    size_t s = os.getByteSizeOfObject();
    construct(s);
    set(v, os);
  }

  Object(size_t s) : vector(0) {
    assert(s != 0);
    construct(s);
  }

#ifdef NGT_PQ4
  Object(void *p) { vector = static_cast<uint8_t *>(p); }
#endif

  virtual ~Object() { clear(); }

  void attach(void *ptr) { vector = static_cast<uint8_t *>(ptr); }
  void detach() { vector = 0; }

  void copy(Object &o, size_t s) {
    assert(vector != 0);
    for (size_t i = 0; i < s; i++) {
      vector[i] = o[i];
    }
  }

  uint8_t &operator[](size_t idx) const { return vector[idx]; }

  void *getPointer(size_t idx = 0) const { return vector + idx; }

  bool isEmpty() { return vector == 0; }

  static Object *allocate(ObjectSpace &objectspace) { return new Object(&objectspace); }

 private:
  void clear() {
    if (vector != 0) {
      MemoryCache::alignedFree(vector);
    }
    vector = 0;
  }

  void construct(size_t s) {
    assert(vector == 0);
    size_t allocsize = ((s - 1) / 64 + 1) * 64;
    vector           = static_cast<uint8_t *>(MemoryCache::alignedAlloc(allocsize));
    memset(vector, 0, allocsize);
  }

  uint8_t *vector;
};

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
class PersistentObject : public BaseObject {
 public:
  PersistentObject(SharedMemoryAllocator &allocator, NGT::ObjectSpace *os = 0) : array(0) {
    assert(os != 0);
    size_t s = os->getByteSizeOfObject();
    construct(s, allocator);
  }
  PersistentObject(SharedMemoryAllocator &allocator, size_t s) : array(0) {
    assert(s != 0);
    construct(s, allocator);
  }

  ~PersistentObject() {}

  uint8_t &at(size_t idx, SharedMemoryAllocator &allocator) const {
    uint8_t *a = (uint8_t *)allocator.getAddr(array);
    return a[idx];
  }
  uint8_t &operator[](size_t idx) const {
    std::cerr << "not implemented" << std::endl;
    assert(0);
    uint8_t *a = 0;
    return a[idx];
  }

  void *getPointer(SharedMemoryAllocator &allocator) { return getPointer(0, allocator); }
  void *getPointer(size_t idx, SharedMemoryAllocator &allocator) {
    uint8_t *a = (uint8_t *)allocator.getAddr(array);
    return a + idx;
  }

  // set v in objectspace to this object using allocator.
  void set(PersistentObject &po, ObjectSpace &objectspace);

  static off_t allocate(ObjectSpace &objectspace);

  void serializeAsText(std::ostream &os, SharedMemoryAllocator &allocator, ObjectSpace *objectspace = 0) {
    serializeAsText(os, objectspace);
  }

  void serializeAsText(std::ostream &os, ObjectSpace *objectspace = 0);

  void deserializeAsText(std::ifstream &is, SharedMemoryAllocator &allocator, ObjectSpace *objectspace = 0) {
    deserializeAsText(is, objectspace);
  }

  void deserializeAsText(std::ifstream &is, ObjectSpace *objectspace = 0);

  void serialize(std::ostream &os, SharedMemoryAllocator &allocator, ObjectSpace *objectspace = 0) {
    std::cerr << "serialize is not implemented" << std::endl;
    assert(0);
  }

 private:
  void construct(size_t s, SharedMemoryAllocator &allocator) {
    assert(array == 0);
    assert(s != 0);
    size_t allocsize = ((s - 1) / 64 + 1) * 64;
    array            = allocator.getOffset(new (allocator) uint8_t[allocsize]);
    memset(getPointer(0, allocator), 0, allocsize);
  }
  off_t array;
};
#endif // NGT_SHARED_MEMORY_ALLOCATOR

} // namespace NGT
