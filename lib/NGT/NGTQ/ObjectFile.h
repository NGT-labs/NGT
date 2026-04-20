//
// Copyright (C) 2021 Yahoo Japan Corporation
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

#include <fstream>
#include <string>
#include <cstddef>
#include <stdint.h>
#include <iostream>
#include <stdexcept>
#include <cerrno>
#include <cstring>
#include "NGT/ArrayFile.h"

namespace NGT {
class ObjectSpace;
};

class ObjectFile : public ArrayFile<NGT::Object> {
 public:
  enum DataType { DataTypeUint8 = 0, DataTypeFloat = 1, DataTypeFloat16 = 2, DataTypeNone = 99 };

  ObjectFile() : objectSpace(0) {}
  ObjectFile(const ObjectFile &of) : objectSpace(0) {
    fileName        = of.fileName;
    dataType        = of.dataType;
    distanceType    = of.distanceType;
    pseudoDimension = of.pseudoDimension;
  }
  ~ObjectFile() {
    closeMultipleStreams();
    close();
    delete objectSpace;
  }

  bool open() {
    if (!ArrayFile<NGT::Object>::open(fileName)) {
      std::stringstream msg;
      msg << "ObjectFile::Cannot open the specified file. " << fileName;
      NGTThrowException(msg);
      return false;
    }
    switch (dataType) {
    case DataTypeFloat:
      genuineDimension = ArrayFile<NGT::Object>::_fileHead.recordSize / sizeof(float);
      objectSpace =
          new NGT::ObjectSpaceRepository<float, double>(genuineDimension, typeid(float), distanceType);
      break;
    case DataTypeUint8:
      genuineDimension = ArrayFile<NGT::Object>::_fileHead.recordSize / sizeof(uint8_t);
      objectSpace =
          new NGT::ObjectSpaceRepository<unsigned char, int>(genuineDimension, typeid(uint8_t), distanceType);
      break;
#ifdef NGT_HALF_FLOAT
    case DataTypeFloat16:
      genuineDimension = ArrayFile<NGT::Object>::_fileHead.recordSize / sizeof(NGT::float16);
      objectSpace = new NGT::ObjectSpaceRepository<NGT::float16, float>(genuineDimension,
                                                                        typeid(NGT::float16), distanceType);
      break;
#endif
    default:
      std::stringstream msg;
      msg << "ObjectFile::Invalid Object Type in the property. " << dataType;
      NGTThrowException(msg);
      break;
    }
    return true;
  }

  bool open(const std::string &file, DataType datat, NGT::Index::Property::DistanceType distt,
            size_t pseudoDim) {
    dataType        = datat;
    fileName        = file;
    distanceType    = distt;
    pseudoDimension = pseudoDim;
    return open();
  }

  bool openMultipleStreams(const size_t nOfStreams) {
    if (!isOpen()) {
      return false;
    }
    if (!objectFiles.empty()) {
      std::cerr << "ObjectFile::openMultipleStreams : already opened multiple streams. close and reopen. # "
                   "of streams="
                << nOfStreams << std::endl;
      closeMultipleStreams();
    }
    for (size_t i = 0; i < nOfStreams; i++) {
      auto *of = new ObjectFile(*this);
      if (!of->open()) {
        std::cerr << "ObjectFile::openMultipleStreams: Cannot open. " << fileName << std::endl;
        return false;
      }
      objectFiles.push_back(of);
    }
    return true;
  }

  void closeMultipleStreams() {
    for (auto *i : objectFiles) {
      i->close();
      delete i;
      i = 0;
    }
    objectFiles.clear();
  }

  template <typename T>
  bool get(const size_t streamID, size_t id, std::vector<T> &data, NGT::ObjectSpace *objectSpace = 0) {
    if (streamID >= objectFiles.size()) {
      std::cerr << "ObjectFile::streamID is invalid. " << streamID << ":" << objectFiles.size() << std::endl;
      return false;
    }
    if (!objectFiles[streamID]->get(id, data)) {
      return false;
    }
    return true;
  }

  template <typename T> bool get(const size_t id, std::vector<T> &data, NGT::ObjectSpace *os = 0) {
    if (objectSpace == 0) {
      std::stringstream msg;
      msg << "ObjectFile::Fatal Error. objectSpace is not set." << std::endl;
      NGTThrowException(msg);
    }
    NGT::Object *object = objectSpace->allocateObject();
    if (!ArrayFile<NGT::Object>::get(id, *object, objectSpace)) {
      objectSpace->deleteObject(object);
      return false;
    }
    const std::type_info &otype = objectSpace->getObjectType();
    size_t dim                  = objectSpace->getDimension();
    data.resize(pseudoDimension);
    if (typeid(T) == otype) {
      auto *v = object->getPointer();
      memcpy(data.data(), v, sizeof(T) * dim);
    } else if (otype == typeid(uint8_t)) {
      auto *v = static_cast<uint8_t *>(object->getPointer());
      for (size_t i = 0; i < dim; i++) {
        data[i] = v[i];
      }
    } else if (otype == typeid(NGT::float16)) {
      auto *v = static_cast<NGT::float16 *>(object->getPointer());
      for (size_t i = 0; i < dim; i++) {
        data[i] = v[i];
      }
    } else if (otype == typeid(float)) {
      auto *v = static_cast<float *>(object->getPointer());
      for (size_t i = 0; i < dim; i++) {
        data[i] = v[i];
      }
    }
    for (size_t i = dim; i < pseudoDimension; i++) {
      data[i] = 0;
    }
    objectSpace->deleteObject(object);
    return true;
  }

  void put(const size_t id, std::vector<float> &data, NGT::ObjectSpace *os = 0) {
    if (objectSpace == 0) {
      std::stringstream msg;
      msg << "ObjectFile::Fatal Error. objectSpace is not set." << std::endl;
      NGTThrowException(msg);
    }
    if (objectSpace->getDimension() != data.size()) {
      std::stringstream msg;
      msg << "ObjectFile::Dimensions are inconsistent. " << objectSpace->getDimension() << ":" << data.size();
      NGTThrowException(msg);
    }
    NGT::Object *object         = objectSpace->allocateObject();
    const std::type_info &otype = objectSpace->getObjectType();
    if (otype == typeid(uint8_t)) {
      auto *v = static_cast<uint8_t *>(object->getPointer());
      for (size_t i = 0; i < data.size(); i++) {
        v[i] = data[i];
      }
    } else if (otype == typeid(NGT::float16)) {
      auto *v = static_cast<NGT::float16 *>(object->getPointer());
      for (size_t i = 0; i < data.size(); i++) {
        v[i] = data[i];
      }
    } else if (otype == typeid(float)) {
      auto *v = static_cast<float *>(object->getPointer());
      memcpy(v, data.data(), sizeof(float) * data.size());
    }
    ArrayFile<NGT::Object>::put(id, *object, objectSpace);
    objectSpace->deleteObject(object);
    return;
  }

  void put(const size_t id, NGT::Object &data, NGT::ObjectSpace *os = 0) {
    return ArrayFile<NGT::Object>::put(id, data, objectSpace);
  }

  bool get(size_t id, NGT::Object &data, NGT::ObjectSpace *os = 0) {
    return ArrayFile<NGT::Object>::get(id, data, objectSpace);
  }

 public:
  std::string fileName;
  size_t pseudoDimension;
  size_t genuineDimension;
  DataType dataType;
  NGT::ObjectSpace::DistanceType distanceType;
  NGT::ObjectSpace *objectSpace;
  std::vector<ObjectFile *> objectFiles;
};

template <class TYPE> class StaticObjectFile {
 private:
  enum Type { TypeFloat = 0, TypeUint8 = 1, TypeInt8 = 2 };
  struct FileHeadStruct {
    uint32_t noOfObjects;
    uint32_t noOfDimensions;
  };

  bool _isOpen;
  std::ifstream _stream;
  FileHeadStruct _fileHead;

  uint32_t _recordSize;
  uint32_t _sizeOfElement;
  std::string _typeName;
  Type _type;
  std::string _objectPath;
  std::string _fileName;

  bool _readFileHead();

  size_t _pseudoDimension;
  std::vector<StaticObjectFile<TYPE> *> _objectFiles;

 public:
  StaticObjectFile();
  ~StaticObjectFile();
  bool create(const std::string &file, const std::string &objectPath);
  bool open(const std::string &file, const size_t pseudoDimension = 0);
  void close();
  size_t insert(TYPE &data, NGT::ObjectSpace *objectSpace = 0) {
    std::cerr << "insert: not implemented.";
    abort();
  }
  void put(const size_t id, TYPE &data, NGT::ObjectSpace *objectSpace = 0) {
    std::cerr << "put: not implemented.";
    abort();
  }
  bool get(size_t id, std::vector<float> &data, NGT::ObjectSpace *objectSpace = 0);
  bool get(const size_t streamID, size_t id, std::vector<float> &data, NGT::ObjectSpace *objectSpace = 0);
  bool get(size_t id, TYPE &data, NGT::ObjectSpace *objectSpace = 0);
  bool get(const size_t streamID, size_t id, TYPE &data, NGT::ObjectSpace *objectSpace = 0);
  void remove(const size_t id) {
    std::cerr << "remove: not implemented.";
    abort();
  }
  bool isOpen() const;
  size_t size();
  size_t getRecordSize() { return _recordSize; }
  bool openMultipleStreams(const size_t nOfStreams);
  void closeMultipleStreams();
};

class StaticObjectFileLoader {
 public:
  StaticObjectFileLoader(const std::string &path, std::string t = "") {
    if (path.find(".u8bin") != std::string::npos || t == "uint8") {
      std::cerr << "type=u8bin" << std::endl;
      type         = "u8";
      sizeOfObject = 1;
    } else if (path.find(".i8bin") != std::string::npos || t == "int8") {
      std::cerr << "type=i8bin" << std::endl;
      type         = "i8";
      sizeOfObject = 1;
    } else if (path.find(".fbin") != std::string::npos || t == "float32") {
      std::cerr << "type=fbin" << std::endl;
      type         = "f";
      sizeOfObject = 4;
    } else {
      std::cerr << "no specified data type. float32 is used as data type." << std::endl;
      type         = "f";
      sizeOfObject = 4;
    }
    stream.open(path, std::ios::in | std::ios::binary);
    if (!stream) {
      std::cerr << "qbg: Error! " << path << std::endl;
      return;
    }
    stream.read(reinterpret_cast<char *>(&noOfObjects), sizeof(noOfObjects));
    stream.read(reinterpret_cast<char *>(&noOfDimensions), sizeof(noOfDimensions));
    sizeOfObject *= noOfDimensions;
    std::cerr << "# of objects=" << noOfObjects << std::endl;
    std::cerr << "# of dimensions=" << noOfDimensions << std::endl;
    counter = 0;
  }

  void seek(size_t id) {
    if (noOfObjects <= id) {
      id = noOfObjects - 1;
    }
    size_t headerSize = sizeof(noOfObjects) + sizeof(noOfDimensions);
    stream.seekg(id * sizeOfObject + headerSize, std::ios_base::beg);
    counter = id;
    return;
  }

  std::vector<float> getObject() {
    std::vector<float> object;
    if (isEmpty()) {
      return object;
    }
    if (type == "u8") {
      for (uint32_t dimidx = 0; dimidx < noOfDimensions; dimidx++) {
        uint8_t v;
        stream.read(reinterpret_cast<char *>(&v), sizeof(v));
        if (stream.eof()) {
          std::cerr << "something wrong" << std::endl;
          return object;
        }
        object.push_back(v);
      }
    } else if (type == "i8") {
      for (uint32_t dimidx = 0; dimidx < noOfDimensions; dimidx++) {
        int8_t v;
        stream.read(reinterpret_cast<char *>(&v), sizeof(v));
        if (stream.eof()) {
          std::cerr << "something wrong" << std::endl;
          return object;
        }
        object.push_back(v);
      }
    } else if (type == "f") {
      for (uint32_t dimidx = 0; dimidx < noOfDimensions; dimidx++) {
        float v;
        stream.read(reinterpret_cast<char *>(&v), sizeof(v));
        if (stream.eof()) {
          std::cerr << "something wrong" << std::endl;
          return object;
        }
        object.push_back(v);
      }
    } else {
      std::cerr << "Fatal error!!!" << std::endl;
      exit(1);
    }
    counter++;
    return object;
  }
  bool isEmpty() { return noOfObjects <= counter; }
  std::ifstream stream;
  uint32_t noOfObjects;
  uint32_t noOfDimensions;
  uint32_t sizeOfObject;
  uint32_t counter;
  std::string type;
};

// constructor
template <class TYPE> StaticObjectFile<TYPE>::StaticObjectFile() : _isOpen(false) {}

// destructor
template <class TYPE> StaticObjectFile<TYPE>::~StaticObjectFile() { close(); }

template <class TYPE>
bool StaticObjectFile<TYPE>::create(const std::string &file, const std::string &objectPath) {
  {
    _stream.open(objectPath, std::ios::in);
    if (!_stream) {
      std::cerr << "Cannot open " << objectPath << std::endl;
      return false;
    }
    bool ret = _readFileHead();
    if (ret == false) {
      return false;
    }
    _stream.close();
  }
  {
    std::ofstream tmpstream;
    tmpstream.open(file);
    std::vector<std::string> tokens;
    NGT::Common::tokenize(objectPath, tokens, ".");
    tmpstream << _fileHead.noOfObjects << std::endl;
    tmpstream << _fileHead.noOfDimensions << std::endl;
    if (tokens.size() <= 1) {
      std::cerr << "The specifiled object file name has no proper extensions. " << objectPath << " use fbin."
                << std::endl;
      tmpstream << "fbin" << std::endl;
    } else {
      tmpstream << tokens.back() << std::endl;
    }
    tmpstream << objectPath << std::endl;
  }
  return true;
}

template <class TYPE> bool StaticObjectFile<TYPE>::open(const std::string &file, size_t pseudoDimension) {
  _pseudoDimension = pseudoDimension;
  uint32_t noOfObjects;
  uint32_t noOfDimensions;
  _fileName = file;
  {
    std::ifstream tmpstream;
    tmpstream.open(file);
    if (!tmpstream) {
      std::cerr << "Cannot open " << file << std::endl;
      abort();
    }
    tmpstream >> noOfObjects;
    tmpstream >> noOfDimensions;
    tmpstream >> _typeName;
    tmpstream >> _objectPath;
  }

  if (_typeName == "fbin") {
    _sizeOfElement = 4;
    _type          = TypeFloat;
  } else if (_typeName == "u8bin") {
    _sizeOfElement = 1;
    _type          = TypeUint8;
  } else if (_typeName == "i8bin") {
    _sizeOfElement = 1;
    _type          = TypeInt8;
  } else {
    _sizeOfElement = 4;
    _type          = TypeFloat;
  }
  _stream.open(_objectPath, std::ios::in);
  if (!_stream) {
    _isOpen = false;
    return false;
  }
  _isOpen = true;

  bool ret = _readFileHead();
  if (_fileHead.noOfObjects != noOfObjects) {
    std::stringstream msg;
    msg << "Invalid # of objects=" << _fileHead.noOfObjects << ":" << noOfObjects;
    NGTThrowException(msg);
  }
  if (_fileHead.noOfDimensions != noOfDimensions) {
    std::stringstream msg;
    msg << "Invalid # of dimensions=" << _fileHead.noOfDimensions << ":" << noOfDimensions;
    NGTThrowException(msg);
  }
  _recordSize = _sizeOfElement * _fileHead.noOfDimensions;
  return ret;
}

template <class TYPE> bool StaticObjectFile<TYPE>::openMultipleStreams(const size_t nOfStreams) {
  if (!isOpen()) {
    return false;
  }
  if (!_objectFiles.empty()) {
    std::cerr << "StaticObjectFile : already opened multiple streams. close and reopen. # of streams="
              << nOfStreams << std::endl;
    closeMultipleStreams();
  }
  for (size_t i = 0; i < nOfStreams; i++) {
    auto *of = new StaticObjectFile<TYPE>;
    if (!of->open(_fileName, _pseudoDimension)) {
      return false;
    }
    _objectFiles.push_back(of);
  }
  return true;
}

template <class TYPE>
bool StaticObjectFile<TYPE>::get(const size_t streamID, size_t id, std::vector<float> &data,
                                 NGT::ObjectSpace *objectSpace) {
  if (streamID >= _objectFiles.size()) {
    std::cerr << "streamID is invalid. " << streamID << ":" << _objectFiles.size() << std::endl;
    return false;
  }
  if (!_objectFiles[streamID]->get(id, data, objectSpace)) {
    return false;
  }
  return true;
}

template <class TYPE>
bool StaticObjectFile<TYPE>::get(const size_t streamID, size_t id, TYPE &data,
                                 NGT::ObjectSpace *objectSpace) {
  if (streamID >= _objectFiles.size()) {
    std::cerr << "streamID is invalid. " << streamID << ":" << _objectFiles.size() << std::endl;
    return false;
  }
  if (!_objectFiles[streamID]->get(id, data, objectSpace)) {
    return false;
  }
  return true;
}

template <class TYPE> void StaticObjectFile<TYPE>::close() {
  _stream.close();
  _isOpen = false;
  closeMultipleStreams();
}

template <class TYPE> void StaticObjectFile<TYPE>::closeMultipleStreams() {
  for (auto *i : _objectFiles) {
    i->close();
    delete i;
    i = 0;
  }
  _objectFiles.clear();
}

template <class TYPE> bool StaticObjectFile<TYPE>::get(size_t id, TYPE &data, NGT::ObjectSpace *objectSpace) {
  std::vector<float> record;
  bool stat = get(id, record, objectSpace);
  std::stringstream object;
  object.write(reinterpret_cast<char *>(record.data()), record.size() * sizeof(float));
  data.deserialize(object, objectSpace);
  return stat;
}

template <class TYPE>
bool StaticObjectFile<TYPE>::get(size_t id, std::vector<float> &data, NGT::ObjectSpace *objectSpace) {
  id--;
  if (size() <= id) {
    return false;
  }
  uint64_t offset_pos = id * _recordSize + sizeof(FileHeadStruct);
  _stream.seekg(offset_pos, std::ios::beg);
  if (!_stream.fail()) {
    switch (_type) {
    case TypeFloat: {
      auto dim = _pseudoDimension;
      if (dim == 0) {
        dim = _recordSize / sizeof(float);
      }
      if (_recordSize > dim * sizeof(float)) {
        abort();
      }
      data.resize(dim, 0.0);
      _stream.read(reinterpret_cast<char *>(data.data()), _recordSize);
      break;
    }
    case TypeUint8:
    case TypeInt8: {
      auto dim = _pseudoDimension;
      if (dim == 0) {
        dim = _recordSize;
      }
      uint8_t src[_recordSize];
      _stream.read(reinterpret_cast<char *>(src), _recordSize);
      data.resize(dim, 0.0);
      if (_type == TypeUint8) {
        for (size_t i = 0; i < _recordSize; i++) {
          data[i] = static_cast<float>(src[i]);
        }
      } else {
        int8_t *intsrc = reinterpret_cast<int8_t *>(&src[0]);
        for (size_t i = 0; i < _recordSize; i++) {
          data[i] = static_cast<float>(intsrc[i]);
        }
      }
    } break;
    }
  } else {
    std::cerr << "StaticObjectFile::get something wrong! id=" << id << " type=" << _type << std::endl;
    abort();
  }

  return true;
}

template <class TYPE> bool StaticObjectFile<TYPE>::isOpen() const { return _isOpen; }

template <class TYPE> size_t StaticObjectFile<TYPE>::size() {
  _stream.seekg(0, std::ios::end);
  int64_t offset_pos = _stream.tellg();
  offset_pos -= sizeof(FileHeadStruct);
  size_t num = offset_pos / _recordSize;
  num++;
  return num;
}

template <class TYPE> bool StaticObjectFile<TYPE>::_readFileHead() {
  _stream.seekg(0, std::ios::beg);
  _stream.read((char *)(&_fileHead), sizeof(FileHeadStruct));
  if (_stream.bad()) {
    return false;
  }
  return true;
}
