//
// Copyright (C) 2015 Yahoo Japan Corporation
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

#include <algorithm>

#if __cplusplus >= 201703L
#include <filesystem>
#else
#include <experimental/filesystem>
#endif

#include <random>

#include "NGT/defines.h"
#include "NGT/Common.h"
#include "NGT/ObjectSpaceRepository.h"
#include "NGT/Index.h"
#include "NGT/Thread.h"
#include "NGT/GraphReconstructor.h"
#include "NGT/Version.h"
#include "NGT/NGTQ/ObjectFile.h"

using namespace std;
using namespace NGT;

void Index::version(ostream &os) {
  os << "libngt:" << endl;
  Version::get(os);
}

string Index::getVersion() { return Version::getVersion(); }

size_t Index::getDimension() { return static_cast<NGT::GraphIndex &>(getIndex()).getProperty().dimension; }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
NGT::Index::Index(NGT::Property &prop, const string &database) : redirect(false) {
  if (prop.dimension == 0) {
    NGTThrowException("Index::Index. Dimension is not specified.");
  }
  Index *idx = 0;
  mkdir(database);
  if (prop.indexType == NGT::Index::Property::GraphAndTree) {
    idx = new NGT::GraphAndTreeIndex(database, prop);
  } else if (prop.indexType == NGT::Index::Property::Graph) {
    idx = new NGT::GraphIndex(database, prop);
  } else {
    NGTThrowException("Index::Index: Not found IndexType in property file.");
  }
  if (idx == 0) {
    stringstream msg;
    msg << "Index::Index: Cannot construct. ";
    NGTThrowException(msg);
  }
  index = idx;
  path  = "";
}
#else
NGT::Index::Index(NGT::Property &prop) : redirect(false) {
  if (prop.dimension == 0) {
    NGTThrowException("Index::Index. Dimension is not specified.");
  }
  Index *idx = 0;
  if (prop.indexType == NGT::Index::Property::GraphAndTree) {
    idx = new NGT::GraphAndTreeIndex(prop);
  } else if (prop.indexType == NGT::Index::Property::Graph) {
    idx = new NGT::GraphIndex(prop);
  } else {
    NGTThrowException("Index::Index: Not found IndexType in property file.");
  }
  if (idx == 0) {
    stringstream msg;
    msg << "Index::Index: Cannot construct. ";
    NGTThrowException(msg);
  }
  index = idx;
  path  = "";
}
#endif

float NGT::Index::getEpsilonFromExpectedAccuracy(double accuracy) {
  return static_cast<NGT::GraphIndex &>(getIndex()).getEpsilonFromExpectedAccuracy(accuracy);
}

void NGT::Index::open(const string &database, bool rdOnly, NGT::Index::OpenType openType) {
  NGT::Property prop;
  prop.load(database);
  Index *idx = 0;
  if ((prop.indexType == NGT::Index::Property::GraphAndTree) &&
      ((openType & NGT::Index::OpenTypeGraphDisabled) == 0)) {
    idx = new NGT::GraphAndTreeIndex(database, prop, rdOnly, openType);
  } else if ((prop.indexType == NGT::Index::Property::Graph) ||
             ((openType & NGT::Index::OpenTypeGraphDisabled) != 0)) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    idx = new NGT::GraphIndex(database, rdOnly, openType);
#else
    idx = new NGT::GraphIndex(database, rdOnly, openType);
#endif
  } else {
    NGTThrowException("Index::Open: Not found IndexType in property file.");
  }
  if (idx == 0) {
    stringstream msg;
    msg << "Index::open: Cannot open. " << database;
    NGTThrowException(msg);
  }
  index = idx;
  path  = database;
}

void NGT::Index::createGraphAndTree(const string &database, NGT::Property &prop, const string &dataFile,
                                    size_t dataSize, bool redirect) {
  if (prop.dimension == 0) {
    NGTThrowException("Index::createGraphAndTree. Dimension is not specified.");
  }
  prop.indexType = NGT::Index::Property::IndexType::GraphAndTree;
  Index *idx     = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  idx = new NGT::Index(prop, database);
#else
  idx = new NGT::Index(prop);
#endif
  idx->redirect = redirect;
  assert(idx != 0);
  StdOstreamRedirector redirector(redirect);
  redirector.begin();
  try {
    if (idx->getObjectSpace().isQintObjectType()) {
      idx->saveIndex(database);
      idx->close();
      auto append     = true;
      auto refinement = false;
      if (!dataFile.empty()) {
        appendFromTextObjectFile(database, dataFile, dataSize, append, refinement, prop.threadPoolSize);
      }
    } else {
      loadAndCreateIndex(*idx, database, dataFile, prop.threadPoolSize, dataSize);
    }
  } catch (Exception &err) {
    delete idx;
    redirector.end();
    throw err;
  }
  delete idx;
  redirector.end();
}

void NGT::Index::create(const string &indexPath, const string &srcIndex, NGT::Property *updateProp,
                        bool redirect) {
  StdOstreamRedirector redirector(redirect);
  redirector.begin();
  NGT::Property prop;
  prop.load(srcIndex);
  if (prop.dimension == 0) {
    NGTThrowException("Index::createGraphAndTree. Dimension is not specified.");
  }
  if (updateProp != nullptr) {
    prop.set(*updateProp);
  }
  try {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    mkdir(indexPath);
    NGT::GraphAndTreeIndex index(indexPath, prop);
#else
    NGT::GraphAndTreeIndex index(prop);
#endif
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
    index.save(indexPath);
#endif
    index.close();
  } catch (Exception &err) {
    redirector.end();
    throw err;
  }
}

void NGT::Index::createGraph(const string &database, NGT::Property &prop, const string &dataFile,
                             size_t dataSize, bool redirect) {
  if (prop.dimension == 0) {
    NGTThrowException("Index::createGraphAndTree. Dimension is not specified.");
  }
  prop.indexType = NGT::Index::Property::IndexType::Graph;
  Index *idx     = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  idx = new NGT::Index(prop, database);
#else
  idx = new NGT::Index(prop);
#endif
  idx->redirect = redirect;
  assert(idx != 0);
  StdOstreamRedirector redirector(redirect);
  redirector.begin();
  try {
    if (idx->getObjectSpace().isQintObjectType()) {
      idx->saveIndex(database);
      idx->close();
      auto append     = true;
      auto refinement = false;
      if (!dataFile.empty()) {
        appendFromTextObjectFile(database, dataFile, dataSize, append, refinement, prop.threadPoolSize);
      }
    } else {
      loadAndCreateIndex(*idx, database, dataFile, prop.threadPoolSize, dataSize);
    }
  } catch (Exception &err) {
    delete idx;
    redirector.end();
    throw err;
  }
  delete idx;
  redirector.end();
}

void NGT::Index::loadAndCreateIndex(Index &index, const string &database, const string &dataFile,
                                    size_t threadSize, size_t dataSize) {
  NGT::Timer timer;
  timer.start();
  if (dataFile.size() != 0) {
    index.load(dataFile, dataSize);
  } else {
    index.saveIndex(database);
    return;
  }
  timer.stop();
  cerr << "loadAndCreateIndex: Data loading time=" << timer.time << " (sec) " << timer.time * 1000.0
       << " (msec)" << endl;
  if (index.getObjectRepositorySize() == 0) {
    NGTThrowException("Index::create: Data file is empty.");
  }
  cerr << "# of objects=" << index.getObjectRepositorySize() - 1 << endl;
  timer.reset();
  timer.start();
  index.createIndex(threadSize);
  timer.stop();
  index.saveIndex(database);
  cerr << "Index creation time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
}

void NGT::Index::append(const string &database, const string &dataFile, size_t threadSize, size_t dataSize) {
  NGT::Index index(database);
  NGT::Timer timer;
  timer.start();
  if (dataFile.size() != 0) {
    index.append(dataFile, dataSize);
  }
  timer.stop();
  cerr << "append: Data loading time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
  cerr << "# of objects=" << index.getObjectRepositorySize() - 1 << endl;
  timer.reset();
  timer.start();
  size_t nOfObjects            = index.getObjectSpace().getRepository().size();
  size_t endOfAppendedObjectID = (nOfObjects == 0 ? 1 : nOfObjects) + dataSize;
  index.createIndex(threadSize, endOfAppendedObjectID);
  timer.stop();
  index.saveIndex(database);
  cerr << "Index creation time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
  return;
}

void NGT::Index::append(const string &database, const float *data, size_t dataSize, size_t threadSize) {
  NGT::Index index(database);
  NGT::Timer timer;
  timer.start();
  if (data != 0 && dataSize != 0) {
    index.append(data, dataSize);
  } else {
    NGTThrowException("Index::append: No data.");
  }
  timer.stop();
  cerr << "Data loading time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
  cerr << "# of objects=" << index.getObjectRepositorySize() - 1 << endl;
  timer.reset();
  timer.start();
  index.createIndex(threadSize);
  timer.stop();
  index.saveIndex(database);
  cerr << "Index creation time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
  return;
}

void NGT::Index::appendFromRefinementObjectFile(const std::string &indexPath, size_t threadSize) {
  NGT::Index index(indexPath);
  index.appendFromRefinementObjectFile();
  index.createIndex(threadSize);
  index.save();
  index.close();
}

void NGT::Index::appendFromRefinementObjectFile() {
  NGT::Property prop;
  getProperty(prop);
  float maxMag    = prop.maxMagnitude;
  bool maxMagSkip = false;
  if (maxMag > 0.0) maxMagSkip = true;
  auto &ros     = getRefinementObjectSpace();
  auto &rrepo   = ros.getRepository();
  size_t dim    = getDimension();
  auto dataSize = rrepo.size();
  std::vector<float> addedElement(dataSize);
  if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
    NGT::Timer timer;
    timer.start();
    for (size_t idx = 1; idx < rrepo.size(); idx++) {
      if (rrepo[idx] == 0) {
        continue;
      }
      std::vector<float> object;
      ros.getObject(idx, object);
      if (object.size() != dim) {
        if (object.size() == dim + 1) {
          object.resize(dim);
        } else {
          std::stringstream msg;
          msg << "Fatal inner error! iInvalid dimension. " << dim << ":" << object.size();
          ;
          NGTThrowException(msg);
        }
      }
      if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
        double mag = 0.0;
        for (auto &v : object) {
          mag += v * v;
        }
        if (!maxMagSkip && mag > maxMag) {
          maxMag = mag;
        }
        addedElement[idx] = mag;
      }
      if (idx % 2000000 == 0) {
        timer.stop();
        std::cerr << "processed " << static_cast<float>(idx) / 1000000.0 << "M objects."
                  << " maxMag=" << maxMag << " time=" << timer << std::endl;
        timer.restart();
      }
    }
    timer.stop();
    std::cerr << "time=" << timer << std::endl;
    std::cerr << "maxMag=" << maxMag << std::endl;
    std::cerr << "dataSize=" << dataSize << std::endl;
    if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
      if (static_cast<NGT::GraphIndex &>(getIndex()).property.maxMagnitude <= 0.0 && maxMag > 0.0) {
        static_cast<NGT::GraphIndex &>(getIndex()).property.maxMagnitude = maxMag;
      }
    }
  }
  if (getObjectSpace().isQintObjectType() && prop.clippingRate >= 0.0) {
    std::priority_queue<float> min;
    std::priority_queue<float, vector<float>, std::greater<float>> max;
    {
      NGT::Timer timer;
      timer.start();
      auto clippingSize = static_cast<float>(dataSize * dim) * prop.clippingRate;
      clippingSize      = clippingSize == 0 ? 1 : clippingSize;
      size_t counter    = 0;
      for (size_t idx = 1; idx < rrepo.size(); idx++) {
        if (rrepo[idx] == 0) continue;
        std::vector<float> object;
        ros.getObject(idx, object);
        if (object.size() != dim) object.resize(dim);
        if (getObjectSpace().isNormalizedDistance()) {
          ObjectSpace::normalize(object);
        }
        if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
          float v = maxMag - addedElement[idx];
          object.emplace_back(sqrt(v >= 0.0 ? v : 0.0));
        }
        for (auto &v : object) {
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
        counter++;
      }
      std::cerr << "time=" << timer << std::endl;
      if (counter != 0) {
        std::cerr << "max:min=" << max.top() << ":" << min.top() << std::endl;
        setQuantizationFromMaxMin(max.top(), min.top());
      }
    }
  }
  {

    for (size_t idx = 1; idx < rrepo.size(); idx++) {
      if (rrepo[idx] == 0) continue;
      std::vector<float> object;
      ros.getObject(idx, object);
      if (object.size() != dim) object.resize(dim);
      if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
        object.emplace_back(sqrt(maxMag - addedElement[idx]));
      }
      append(object);
      if (idx + 1 != getObjectRepositorySize()) {
        std::stringstream msg;
        msg << "The object repository and refinement repository are inconsistent. " << idx + 1 << ":"
            << getObjectRepositorySize();
        NGTThrowException(msg);
      }
    }
  }
}

void NGT::Index::insertFromRefinementObjectFile() {
  NGT::Property prop;
  getProperty(prop);
  float maxMag = prop.maxMagnitude;
  if (prop.maxMagnitude <= 0.0) {
    std::stringstream msg;
    msg << "Max magnitude is not set yet. " << maxMag;
    NGTThrowException(msg);
  }
  auto &ros     = getRefinementObjectSpace();
  auto &rrepo   = ros.getRepository();
  auto &repo    = getObjectSpace().getRepository();
  size_t dim    = getDimension();
  auto dataSize = rrepo.size();
  std::vector<float> addedElement(dataSize);

  for (size_t idx = 1; idx < rrepo.size(); idx++) {
    if (rrepo[idx] == 0) continue;
    if (repo.size() > idx && repo[idx] != 0) continue;
    std::vector<float> object;
    ros.getObject(idx, object);
    if (object.size() != dim) {
      if (object.size() == dim + 1) {
        object.resize(dim);
      } else {
        std::stringstream msg;
        msg << "Fatal inner error! iInvalid dimension. " << dim << ":" << object.size();
        ;
        NGTThrowException(msg);
      }
    }
    if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
      double mag = 0.0;
      for (auto &v : object) {
        mag += v * v;
      }
      if (mag > maxMag) {
        maxMag = mag;
      }
      object.emplace_back(sqrt(maxMag - mag));
    }
    try {
      insert(idx, object);
    } catch (NGT::Exception &err) {
      std::stringstream msg;
      msg << "Cannot insert. " << idx << " " << err.what();
      NGTThrowException(msg);
    }
    if (idx + 1 > getObjectRepositorySize()) {
      std::stringstream msg;
      msg << "The object repository and refinement repository are inconsistent. " << idx + 1 << ":"
          << getObjectRepositorySize();
      NGTThrowException(msg);
    }
  }
}

void NGT::Index::appendFromTextObjectFile(const std::string &indexPath, const std::string &data,
                                          size_t dataSize, bool append, bool refinement, size_t threadSize) {

  NGT::Index index(indexPath);
  index.appendFromTextObjectFile(data, dataSize, append, refinement);
  index.createIndex(threadSize);
  index.save();
  index.close();
}

void NGT::Index::appendFromTextObjectFile(const std::string &data, size_t dataSize, bool append,
                                          bool refinement) {
  StdOstreamRedirector redirector(redirect);
  redirector.begin();
  NGT::Property prop;
  getProperty(prop);
  float maxMag    = prop.maxMagnitude;
  bool maxMagSkip = false;
  if (maxMag > 0.0) maxMagSkip = true;
  std::vector<float> addedElement;
  size_t dim = prop.dimension;
  bool warn  = false;
  if (append && prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
    NGT::Timer timer;
    timer.start();
    ifstream is(data);
    if (!is) {
      std::stringstream msg;
      msg << "Cannot open the specified data file. " << data;
      NGTThrowException(msg);
    }
    std::string line;
    size_t counter = 0;
    while (getline(is, line)) {
      if (dataSize > 0 && counter >= dataSize) break;
      vector<float> object;
      vector<string> tokens;
      NGT::Common::tokenize(line, tokens, "\t, ");
      if (tokens.back() == "") tokens.pop_back();
      if (tokens.size() > dim && warn == false) {
        warn = true;
        std::cerr << "Warning! Invalid dimension of the specified data. The specified data is "
                  << tokens.size() << ". The index is " << dim << "." << std::endl;
        std::cerr << "Cut the tail." << std::endl;
      }
      if (tokens.size() < dim) {
        std::stringstream msg;
        msg << "The dimensions are not inconsist. " << counter << ":" << dim << "x" << tokens.size() << data;
        NGTThrowException(msg);
      }
      if (tokens.size() > dim) {
        tokens.resize(dim);
      }
      if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
        double mag = 0.0;
        for (auto &vstr : tokens) {
          auto v = NGT::Common::strtof(vstr);
          mag += v * v;
        }
        if (!maxMagSkip && mag > maxMag) {
          maxMag = mag;
        }
        addedElement.emplace_back(mag);
      }
      counter++;
      if (counter % 2000000 == 0) {
        timer.stop();
        std::cerr << "processed " << static_cast<float>(counter) / 1000000.0 << "M objects."
                  << " maxMag=" << maxMag << " time=" << timer << std::endl;
        timer.restart();
      }
    }
    timer.stop();
    dataSize = counter;
    std::cerr << "time=" << timer << std::endl;
    std::cerr << "maxMag=" << maxMag << std::endl;
    std::cerr << "dataSize=" << dataSize << std::endl;
    if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
      if (static_cast<NGT::GraphIndex &>(getIndex()).property.maxMagnitude <= 0.0 && maxMag > 0.0) {
        static_cast<NGT::GraphIndex &>(getIndex()).property.maxMagnitude = maxMag;
      }
    }
  }
  if (append && getObjectSpace().isQintObjectType() && prop.clippingRate >= 0.0) {
    std::priority_queue<float> min;
    std::priority_queue<float, vector<float>, std::greater<float>> max;
    {
      NGT::Timer timer;
      timer.start();
      ifstream is(data);
      if (!is) {
        std::stringstream msg;
        msg << "Cannot open the specified data file. " << data;
        NGTThrowException(msg);
      }
      auto clippingSize = static_cast<float>(dataSize * dim) * prop.clippingRate;
      clippingSize      = clippingSize == 0 ? 1 : clippingSize;
      std::string line;
      size_t counter = 0;
      while (getline(is, line)) {
        if (dataSize > 0 && counter >= dataSize) break;
        vector<float> object;
        vector<string> tokens;
        NGT::Common::tokenize(line, tokens, "\t, ");
        if (tokens.back() == "") tokens.pop_back();
        if (tokens.size() > dim && warn == false) {
          warn = true;
          std::cerr << "Warning! Invalid dimension of the specified data. The specified data is "
                    << tokens.size() << ". The index is " << dim << "." << std::endl;
          std::cerr << "Cut the tail." << std::endl;
        }
        for (auto &vstr : tokens) {
          auto v = NGT::Common::strtof(vstr);
          object.emplace_back(v);
        }
        if (object.size() > dim) {
          object.resize(dim);
        }
        if (getObjectSpace().isNormalizedDistance()) {
          ObjectSpace::normalize(object);
        }
        if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
          float v = maxMag - addedElement[counter];
          object.emplace_back(sqrt(v >= 0.0 ? v : 0.0));
        }
        for (auto &v : object) {
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
        counter++;
      }
      timer.stop();
      std::cerr << "time=" << timer << std::endl;
      if (counter != 0) {
        std::cerr << "max:min=" << max.top() << ":" << min.top() << std::endl;
        setQuantizationFromMaxMin(max.top(), min.top());
      }
    }
  }
  if (append || refinement) {

    ifstream is(data);
    if (!is) {
      std::stringstream msg;
      msg << "Cannot open the specified data file. " << data;
      NGTThrowException(msg);
    }
    std::string line;
    size_t counter = 0;
    while (getline(is, line)) {
      if (dataSize > 0 && counter >= dataSize) break;
      vector<float> object;
      vector<string> tokens;
      NGT::Common::tokenize(line, tokens, "\t, ");
      if (tokens.back() == "") tokens.pop_back();
      if (tokens.size() > dim && warn == false) {
        warn = true;
        std::cerr << "Warning! Invalid dimension of the specified data. The specified data is "
                  << tokens.size() << ". The index is " << dim << "." << std::endl;
        std::cerr << "Cut the tail." << std::endl;
      }
      for (auto &vstr : tokens) {
        auto v = NGT::Common::strtof(vstr);
        object.emplace_back(v);
      }
      if (object.size() > dim) {
        object.resize(dim);
      }
#ifdef NGT_REFINEMENT
      if (refinement) {
        appendToRefinement(object);
      }
#endif
      if (append) {
        if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct && maxMag > 0.0) {
          float v = maxMag - addedElement[counter];
          object.emplace_back(sqrt(v >= 0.0 ? v : 0.0));
        }
        NGT::Index::append(object);
      }
      counter++;
    }
  }
  redirector.end();
}

void NGT::Index::appendFromBinaryObjectFile(const std::string &indexPath, const std::string &data,
                                            size_t dataSize, bool append, bool refinement,
                                            size_t threadSize) {
  NGT::Index index(indexPath);
  index.appendFromBinaryObjectFile(data, dataSize, append, refinement);
  index.createIndex(threadSize);
  index.save();
  index.close();
}

void NGT::Index::appendFromBinaryObjectFile(const std::string &data, size_t dataSize, bool append,
                                            bool refinement) {
  NGT::Property prop;
  getProperty(prop);
  float maxMag    = prop.maxMagnitude;
  bool maxMagSkip = false;
  if (maxMag > 0.0) maxMagSkip = true;
  std::vector<float> addedElement;
  size_t dim = 0;
  if (append && prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
    NGT::Timer timer;
    timer.start();
    StaticObjectFileLoader loader(data);
    size_t counter = 0;
    while (!loader.isEmpty()) {
      if (dataSize > 0 && counter >= dataSize) break;
      auto object = loader.getObject();
      if (dim == 0) {
        dim = object.size();
      } else if (dim != object.size()) {
        std::stringstream msg;
        msg << "The dimensions are not inconsist. " << counter << ":" << dim << "x" << object.size() << data;
        NGTThrowException(msg);
      }
      if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
        double mag = 0.0;
        for (auto &v : object) {
          mag += v * v;
        }
        if (!maxMagSkip && mag > maxMag) {
          maxMag = mag;
        }
        addedElement.emplace_back(mag);
      }
      counter++;
      if (counter % 2000000 == 0) {
        timer.stop();
        std::cerr << "processed " << static_cast<float>(counter) / 1000000.0 << "M objects."
                  << " maxMag=" << maxMag << " time=" << timer << std::endl;
        timer.restart();
      }
    }
    timer.stop();
    dataSize = counter;
    std::cerr << "time=" << timer << std::endl;
    std::cerr << "maxMag=" << maxMag << std::endl;
    std::cerr << "dataSize=" << dataSize << std::endl;
    if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
      if (static_cast<NGT::GraphIndex &>(getIndex()).property.maxMagnitude <= 0.0 && maxMag > 0.0) {
        static_cast<NGT::GraphIndex &>(getIndex()).property.maxMagnitude = maxMag;
      }
    }
  }
  if (append && getObjectSpace().isQintObjectType() && prop.clippingRate >= 0.0) {
    std::priority_queue<float> min;
    std::priority_queue<float, vector<float>, std::greater<float>> max;
    {
      NGT::Timer timer;
      timer.start();
      auto clippingSize = static_cast<float>(dataSize * dim) * prop.clippingRate;
      clippingSize      = clippingSize == 0 ? 1 : clippingSize;
      StaticObjectFileLoader loader(data);
      size_t counter = 0;
      while (!loader.isEmpty()) {
        if (dataSize > 0 && counter >= dataSize) break;
        auto object = loader.getObject();
        if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
          float v = maxMag - addedElement[counter];
          object.emplace_back(sqrt(v >= 0.0 ? v : 0.0));
        }
        if (getObjectSpace().isNormalizedDistance()) {
          ObjectSpace::normalize(object);
        }
        for (auto &v : object) {
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
        counter++;
      }
      timer.stop();
      std::cerr << "time=" << timer << std::endl;
      if (counter != 0) {
        std::cerr << "max:min=" << max.top() << ":" << min.top() << std::endl;
        setQuantizationFromMaxMin(max.top(), min.top());
      }
    }
  }
  if (append || refinement) {
    StaticObjectFileLoader loader(data);
    size_t counter = 0;
    while (!loader.isEmpty()) {
      if (dataSize > 0 && counter >= dataSize) break;
      auto object = loader.getObject();
#ifdef NGT_REFINEMENT
      if (refinement) {
        appendToRefinement(object);
      }
#endif
      if (append) {
        if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
          object.emplace_back(sqrt(maxMag - addedElement[counter]));
        }
        NGT::Index::append(object);
      }
      counter++;
    }
  }
}

void NGT::Index::remove(const string &database, vector<ObjectID> &objects, bool force) {
  NGT::Index index(database);
  NGT::Timer timer;
  timer.start();
  for (vector<ObjectID>::iterator i = objects.begin(); i != objects.end(); i++) {
    try {
      index.remove(*i, force);
    } catch (Exception &err) {
      cerr << "Warning: Cannot remove the node. ID=" << *i << " : " << err.what() << endl;
      continue;
    }
  }
  timer.stop();
  cerr << "Data removing time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
  cerr << "# of objects=" << index.getObjectRepositorySize() - 1 << endl;
  index.saveIndex(database);
  return;
}

void NGT::Index::importIndex(const string &database, const string &file) {
  Index *idx = 0;
  NGT::Property property;
  property.importProperty(file);
  NGT::Timer timer;
  timer.start();
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  property.databaseType = NGT::Index::Property::DatabaseType::MemoryMappedFile;
  mkdir(database);
#else
  property.databaseType = NGT::Index::Property::DatabaseType::Memory;
#endif
  if (property.indexType == NGT::Index::Property::IndexType::GraphAndTree) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    idx = new NGT::GraphAndTreeIndex(database, property);
#else
    idx = new NGT::GraphAndTreeIndex(property);
#endif
    assert(idx != 0);
  } else if (property.indexType == NGT::Index::Property::IndexType::Graph) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    idx = new NGT::GraphIndex(database, property);
#else
    idx = new NGT::GraphIndex(property);
#endif
    assert(idx != 0);
  } else {
    NGTThrowException("Index::Open: Not found IndexType in property file.");
  }
  idx->importIndex(file);
  timer.stop();
  cerr << "Data importing time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
  cerr << "# of objects=" << idx->getObjectRepositorySize() - 1 << endl;
  idx->saveIndex(database);
  delete idx;
}

void NGT::Index::exportIndex(const string &database, const string &file) {
  NGT::Index idx(database);
  NGT::Timer timer;
  timer.start();
  idx.exportIndex(file);
  timer.stop();
  cerr << "Data exporting time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
  cerr << "# of objects=" << idx.getObjectRepositorySize() - 1 << endl;
}

void NGT::Index::searchUsingOnlyGraph(NGT::SearchContainer &sc) {
  static_cast<GraphIndex &>(getIndex()).search(sc);
}

void NGT::Index::searchUsingOnlyGraph(NGT::SearchQuery &searchQuery) {
  static_cast<GraphIndex &>(getIndex()).GraphIndex::search(searchQuery);
}

std::vector<float> NGT::Index::makeSparseObject(std::vector<uint32_t> &object) {
  if (static_cast<NGT::GraphIndex &>(getIndex()).getProperty().distanceType !=
      NGT::ObjectSpace::DistanceType::DistanceTypeSparseJaccard) {
    NGTThrowException("NGT::Index::makeSparseObject: Not sparse jaccard.");
  }
  size_t dimension = getObjectSpace().getDimension();
  if (object.size() + 1 > dimension) {
    std::stringstream msg;
    dimension = object.size() + 1;
  }
  std::vector<float> obj(dimension, 0.0);
  for (size_t i = 0; i < object.size(); i++) {
    float fv = *reinterpret_cast<float *>(&object[i]);
    obj[i]   = fv;
  }
  return obj;
}

void NGT::Index::setQuantizationFromMaxMin(float max, float min) {

  float offset;
  float scale;
  if (getObjectSpace().getObjectType() == typeid(NGT::qsint8)) {
    offset = 0.0;
    scale  = std::max(fabs(max), fabs(min));
  } else {
    offset = min;
    scale  = max - offset;
  }
  setQuantization(scale, offset);
}

void NGT::Index::setQuantization(float scale, float offset) {
  static_cast<NGT::GraphIndex &>(getIndex()).property.quantizationScale  = scale;
  static_cast<NGT::GraphIndex &>(getIndex()).property.quantizationOffset = offset;
  getObjectSpace().setQuantization(scale, offset);
}

void NGT::Index::extractInsertionOrder(InsertionOrder &insertionOrder) {
  static_cast<NGT::GraphIndex &>(getIndex()).extractInsertionOrder(insertionOrder);
}

void NGT::Index::createIndex(size_t threadNumber, size_t sizeOfRepository) {
  StdOstreamRedirector redirector(redirect);
  redirector.begin();

  try {
    InsertionOrder insertionOrder;
    NGT::Property prop;
    getProperty(prop);
    if (prop.objectType == NGT::ObjectSpace::ObjectType::Qsuint8) {
      auto &ros = getRefinementObjectSpace();
      auto &os  = getObjectSpace();
      if (static_cast<void *>(&ros) != 0 && ros.getRepository().size() > os.getRepository().size()) {
        if (os.getRepository().size() <= 1) {
          if (ros.getRepository().size() < 100) {
            std::cerr << "Warning! # of refinement objects is too small. " << ros.getRepository().size()
                      << std::endl;
          }
          appendFromRefinementObjectFile();
        } else {
          if (prop.quantizationScale <= 0.0) {
            stringstream msg;
            msg << "Fatal inner error! Scalar quantization parameters are not set yet. "
                << prop.quantizationScale << ":" << prop.quantizationOffset;
            NGTThrowException(msg);
          }
          insertFromRefinementObjectFile();
        }
      }
    } else {
      if (prop.distanceType == ObjectSpace::DistanceTypeInnerProduct) {
        size_t beginId                        = 1;
        NGT::GraphRepository &graphRepository = static_cast<NGT::GraphIndex &>(getIndex()).repository;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        auto &graphNodes       = static_cast<PersistentRepository<GraphNode> &>(graphRepository);
        auto &graphNodeVectors = reinterpret_cast<PersistentRepository<void> &>(graphNodes);
#else
        auto &graphNodes       = static_cast<Repository<GraphNode> &>(graphRepository);
        auto &graphNodeVectors = reinterpret_cast<Repository<void> &>(graphNodes);
#endif
        if (prop.maxMagnitude <= 0.0) {
          getObjectSpace().setMagnitude(prop.maxMagnitude, graphNodeVectors, beginId);
        } else {
          auto maxMag = getObjectSpace().computeMaxMagnitude(beginId);
          static_cast<NGT::GraphIndex &>(getIndex()).property.maxMagnitude = maxMag;
          getObjectSpace().setMagnitude(maxMag, graphNodeVectors, beginId);
        }
      }
    }
    if (prop.nOfNeighborsForInsertionOrder != 0) {
      insertionOrder.nOfNeighboringNodes = prop.nOfNeighborsForInsertionOrder;
      insertionOrder.epsilon             = prop.epsilonForInsertionOrder;
      extractInsertionOrder(insertionOrder);
    }
    createIndexWithInsertionOrder(insertionOrder, threadNumber, sizeOfRepository);
  } catch (Exception &err) {
    redirector.end();
    throw err;
  }
  redirector.end();
}

void NGT::Index::Property::set(NGT::Property &prop) {
  if (prop.dimension != -1) dimension = prop.dimension;
  if (prop.threadPoolSize != -1) threadPoolSize = prop.threadPoolSize;
  if (prop.objectType != ObjectSpace::ObjectTypeNone) objectType = prop.objectType;
#ifdef NGT_REFINEMENT
  if (prop.refinementObjectType != ObjectSpace::ObjectTypeNone)
    refinementObjectType = prop.refinementObjectType;
#endif
  if (prop.distanceType != DistanceType::DistanceTypeNone) distanceType = prop.distanceType;
  if (prop.indexType != IndexTypeNone) indexType = prop.indexType;
  if (prop.databaseType != DatabaseTypeNone) databaseType = prop.databaseType;
  if (prop.objectAlignment != ObjectAlignmentNone) objectAlignment = prop.objectAlignment;
  if (prop.pathAdjustmentInterval != -1) pathAdjustmentInterval = prop.pathAdjustmentInterval;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  if (prop.graphSharedMemorySize != -1) graphSharedMemorySize = prop.graphSharedMemorySize;
  if (prop.treeSharedMemorySize != -1) treeSharedMemorySize = prop.treeSharedMemorySize;
  if (prop.objectSharedMemorySize != -1) objectSharedMemorySize = prop.objectSharedMemorySize;
#endif
  if (prop.prefetchOffset != -1) prefetchOffset = prop.prefetchOffset;
  if (prop.prefetchSize != -1) prefetchSize = prop.prefetchSize;
  if (prop.accuracyTable != "") accuracyTable = prop.accuracyTable;
  if (prop.maxMagnitude != -1) maxMagnitude = prop.maxMagnitude;
  if (prop.quantizationScale != -1) quantizationScale = prop.quantizationScale;
  if (prop.quantizationOffset != -1) quantizationOffset = prop.quantizationOffset;
  if (prop.clippingRate != -1) clippingRate = prop.clippingRate;
  if (prop.nOfNeighborsForInsertionOrder != -1)
    nOfNeighborsForInsertionOrder = prop.nOfNeighborsForInsertionOrder;
  if (prop.epsilonForInsertionOrder != -1) epsilonForInsertionOrder = prop.epsilonForInsertionOrder;
  if (prop.leafNodeSize != -1) leafNodeSize = prop.leafNodeSize;
  if (prop.internalChildrenSize != -1) internalChildrenSize = prop.internalChildrenSize;
}

void NGT::Index::Property::get(NGT::Property &prop) {
  prop.dimension      = dimension;
  prop.threadPoolSize = threadPoolSize;
  prop.objectType     = objectType;
#ifdef NGT_REFINEMENT
  prop.refinementObjectType = refinementObjectType;
#endif
  prop.distanceType           = distanceType;
  prop.indexType              = indexType;
  prop.databaseType           = databaseType;
  prop.pathAdjustmentInterval = pathAdjustmentInterval;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  prop.graphSharedMemorySize  = graphSharedMemorySize;
  prop.treeSharedMemorySize   = treeSharedMemorySize;
  prop.objectSharedMemorySize = objectSharedMemorySize;
#endif
  prop.prefetchOffset                = prefetchOffset;
  prop.prefetchSize                  = prefetchSize;
  prop.accuracyTable                 = accuracyTable;
  prop.maxMagnitude                  = maxMagnitude;
  prop.quantizationScale             = quantizationScale;
  prop.quantizationOffset            = quantizationOffset;
  prop.clippingRate                  = clippingRate;
  prop.nOfNeighborsForInsertionOrder = nOfNeighborsForInsertionOrder;
  prop.epsilonForInsertionOrder      = epsilonForInsertionOrder;
  prop.leafNodeSize                  = leafNodeSize;
  prop.internalChildrenSize          = internalChildrenSize;
}

class CreateIndexJob {
 public:
  CreateIndexJob() {}
  CreateIndexJob &operator=(const CreateIndexJob &d) {
    id       = d.id;
    results  = d.results;
    object   = d.object;
    batchIdx = d.batchIdx;
    return *this;
  }
  friend bool operator<(const CreateIndexJob &ja, const CreateIndexJob &jb) {
    return ja.batchIdx < jb.batchIdx;
  }
  NGT::ObjectID id;
  NGT::Object *object; // this will be a node of the graph later.
  NGT::ObjectDistances *results;
  size_t batchIdx;
};

class CreateIndexSharedData {
 public:
  CreateIndexSharedData(NGT::GraphIndex &nngt) : graphIndex(nngt) {}
  NGT::GraphIndex &graphIndex;
};

class CreateIndexThread : public NGT::Thread {
 public:
  CreateIndexThread() {}
  virtual ~CreateIndexThread() {}
  virtual int run();
};

typedef NGT::ThreadPool<CreateIndexJob, CreateIndexSharedData *, CreateIndexThread> CreateIndexThreadPool;

int CreateIndexThread::run() {

  NGT::ThreadPool<CreateIndexJob, CreateIndexSharedData *, CreateIndexThread>::Thread &poolThread =
      (NGT::ThreadPool<CreateIndexJob, CreateIndexSharedData *, CreateIndexThread>::Thread &)*this;

  CreateIndexSharedData &sd   = *poolThread.getSharedData();
  NGT::GraphIndex &graphIndex = sd.graphIndex;

  for (;;) {
    CreateIndexJob job;
    try {
      poolThread.getInputJobQueue().popFront(job);
    } catch (NGT::ThreadTerminationException &err) {
      break;
    } catch (NGT::Exception &err) {
      cerr << "CreateIndex::search:Error! popFront " << err.what() << endl;
      break;
    }
    ObjectDistances *rs = new ObjectDistances;
    Object &obj         = *job.object;
    try {
      if (graphIndex.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeKNNG) {
        graphIndex.searchForKNNGInsertion(obj, job.id, *rs); // linear search
      } else {
        graphIndex.searchForNNGInsertion(obj, *rs);
      }
    } catch (NGT::Exception &err) {
      stringstream msg;
      msg << "CreateIndex::search:Fatal error! ID=" << job.id << " " << err.what();
      NGTThrowException(msg);
    }
    job.results = rs;
    poolThread.getOutputJobQueue().pushBack(job);
  }

  return 0;
}

class BuildTimeController {
 public:
  BuildTimeController(GraphIndex &graph, NeighborhoodGraph::Property &prop) : property(prop) {
    noOfInsertedObjects            = graph.objectSpace->getRepository().size() - graph.repository.size();
    interval                       = 10000;
    count                          = interval;
    edgeSizeSave                   = property.edgeSizeForCreation;
    insertionRadiusCoefficientSave = property.insertionRadiusCoefficient;
    buildTimeLimit                 = property.buildTimeLimit;
    time                           = 0.0;
    timer.start();
  }
  ~BuildTimeController() {
    property.edgeSizeForCreation        = edgeSizeSave;
    property.insertionRadiusCoefficient = insertionRadiusCoefficientSave;
  }
  void adjustEdgeSize(size_t c) {
    if (buildTimeLimit > 0.0 && count <= c) {
      timer.stop();
      double estimatedTime = time + timer.time / interval * (noOfInsertedObjects - count);
      estimatedTime /= 60 * 60; // hour
      const size_t edgeInterval  = 5;
      const int minimumEdge      = 5;
      const float radiusInterval = 0.02;
      if (estimatedTime > buildTimeLimit) {
        if (property.insertionRadiusCoefficient - radiusInterval >= 1.0) {
          property.insertionRadiusCoefficient -= radiusInterval;
        } else {
          property.edgeSizeForCreation -= edgeInterval;
          if (property.edgeSizeForCreation < minimumEdge) {
            property.edgeSizeForCreation = minimumEdge;
          }
        }
      }
      time += timer.time;
      count += interval;
      timer.start();
    }
  }

  size_t noOfInsertedObjects;
  size_t interval;
  size_t count;
  size_t edgeSizeSave;
  double insertionRadiusCoefficientSave;
  Timer timer;
  double time;
  double buildTimeLimit;
  NeighborhoodGraph::Property &property;
};

void NGT::GraphIndex::constructObjectSpace(NGT::Property &prop) {
  assert(prop.dimension != 0);
  size_t dimension = prop.dimension;
  if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeSparseJaccard ||
      prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeInnerProduct) {
    dimension++;
  }

  switch (prop.objectType) {
  case NGT::ObjectSpace::ObjectType::ObjectTypeUnset:
  case NGT::ObjectSpace::ObjectType::Float:
    objectSpace = new ObjectSpaceRepository<float, double>(dimension, typeid(float), prop.distanceType);
    break;
  case NGT::ObjectSpace::ObjectType::Uint8:
    objectSpace =
        new ObjectSpaceRepository<unsigned char, int>(dimension, typeid(uint8_t), prop.distanceType);
    break;
#ifdef NGT_HALF_FLOAT
  case NGT::ObjectSpace::ObjectType::Float16:
    objectSpace = new ObjectSpaceRepository<float16, float>(dimension, typeid(float16), prop.distanceType);
    break;
#endif
  case NGT::ObjectSpace::ObjectType::Qsuint8:
    objectSpace = new ObjectSpaceRepository<qsint8, float>(dimension, typeid(qsint8), prop.distanceType);
    break;
#ifdef NGT_PQ4
  case NGT::ObjectSpace::ObjectType::Qint4:
    objectSpace = new ObjectSpaceRepository<qint4, float>(dimension, typeid(qint4), prop.distanceType);
    break;
#endif
  default:
    stringstream msg;
    msg << "Invalid Object Type in the property. " << prop.objectType;
    NGTThrowException(msg);
  }
  objectSpace->setQuantization(prop.quantizationScale, prop.quantizationOffset);
#ifdef NGT_REFINEMENT
  auto dtype = prop.distanceType;
  dtype      = dtype == ObjectSpace::DistanceTypeInnerProduct ? ObjectSpace::DistanceTypeDotProduct
                                                              : prop.distanceType;
  switch (prop.refinementObjectType) {
  case NGT::ObjectSpace::ObjectType::Float:
    refinementObjectSpace = new ObjectSpaceRepository<float, double>(dimension, typeid(float), dtype);
    break;
  case NGT::ObjectSpace::ObjectType::Uint8:
    refinementObjectSpace = new ObjectSpaceRepository<unsigned char, int>(dimension, typeid(uint8_t), dtype);
    break;
#ifdef NGT_HALF_FLOAT
  case NGT::ObjectSpace::ObjectType::Float16:
    refinementObjectSpace = new ObjectSpaceRepository<float16, float>(dimension, typeid(float16), dtype);
    break;
#endif
#ifdef NGT_BFLOAT
  case NGT::ObjectSpace::ObjectType::Bfloat16:
    refinementObjectSpace = new ObjectSpaceRepository<bfloat16, float>(dimension, typeid(bfloat16), dtype);
    break;
#endif
  default:
    stringstream msg;
    msg << "Invalid Refinement Object Type in the property. " << prop.refinementObjectType;
    NGTThrowException(msg);
  }
#endif
}

void NGT::GraphIndex::loadGraph(const string &ifile, NGT::GraphRepository &graph) {
#ifdef _WIN32
  ifstream isg(ifile + "/grp", std::ios::binary);
#else
  ifstream isg(ifile + "/grp");
#endif
  graph.deserialize(isg);
}

void NGT::GraphIndex::loadIndex(const string &ifile, bool readOnly, NGT::Index::OpenType openType) {
  if ((openType & NGT::Index::OpenTypeObjectDisabled) == 0) {
    objectSpace->deserialize(ifile + "/obj");
  }
#ifdef NGT_PQ4
  if (objectSpace != 0) {
    objectSpace->openQuantizer(ifile);
  }
#endif
#ifdef NGT_REFINEMENT
  try {
    refinementObjectSpace->deserialize(ifile + "/robj");
  } catch (Exception &err) {
    std::cerr << "Warning. Cannot open the refinment objects. " << err.what() << std::endl;
  }
#endif
  if ((openType & NGT::Index::OpenTypeGraphDisabled) == 0) {
    try {
#ifdef NGT_GRAPH_READ_ONLY_GRAPH
      if (readOnly) {
        GraphIndex::NeighborhoodGraph::loadSearchGraph(ifile);
      } else {
        loadGraph(ifile, repository);
        checkEdgeLengths(1000);
      }
#else
      loadGraph(ifile, repository);
      checkEdgeLengths(1000);
#endif
    } catch (Exception &err) {
      std::stringstream msg;
      msg << "Fatal error! Cannot load the graph. :" << err.what();
      NGTThrowException(msg);
    }
  }

  if (!readOnly && (openType & NGT::Index::OpenTypeIncomingEdgesEnabled) != 0 &&
      (openType & NGT::Index::OpenTypeGraphDisabled) == 0) {
    buildIncomingEdgesIfNecessary();
  }
}

void NGT::GraphIndex::saveObjectRepository(const std::string &ofile) {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
  try {
    mkdir(ofile);
  } catch (...) {
  }
  bool save = false;
  {
    if (property.objectType == NGT::ObjectSpace::ObjectType::ObjectTypeUnset &&
        objectSpace->getRepository().size() != 0) {
      property.objectType = NGT::ObjectSpace::ObjectType::Float;
      if (repository.size() == 0) {
        NGT::ObjectSpace::ObjectType type = objectSpace->getEstimatedObjectType();
        if (type != NGT::ObjectSpace::ObjectType::Float) {
          NGT::ObjectSpace *convertedObjectSpace = objectSpace->convertObjectSpace(*objectSpace, type);
          convertedObjectSpace->serialize(ofile + "/obj");
          convertedObjectSpace->deleteAll();
          delete convertedObjectSpace;
          save                = true;
          property.objectType = type;
        }
      }
    }
  }
  if (objectSpace != 0 && save == false) {
    objectSpace->serialize(ofile + "/obj");
  } else {
    std::cerr << "saveIndex::Warning! ObjectSpace is null. continue saving..." << std::endl;
  }
#ifdef NGT_REFINEMENT
  if (refinementObjectSpace != 0) {
    refinementObjectSpace->serialize(ofile + "/robj");
  }
#endif
#endif
}

void NGT::GraphIndex::saveProperty(const std::string &file) { NGT::Property::save(*this, file); }

void NGT::GraphIndex::exportProperty(const std::string &file) { NGT::Property::exportProperty(*this, file); }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
NGT::GraphIndex::GraphIndex(const string &allocator, bool rdonly, NGT::Index::OpenType openType)
    : readOnly(rdonly) {
  NGT::Property prop;
  prop.load(allocator);
  if (prop.databaseType != NGT::Index::Property::DatabaseType::MemoryMappedFile) {
    NGTThrowException("GraphIndex: Cannot open. Not memory mapped file type.");
  }
  initialize(allocator, prop);
#ifdef NGT_GRAPH_READ_ONLY_GRAPH
  auto methods                   = NeighborhoodGraph::Search::getMethods(prop.distanceType, prop.objectType,
                                                                         objectSpace->getRepository().size());
  searchUnupdatableGraph         = methods.plain;
  searchUnupdatableGraphFiltered = methods.filtered;
#endif
  if (!readOnly && (openType & NGT::Index::OpenTypeIncomingEdgesEnabled) != 0) {
    buildIncomingEdgesIfNecessary();
  }
}

NGT::GraphAndTreeIndex::GraphAndTreeIndex(const string &allocator, NGT::Property &prop)
    : GraphIndex(allocator, prop), DVPTree(prop) {
  initialize(allocator, prop.treeSharedMemorySize);
}

void GraphAndTreeIndex::createTreeIndex() {
  ObjectRepository &fr = GraphIndex::objectSpace->getRepository();
  for (size_t id = 0; id < fr.size(); id++) {
    if (id % 100000 == 0) {
      cerr << " Processed id=" << id << endl;
    }
    if (fr.isEmpty(id)) {
      continue;
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    Object *f = GraphIndex::objectSpace->allocateObject(*fr[id]);
    DVPTree::InsertContainer tiobj(*f, id);
#else
    DVPTree::InsertContainer tiobj(*fr[id], id);
#endif
    try {
      DVPTree::insert(tiobj);
    } catch (Exception &err) {
      cerr << "GraphAndTreeIndex::createTreeIndex: Warning. ID=" << id << ":";
      cerr << err.what() << " continue.." << endl;
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    GraphIndex::objectSpace->deleteObject(f);
#endif
  }
}

void NGT::GraphIndex::initialize(const string &allocator, NGT::Property &prop) {
  constructObjectSpace(prop);
  repository.open(allocator + "/grp", prop.graphSharedMemorySize);
  objectSpace->open(allocator + "/obj", prop.objectSharedMemorySize);
#ifdef NGT_REFINEMENT
  refinementObjectSpace->open(allocator + "/robj", prop.objectSharedMemorySize);
#endif
  setProperty(prop);
}
#else // NGT_SHARED_MEMORY_ALLOCATOR
NGT::GraphIndex::GraphIndex(const string &database, bool rdOnly, NGT::Index::OpenType openType)
    : readOnly(rdOnly) {
  NGT::Property prop;
  prop.load(database);
  if (prop.databaseType != NGT::Index::Property::DatabaseType::Memory) {
    NGTThrowException("GraphIndex: Cannot open. Not memory type.");
  }
  assert(prop.dimension != 0);
  initialize(prop);
  loadIndex(database, readOnly, openType);
#ifdef NGT_GRAPH_READ_ONLY_GRAPH
  if (prop.searchType == "Large") {
    auto methods = NeighborhoodGraph::Search::getMethods(prop.distanceType, prop.objectType, 10000000);
    searchUnupdatableGraph         = methods.plain;
    searchUnupdatableGraphFiltered = methods.filtered;
  } else if (prop.searchType == "Small") {
    auto methods           = NeighborhoodGraph::Search::getMethods(prop.distanceType, prop.objectType, 0);
    searchUnupdatableGraph = methods.plain;
    searchUnupdatableGraphFiltered = methods.filtered;
  } else {
    auto methods           = NeighborhoodGraph::Search::getMethods(prop.distanceType, prop.objectType,
                                                                   objectSpace->getRepository().size());
    searchUnupdatableGraph = methods.plain;
    searchUnupdatableGraphFiltered = methods.filtered;
  }
#endif
}
#endif

void GraphIndex::extractSparseness(InsertionOrder &insertionOrder) {
  if (getNumberOfIndexedObjects() == 0) {
    NGTThrowException("extractInsertionOrder: No indexed objects.");
  }
  auto nOfThreads =
      insertionOrder.nOfThreads == 0 ? std::thread::hardware_concurrency() : insertionOrder.nOfThreads;
#ifndef _OPENMP
  nOfThreads = 1;
#endif
  NGT::Timer timer;
  timer.start();
  std::cerr << "extractInsertionOrder" << std::endl;
  std::cerr << "VM size=" << NGT::Common::getProcessVmSizeStr() << std::endl;
  std::cerr << "Peak VM size=" << NGT::Common::getProcessVmPeakStr() << std::endl;
  std::cerr << "searching..." << std::endl;

  if (getObjectRepositorySize() != getGraphRepositorySize()) {
    std::stringstream msg;
    msg << "extractInsertionOrder: # of objects and # of indexed objects are not consistent. "
        << getObjectRepositorySize() << ":" << getGraphRepositorySize();
    NGTThrowException(msg);
  }

#ifdef _OPENMP
  omp_set_num_threads(nOfThreads);
#endif

  std::cerr << "search size=" << insertionOrder.nOfNeighboringNodes << std::endl;
  std::vector<uint32_t> counter(nOfThreads);
  std::vector<uint32_t> indegrees[nOfThreads];
  for (size_t tidx = 0; tidx < nOfThreads; tidx++) {
    indegrees[tidx].resize(getGraphRepositorySize());
  }
  std::vector<std::pair<float, uint32_t>> length;
  length.resize(getObjectRepositorySize());
#pragma omp parallel for
  for (NGT::ObjectID query = 1; query < getObjectRepositorySize(); query++) {
#ifndef _OPENMP
    auto thdID = 0;
#else
    auto thdID = omp_get_thread_num();
#endif
    counter[thdID]++;
    if (query % 100000 == 0) {
      size_t n = 0;
      for (auto &c : counter)
        n += c;
      timer.stop();
      std::cerr << "# of the processed objects=" << n << " VM size=" << NGT::Common::getProcessVmSizeStr()
                << " Peak VM size=" << NGT::Common::getProcessVmPeakStr() << " Time=" << timer << std::endl;
      timer.restart();
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    NGT::Object *object = getObjectSpace().allocateObject(*getObjectSpace().getRepository().get(query));
#else
    NGT::Object *object = getObjectSpace().getRepository().get(query);
#endif
    {
      NGT::SearchContainer sc(*object);
      NGT::ObjectDistances objects;
      sc.setResults(&objects);
      sc.setSize(insertionOrder.nOfNeighboringNodes);
      sc.setEpsilon(insertionOrder.epsilon);
      sc.setEdgeSize(-2);
      NGT::Timer timer;
      try {
        timer.start();
        search(sc);
        timer.stop();
      } catch (NGT::Exception &err) {
        std::cerr << "extractSparseness: Warning! " << err.what() << ":" << query << std::endl;
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      getObjectSpace().deleteObject(object);
#endif
      float len    = 0.0;
      size_t count = 0;
      if (objects.size() == 0) {
        std::stringstream msg;
        msg << "extractInsertionOrder: Error! # of the searched objects is zero. " << query << ":"
            << getPath() << std::endl;
        NGTThrowException(msg);
      }
      for (size_t i = 0; i < objects.size(); i++) {
        if (query == objects[i].id) continue;
        len += objects[i].distance;
        count++;
        if (objects[i].id >= indegrees[thdID].size()) {
          std::cerr << "too large. " << objects[i].id << ":" << indegrees[thdID].size() << std::endl;
          exit(1);
        }
        indegrees[thdID][objects[i].id]++;
      }
      length[query].first  = len / count;
      length[query].second = query;
    }
  }

  std::sort(length.begin(), length.end());

  size_t max = 0;
  for (NGT::ObjectID id = 1; id < getObjectRepositorySize(); id++) {
    for (size_t tidx = 1; tidx < nOfThreads; tidx++) {
      indegrees[0][id] += indegrees[tidx][id];
    }
    if (indegrees[0][id] > max) max = indegrees[0][id];
  }
  std::cerr << "max=" << max << std::endl;
  if (insertionOrder.indegreeOrder) {
    std::vector<std::vector<uint32_t>> sortedIndegrees;
    sortedIndegrees.resize(max + 1);
    for (uint32_t oid = 1; oid < indegrees[0].size(); oid++) {

      sortedIndegrees[indegrees[0][oid]].push_back(oid);
    }
    {
      size_t c = 0;
      insertionOrder.reserve(getObjectRepositorySize());
      for (uint32_t ind = 0; ind < sortedIndegrees.size(); ind++) {
        c += ind * sortedIndegrees[ind].size();
        if (sortedIndegrees[ind].size() != 0) {
          for (auto &id : sortedIndegrees[ind]) {
            insertionOrder.push_back(id);
          }
        }
      }
      std::cerr << "total number of the incoming edges=" << c << ":"
                << (insertionOrder.nOfThreads - 1) * (getObjectRepositorySize() - 1) << std::endl;
    }
  } else {
    insertionOrder.reserve(getObjectRepositorySize());
    for (NGT::ObjectID id = getObjectRepositorySize() - 1; id != 0; id--) {
      insertionOrder.push_back(length[id].second);
    }
  }
}

void GraphIndex::extractInsertionOrder(InsertionOrder &insertionOrder) {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
  if (getNumberOfObjects() == 0) {
    NGTThrowException("extractInsertionOrder: No objects.");
  }
  auto edgeSizeBackup                             = NeighborhoodGraph::property.edgeSizeForCreation;
  NeighborhoodGraph::property.edgeSizeForCreation = 10;
  auto nOfThreads =
      insertionOrder.nOfThreads == 0 ? std::thread::hardware_concurrency() : insertionOrder.nOfThreads;

  try {
    InsertionOrder io;
    GraphIndex::createIndexWithInsertionOrder(io, nOfThreads);
  } catch (Exception &err) {
    NeighborhoodGraph::property.edgeSizeForCreation = edgeSizeBackup;
    throw err;
  }
  NeighborhoodGraph::property.edgeSizeForCreation = edgeSizeBackup;

  extractSparseness(insertionOrder);

  NeighborhoodGraph::repository.initialize();
#endif
}

void GraphIndex::createIndexWithSingleThread() {
  GraphRepository &anngRepo = repository;
  ObjectRepository &fr      = objectSpace->getRepository();
  size_t pathAdjustCount    = property.pathAdjustmentInterval;
  NGT::ObjectID id          = 1;
  size_t count              = 0;
  BuildTimeController buildTimeController(*this, NeighborhoodGraph::property);
  for (; id < fr.size(); id++) {
    if (id < anngRepo.size() && anngRepo[id] != 0) {
      continue;
    }
    insert(id);
    buildTimeController.adjustEdgeSize(++count);
    if (pathAdjustCount > 0 && pathAdjustCount <= id) {
      GraphReconstructor::adjustPathsEffectively(static_cast<GraphIndex &>(*this));
      pathAdjustCount += property.pathAdjustmentInterval;
    }
  }
}

static size_t searchMultipleQueryForCreation(GraphIndex &neighborhoodGraph, NGT::ObjectID &id,
                                             CreateIndexJob &job, CreateIndexThreadPool &threads,
                                             size_t sizeOfRepository, Index::InsertionOrder &insertionOrder) {
  ObjectRepository &repo    = neighborhoodGraph.objectSpace->getRepository();
  GraphRepository &anngRepo = neighborhoodGraph.repository;
  size_t cnt                = 0;
  for (; id < repo.size(); id++) {
    if (sizeOfRepository > 0 && id >= sizeOfRepository) {
      break;
    }
    auto oid = insertionOrder.empty() ? id : insertionOrder.getID(id);
    if (repo[oid] == 0) {
      continue;
    }
    if (neighborhoodGraph.NeighborhoodGraph::property.graphType != NeighborhoodGraph::GraphTypeBKNNG) {
      if (oid < anngRepo.size() && anngRepo[oid] != 0) {
        continue;
      }
    }
    job.id = oid;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    job.object = neighborhoodGraph.objectSpace->allocateObject(*repo[oid]);
#else
    job.object = repo[oid];
#endif
    job.batchIdx = cnt;
    threads.pushInputQueue(job);
    cnt++;
    if (cnt >= (size_t)neighborhoodGraph.NeighborhoodGraph::property.batchSizeForCreation) {
      id++;
      break;
    }
  } // for
  return cnt;
}

static void insertMultipleSearchResults(GraphIndex &neighborhoodGraph,
                                        CreateIndexThreadPool::OutputJobQueue &output, ObjectID id,
                                        size_t dataSize) {
  if (neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeRANNG ||
      neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeRIANNG) {
#pragma omp parallel for
    for (size_t i = 0; i < dataSize; i++) {
      neighborhoodGraph.deleteShortcutEdges(*output[i].results);
    }
  }
  // compute distances among all of the resultant objects
  if (neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeANNG ||
      neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeIANNG ||
      neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeONNG ||
      neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeDNNG ||
      neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeSNNG ||
      neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeRANNG ||
      neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeRIANNG) {
    // This processing occupies about 30% of total indexing time when batch size is 200.
    // Only initial batch objects should be connected for each other.n
    // The number of nodes in the graph is checked to know whether the batch is initial.
    size_t size = neighborhoodGraph.NeighborhoodGraph::property.searchMultiplier <= 1.0
                      ? neighborhoodGraph.NeighborhoodGraph::property.edgeSizeForCreation
                      : neighborhoodGraph.NeighborhoodGraph::property.edgeSizeForCreation *
                            neighborhoodGraph.NeighborhoodGraph::property.searchMultiplier;
    // add distances from a current object to subsequence objects to imitate of sequential insertion.

    sort(output.begin(), output.end()); // sort by batchIdx

    for (size_t idxi = 0; idxi < dataSize; idxi++) {
      // add distances
      ObjectDistances &objs = *output[idxi].results;
      for (size_t idxj = 0; idxj < idxi; idxj++) {
        ObjectDistance r;
        r.distance =
            neighborhoodGraph.objectSpace->getComparator()(*output[idxi].object, *output[idxj].object);
        r.id = output[idxj].id;
        objs.push_back(r);
      }
      // sort and cut excess edges
      std::sort(objs.begin(), objs.end());
      if (objs.size() > size) {
        objs.resize(size);
      }
    } // for (size_t idxi ....
  } // if (neighborhoodGraph.graphType == NeighborhoodGraph::GraphTypeUDNNG)
  // insert resultant objects into the graph as edges
  for (size_t i = 0; i < dataSize; i++) {
    CreateIndexJob &gr = output[i];
    if ((*gr.results).size() == 0) {
    }
    auto targetID = id == 0 ? gr.id : (id + i);
    if (static_cast<int>(targetID) > neighborhoodGraph.NeighborhoodGraph::property.edgeSizeForCreation &&
        static_cast<int>(gr.results->size()) <
            neighborhoodGraph.NeighborhoodGraph::property.edgeSizeForCreation) {
      if (neighborhoodGraph.NeighborhoodGraph::property.graphType != NeighborhoodGraph::GraphTypeRANNG &&
          neighborhoodGraph.NeighborhoodGraph::property.graphType != NeighborhoodGraph::GraphTypeRIANNG) {
        cerr << "createIndex: Warning. The specified number of edges could not be acquired, because the "
                "pruned parameter [-S] might be set."
             << endl;
        cerr << "  The node id=" << gr.id << ":" << id + i << ":" << targetID << endl;
        cerr << "  The number of edges for creation="
             << neighborhoodGraph.NeighborhoodGraph::property.edgeSizeForCreation << endl;
        cerr << "  The number of edges for the node=" << gr.results->size() << endl;
        cerr << "  The pruned parameter (edgeSizeForSearch [-S])="
             << neighborhoodGraph.NeighborhoodGraph::property.edgeSizeForSearch << endl;
      }
    }
    try {
      neighborhoodGraph.insertNode(gr.id, *gr.results);
    } catch (NGT::Exception &err) {
      std::stringstream msg;
      msg << " Cannot insert the node. " << gr.id << ". " << err.what();
      NGTThrowException(msg);
    }
  }
}

void GraphIndex::createIndexWithInsertionOrder(InsertionOrder &insertionOrder, size_t threadPoolSize,
                                               size_t sizeOfRepository) {
  if (NeighborhoodGraph::property.edgeSizeForCreation == 0) {
    return;
  }
  if (!insertionOrder.empty()) {
    if (objectSpace->getRepository().size() - 1 != insertionOrder.size()) {
      stringstream msg;
      msg << "Index::createIndex: The insertion order size is invalid. "
          << (objectSpace->getRepository().size() - 1) << ":" << insertionOrder.size();
      NGTThrowException(msg);
    }
  }
  threadPoolSize = threadPoolSize == 0 ? std::thread::hardware_concurrency() : threadPoolSize;
  threadPoolSize = threadPoolSize == 0 ? 8 : threadPoolSize;
  if (threadPoolSize <= 1) {
    createIndexWithSingleThread();
  } else {
    Timer timer;
    size_t timerInterval = 100000;
    timerInterval        = 10000;
    size_t timerCount    = timerInterval;
    size_t count         = 0;
    timer.start();

    size_t pathAdjustCount = property.pathAdjustmentInterval;
    CreateIndexThreadPool threads(threadPoolSize);
    CreateIndexSharedData sd(*this);

    threads.setSharedData(&sd);
    threads.create();
    CreateIndexThreadPool::OutputJobQueue &output = threads.getOutputJobQueue();

    BuildTimeController buildTimeController(*this, NeighborhoodGraph::property);

    try {
      CreateIndexJob job;
      NGT::ObjectID id = 1;
      for (;;) {
        // search for the nearest neighbors
        size_t cnt =
            searchMultipleQueryForCreation(*this, id, job, threads, sizeOfRepository, insertionOrder);
        if (cnt == 0) {
          break;
        }
        // wait for the completion of the search
        threads.waitForFinish();
        if (output.size() != cnt) {
          cerr << "NNTGIndex::insertGraphIndexByThread: Warning!! Thread response size is wrong." << endl;
          cnt = output.size();
        }
        // insertion
        insertMultipleSearchResults(*this, output, insertionOrder.empty() ? 0 : (id - cnt), cnt);

        while (!output.empty()) {
          delete output.front().results;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          GraphIndex::objectSpace->deleteObject(output.front().object);
#endif
          output.pop_front();
        }

        count += cnt;
        if (timerCount <= count) {
          timer.stop();
          cerr << "GraphIndex::createIndexWithInsertionOrder: ";
          cerr << "Processed " << timerCount << " objects. time= " << timer
               << " vm size=" << NGT::Common::getProcessVmSizeStr() << ":"
               << NGT::Common::getProcessVmPeakStr() << endl;
          timerCount += timerInterval;
          timer.restart();
        }
        buildTimeController.adjustEdgeSize(count);
        if (pathAdjustCount > 0 && pathAdjustCount <= count) {
          GraphReconstructor::adjustPathsEffectively(static_cast<GraphIndex &>(*this));
          pathAdjustCount += property.pathAdjustmentInterval;
        }
      }
    } catch (Exception &err) {
      threads.terminate();
      throw err;
    }
    threads.terminate();
  }
}

void GraphIndex::setupPrefetch(NGT::Property &prop) {
  assert(GraphIndex::objectSpace != 0);
  prop.prefetchOffset = GraphIndex::objectSpace->setPrefetchOffset(prop.prefetchOffset);
  prop.prefetchSize   = GraphIndex::objectSpace->setPrefetchSize(prop.prefetchSize);
}

bool NGT::GraphIndex::showStatisticsOfGraph(NGT::GraphIndex &outGraph, char mode, size_t edgeSize) {
  long double distance             = 0.0;
  size_t numberOfNodes             = 0;
  size_t numberOfOutdegree         = 0;
  size_t numberOfNodesWithoutEdges = 0;
  size_t maxNumberOfOutdegree      = 0;
  size_t minNumberOfOutdegree      = SIZE_MAX;
  std::vector<int64_t> indegreeCount;
  std::vector<size_t> outdegreeHistogram;
  std::vector<size_t> indegreeHistogram;
  std::vector<std::vector<float>> indegree;
  NGT::GraphRepository &graph = outGraph.repository;
  NGT::ObjectRepository &repo = outGraph.objectSpace->getRepository();
#ifdef NGT_REFINEMENT
  auto &rrepo = outGraph.refinementObjectSpace->getRepository();
#endif
  indegreeCount.resize(graph.size(), 0);
  indegree.resize(graph.size());
  size_t removedObjectCount = 0;
  bool valid                = true;
  auto &comparator          = outGraph.objectSpace->getComparator();
  size_t nOfEdges           = 0;
  size_t nOfDifferentEdges  = 0;
  for (size_t id = 1; id < graph.size(); id++) {
    if (repo[id] == 0) {
      removedObjectCount++;
      try {
        outGraph.getNode(id);
        std::cerr << "Warning! The removed node exists in the graph. ID=" << id << std::endl;
      } catch (...) {
      }
      continue;
    }
    NGT::GraphNode *node = 0;
    try {
      node = outGraph.getNode(id);
    } catch (NGT::Exception &err) {
      std::cerr << "ngt info: Error. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
      valid = false;
      continue;
    }
    numberOfNodes++;
    if (numberOfNodes % 1000000 == 0) {
      std::cerr << "Processed " << numberOfNodes << std::endl;
    }
    size_t esize = node->size() > edgeSize ? edgeSize : node->size();
    if (esize == 0) {
      numberOfNodesWithoutEdges++;
    }
    if (esize > maxNumberOfOutdegree) {
      maxNumberOfOutdegree = esize;
    }
    if (esize < minNumberOfOutdegree) {
      minNumberOfOutdegree = esize;
    }
    if (outdegreeHistogram.size() <= esize) {
      outdegreeHistogram.resize(esize + 1);
    }
    outdegreeHistogram[esize]++;
    if (mode == 'e') {
      std::cout << id << "," << esize << ": ";
    }
    NGT::PersistentObject *obj = 0;
    if (mode == 'd' || mode == 'D') {
      if (!repo.isEmpty(id)) {
        obj = repo.get(id);
      } else {
        std::cerr << "This graph node dose not exist in the object repository! " << id << std::endl;
      }
    }
    for (size_t i = 0; i < esize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      NGT::ObjectDistance &n = (*node).at(i, graph.allocator);
#else
      NGT::ObjectDistance &n = (*node)[i];
#endif
      NGT::ObjectDistance *prevn = nullptr;
      if (i > 0) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        prevn = &(*node).at(i - 1, graph.allocator);
#else
        prevn = &(*node)[i - 1];
#endif
      }
      if (std::isnan(n.distance)) {
        stringstream msg;
        msg << "Index::showStatisticsOfGraph: Fatal inner error! The graph has a node with nan distance. "
            << id << ":" << n.id << ":" << n.distance;
        NGTThrowException(msg);
      }
      if (n.id == 0) {
        std::cerr << "ngt info: Warning. id is zero." << std::endl;
        valid = false;
        continue;
      }
      if (id == n.id) {
        std::cerr << "Warning: Found an edge to itself! " << id << std::endl;
        valid = false;
        continue;
      }
      if (i > 0 && n.id == prevn->id) {
        std::cerr << "Warning: Found identical IDs " << id << ":" << i << ":" << n.id << std::endl;
      }
      if (mode == 'd' || mode == 'D') {
        try {
          outGraph.getNode(n.id);
        } catch (NGT::Exception &err) {
          std::cerr << "Warning: The graph node of the edge destination does not exist! " << n.id
                    << std::endl;
        }
        if (repo.isEmpty(n.id)) {
          std::cerr << "Warning: The object of the edge destination does not exist! " << n.id << std::endl;
        }
        if (!repo.isEmpty(id) && !repo.isEmpty(n.id)) {
          nOfEdges++;
          float d = comparator(*obj, *repo.get(n.id));
          if (d != n.distance) {
            nOfDifferentEdges++;
            if (mode == 'D') {
              std::cerr << "The current edge length is different from the indexed length. "
                        << std::setprecision(15) << d << ":" << n.distance << std::setprecision(6) << " "
                        << nOfDifferentEdges << "/" << nOfEdges << std::endl;
            }
          }
        }
      }
      indegreeCount[n.id]++;
      indegree[n.id].push_back(n.distance);
      numberOfOutdegree++;
      double d = n.distance;
      if (mode == 'e') {
        std::cout << n.id << ":" << d << " ";
      }
      distance += d;
    }
    if (mode == 'e') {
      std::cout << std::endl;
    }
  }

  if (mode == 'a') {
    size_t count = 0;
    for (size_t id = 1; id < graph.size(); id++) {
      if (repo[id] == 0) {
        continue;
      }
      NGT::GraphNode *n = 0;
      try {
        n = outGraph.getNode(id);
      } catch (NGT::Exception &err) {
        continue;
      }
      NGT::GraphNode &node = *n;
      for (size_t i = 0; i < node.size(); i++) {
        NGT::GraphNode *nn = 0;
        try {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          nn = outGraph.getNode(node.at(i, graph.allocator).id);
#else
          nn = outGraph.getNode(node[i].id);
#endif
        } catch (NGT::Exception &err) {
          count++;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          std::cerr << "Directed edge! " << id << "->" << node.at(i, graph.allocator).id << " no object. "
                    << node.at(i, graph.allocator).id << std::endl;
#else
          std::cerr << "Directed edge! " << id << "->" << node[i].id << " no object. " << node[i].id
                    << std::endl;
#endif
          continue;
        }
        NGT::GraphNode &nnode = *nn;
        bool found            = false;
        for (size_t i = 0; i < nnode.size(); i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          if (nnode.at(i, graph.allocator).id == id) {
#else
          if (nnode[i].id == id) {
#endif
            found = true;
            break;
          }
        }
        if (!found) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          std::cerr << "Directed edge! " << id << "->" << node.at(i, graph.allocator).id << " no edge. "
                    << node.at(i, graph.allocator).id << "->" << id << std::endl;
#else
          std::cerr << "Directed edge! " << id << "->" << node[i].id << " no edge. " << node[i].id << "->"
                    << id << std::endl;
#endif
          count++;
        }
      }
    }
    std::cerr << "The number of directed edges=" << count << std::endl;
  }

  // calculate outdegree distance 10
  size_t d10count        = 0;
  long double distance10 = 0.0;
  size_t d10SkipCount    = 0;
  const size_t dcsize    = 10;
  for (size_t id = 1; id < graph.size(); id++) {
    if (repo[id] == 0) {
      continue;
    }
    if (graph.isEmpty(id)) {
      continue;
    }
    NGT::GraphNode *n = 0;
    try {
      n = outGraph.getNode(id);
    } catch (NGT::Exception &err) {
      std::cerr << "ngt info: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
      continue;
    }
    NGT::GraphNode &node = *n;
    if (node.size() < dcsize) {
      d10SkipCount++;
      continue;
    }
    for (size_t i = 0; i < dcsize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      distance10 += node.at(i, graph.allocator).distance;
#else
      distance10 += node[i].distance;
#endif
      d10count++;
    }
  }
  if (d10count != 0) {
    distance10 /= (long double)d10count;
  }

  // calculate indegree distance 10
  size_t ind10count              = 0;
  long double indegreeDistance10 = 0.0;
  size_t ind10SkipCount          = 0;
  for (size_t id = 1; id < indegree.size(); id++) {
    std::vector<float> &node = indegree[id];
    if (node.size() < dcsize) {
      ind10SkipCount++;
      continue;
    }
    std::sort(node.begin(), node.end());
    for (size_t i = 0; i < dcsize; i++) {
      if (i > 0 && node[i - 1] > node[i]) {
        stringstream msg;
        msg << "Index::showStatisticsOfGraph: Fatal inner error! Wrong distance order " << node[i - 1] << ":"
            << node[i];
        NGTThrowException(msg);
      }
      indegreeDistance10 += node[i];
      ind10count++;
    }
  }
  if (ind10count != 0) {
    indegreeDistance10 /= (long double)ind10count;
  }

  // calculate variance
  double averageNumberOfOutdegree = (double)numberOfOutdegree / (double)numberOfNodes;
  double sumOfSquareOfOutdegree   = 0;
  double sumOfSquareOfIndegree    = 0;
  for (size_t id = 1; id < graph.size(); id++) {
    if (repo[id] == 0) {
      continue;
    }
    NGT::GraphNode *node = 0;
    try {
      node = outGraph.getNode(id);
    } catch (NGT::Exception &err) {
      std::cerr << "ngt info: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
      continue;
    }
    size_t esize = node->size();
    sumOfSquareOfOutdegree +=
        ((double)esize - averageNumberOfOutdegree) * ((double)esize - averageNumberOfOutdegree);
    sumOfSquareOfIndegree += ((double)indegreeCount[id] - averageNumberOfOutdegree) *
                             ((double)indegreeCount[id] - averageNumberOfOutdegree);
  }

  size_t numberOfNodesWithoutIndegree = 0;
  size_t maxNumberOfIndegree          = 0;
  size_t minNumberOfIndegree          = INT64_MAX;
  for (size_t id = 1; id < graph.size(); id++) {
    if (graph[id] == 0) {
      continue;
    }
    if (indegreeCount[id] == 0) {
      numberOfNodesWithoutIndegree++;
      std::cerr << "Warning! The node without incoming edges. " << id << std::endl;
      valid = false;
    }
    if (indegreeCount[id] > static_cast<int>(maxNumberOfIndegree)) {
      maxNumberOfIndegree = indegreeCount[id];
    }
    if (indegreeCount[id] < static_cast<int64_t>(minNumberOfIndegree)) {
      minNumberOfIndegree = indegreeCount[id];
    }
    if (static_cast<int>(indegreeHistogram.size()) <= indegreeCount[id]) {
      indegreeHistogram.resize(indegreeCount[id] + 1);
    }
    indegreeHistogram[indegreeCount[id]]++;
  }

  size_t count         = 0;
  int medianOutdegree  = -1;
  size_t modeOutdegree = 0;
  size_t max           = 0;
  double c95           = 0.0;
  double c99           = 0.0;
  for (size_t i = 0; i < outdegreeHistogram.size(); i++) {
    count += outdegreeHistogram[i];
    if (medianOutdegree == -1 && count >= numberOfNodes / 2) {
      medianOutdegree = i;
    }
    if (max < outdegreeHistogram[i]) {
      max           = outdegreeHistogram[i];
      modeOutdegree = i;
    }
    if (count > numberOfNodes * 0.95) {
      if (c95 == 0.0) {
        c95 += i * (count - numberOfNodes * 0.95);
      } else {
        c95 += i * outdegreeHistogram[i];
      }
    }
    if (count > numberOfNodes * 0.99) {
      if (c99 == 0.0) {
        c99 += i * (count - numberOfNodes * 0.99);
      } else {
        c99 += i * outdegreeHistogram[i];
      }
    }
  }
  c95 /= (double)numberOfNodes * 0.05;
  c99 /= (double)numberOfNodes * 0.01;

  count               = 0;
  int medianIndegree  = -1;
  size_t modeIndegree = 0;
  max                 = 0;
  double c5           = 0.0;
  double c1           = 0.0;
  for (size_t i = 0; i < indegreeHistogram.size(); i++) {
    if (count < numberOfNodes * 0.05) {
      if (count + indegreeHistogram[i] >= numberOfNodes * 0.05) {
        c5 += i * (numberOfNodes * 0.05 - count);
      } else {
        c5 += i * indegreeHistogram[i];
      }
    }
    if (count < numberOfNodes * 0.01) {
      if (count + indegreeHistogram[i] >= numberOfNodes * 0.01) {
        c1 += i * (numberOfNodes * 0.01 - count);
      } else {
        c1 += i * indegreeHistogram[i];
      }
    }
    count += indegreeHistogram[i];
    if (medianIndegree == -1 && count >= numberOfNodes / 2) {
      medianIndegree = i;
    }
    if (max < indegreeHistogram[i]) {
      max          = indegreeHistogram[i];
      modeIndegree = i;
    }
  }
  c5 /= (double)numberOfNodes * 0.05;
  c1 /= (double)numberOfNodes * 0.01;

  std::cerr << "The number of the objects:\t" << outGraph.getNumberOfObjects() << std::endl;
  std::cerr << "The number of the indexed objects:\t" << outGraph.getNumberOfIndexedObjects() << std::endl;
  std::cerr << "The size of the object repository (not the number of the objects):\t"
            << (repo.size() == 0 ? 0 : repo.size() - 1) << std::endl;
#ifdef NGT_REFINEMENT
  std::cerr << "The size of the refinement object repository (not the number of the objects):\t"
            << (rrepo.size() == 0 ? 0 : rrepo.size() - 1) << std::endl;
#endif
  std::cerr << "The number of the removed objects:\t" << removedObjectCount << "/"
            << (repo.size() == 0 ? 0 : repo.size() - 1) << std::endl;
  std::cerr << "The number of the nodes:\t" << numberOfNodes << std::endl;
  std::cerr << "The number of the edges:\t" << numberOfOutdegree << std::endl;
  std::cerr << "The mean of the edge lengths:\t" << std::setprecision(10)
            << (numberOfOutdegree != 0.0 ? distance / (double)numberOfOutdegree : 0) << std::endl;
  std::cerr << "The mean of the number of the edges per node:\t"
            << (numberOfNodes != 0.0 ? (double)numberOfOutdegree / (double)numberOfNodes : 0) << std::endl;
  std::cerr << "The number of the nodes without edges:\t" << numberOfNodesWithoutEdges << std::endl;
  std::cerr << "The maximum of the outdegrees:\t" << maxNumberOfOutdegree << std::endl;
  if (minNumberOfOutdegree == SIZE_MAX) {
    std::cerr << "The minimum of the outdegrees:\t-NA-" << std::endl;
  } else {
    std::cerr << "The minimum of the outdegrees:\t" << minNumberOfOutdegree << std::endl;
  }
  std::cerr << "The number of the nodes where indegree is 0:\t" << numberOfNodesWithoutIndegree << std::endl;
  std::cerr << "The maximum of the indegrees:\t" << maxNumberOfIndegree << std::endl;
  if (minNumberOfIndegree == INT64_MAX) {
    std::cerr << "The minimum of the indegrees:\t-NA-" << std::endl;
  } else {
    std::cerr << "The minimum of the indegrees:\t" << minNumberOfIndegree << std::endl;
  }
  std::cerr << "The mean of the edge lengths for 10 edges:\t" << std::setprecision(10) << distance10 << "/"
            << d10count << std::endl;
  if (mode == 'd' || mode == 'D') {
    std::cerr << "The number of the different length edges:\t" << nOfDifferentEdges << "/" << nOfEdges
              << std::endl;
  }
  std::cerr
      << "#-nodes,#-edges,#-no-indegree,avg-edges,avg-dist,max-out,min-out,v-out,max-in,min-in,v-in,med-out,"
         "med-in,mode-out,mode-in,c95,c5,o-distance(10),o-skip,i-distance(10),i-skip:"
      << numberOfNodes << ":" << numberOfOutdegree << ":" << numberOfNodesWithoutIndegree << ":"
      << std::setprecision(10) << (double)numberOfOutdegree / (double)numberOfNodes << ":"
      << distance / (double)numberOfOutdegree << ":" << maxNumberOfOutdegree << ":" << minNumberOfOutdegree
      << ":" << sumOfSquareOfOutdegree / (double)numberOfOutdegree << ":" << maxNumberOfIndegree << ":"
      << minNumberOfIndegree << ":" << sumOfSquareOfIndegree / (double)numberOfOutdegree << ":"
      << medianOutdegree << ":" << medianIndegree << ":" << modeOutdegree << ":" << modeIndegree << ":" << c95
      << ":" << c5 << ":" << c99 << ":" << c1 << ":" << distance10 << ":" << d10SkipCount << ":"
      << indegreeDistance10 << ":" << ind10SkipCount << std::endl;
  if (mode == 'h') {
    std::cerr << "#\tout\tin" << std::endl;
    for (size_t i = 0; i < outdegreeHistogram.size() || i < indegreeHistogram.size(); i++) {
      size_t out = outdegreeHistogram.size() <= i ? 0 : outdegreeHistogram[i];
      size_t in  = indegreeHistogram.size() <= i ? 0 : indegreeHistogram[i];
      std::cerr << i << "\t" << out << "\t" << in << std::endl;
    }
  } else if (mode == 'p') {
    std::cerr << "ID\toutdegree\tindegree" << std::endl;
    for (size_t id = 1; id < graph.size(); id++) {
      std::cerr << id << "\t" << outGraph.getNode(id)->size() << "\t" << indegreeCount[id] << std::endl;
    }
  }
  return valid;
}

NGT::GraphIndex::GraphStatistics NGT::GraphIndex::getGraphStatistics(NGT::GraphIndex &outGraph, char mode,
                                                                     size_t edgeSize) {
  NGT::GraphIndex::GraphStatistics stats = {};
  long double distance                   = 0.0;
  size_t numberOfNodes                   = 0;
  size_t numberOfOutdegree               = 0;
  size_t numberOfNodesWithoutEdges       = 0;
  size_t maxNumberOfOutdegree            = 0;
  size_t minNumberOfOutdegree            = SIZE_MAX;
  std::vector<int64_t> indegreeCount;
  std::vector<size_t> outdegreeHistogram;
  std::vector<size_t> indegreeHistogram;
  std::vector<std::vector<float>> indegree;
  NGT::GraphRepository &graph = outGraph.repository;
  NGT::ObjectRepository &repo = outGraph.objectSpace->getRepository();

#ifdef NGT_REFINEMENT
  auto &rrepo = outGraph.refinementObjectSpace->getRepository();
#endif

  indegreeCount.resize(graph.size(), 0);
  indegree.resize(graph.size());
  size_t removedObjectCount = 0;
  bool valid                = true;

  for (size_t id = 1; id < graph.size(); id++) {
    if (repo[id] == 0) {
      removedObjectCount++;
      continue;
    }
    NGT::GraphNode *node = nullptr;
    try {
      node = outGraph.getNode(id);
    } catch (NGT::Exception &err) {
      std::cerr << "ngt info: Error. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
      valid = false;
      continue;
    }
    if (node == nullptr) continue;
    numberOfNodes++;
    size_t esize = std::min(node->size(), edgeSize); // edge size limitation by using edgeSize argument.
    if (esize == 0) {
      numberOfNodesWithoutEdges++;
    }
    maxNumberOfOutdegree = std::max(maxNumberOfOutdegree, esize);
    minNumberOfOutdegree = std::min(minNumberOfOutdegree, esize);
    if (outdegreeHistogram.size() <= esize) {
      outdegreeHistogram.resize(esize + 1);
    }
    outdegreeHistogram[esize]++;
    for (size_t i = 0; i < esize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      NGT::ObjectDistance &n = (*node).at(i, graph.allocator);
#else
      NGT::ObjectDistance &n = (*node)[i];
#endif
      if (std::isnan(n.distance)) {
        std::stringstream msg;
        msg << "NGT::GraphIndex::getGraphStatistics: Fatal inner error! The graph has a node with nan "
               "distance. "
            << id << ":" << n.id << ":" << n.distance;
        NGTThrowException(msg);
      }
      if (n.id == 0) {
        std::cerr << "ngt info: Warning. id is zero." << std::endl;
        valid = false;
      }
      indegreeCount[n.id]++;
      indegree[n.id].push_back(n.distance);
      numberOfOutdegree++;
      distance += n.distance;
    }
  }

  // if mode is 'a' process additional edge checking
  if (mode == 'a') {
    size_t count = 0;
    for (size_t id = 1; id < graph.size(); id++) {
      if (repo[id] == 0) continue;
      NGT::GraphNode *n = nullptr;
      try {
        n = outGraph.getNode(id);
      } catch (NGT::Exception &err) {
        continue;
      }
      if (n == nullptr) continue;
      NGT::GraphNode &node = *n;
      for (size_t i = 0; i < node.size(); i++) {
        NGT::GraphNode *nn = nullptr;
        try {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          nn = outGraph.getNode(node.at(i, graph.allocator).id);
#else
          nn = outGraph.getNode(node[i].id);
#endif
        } catch (NGT::Exception &err) {
          count++;
          continue;
        }
        NGT::GraphNode &nnode = *nn;
        bool found            = false;
        for (size_t i = 0; i < nnode.size(); i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          if (nnode.at(i, graph.allocator).id == id) {
#else
          if (nnode[i].id == id) {
#endif
            found = true;
            break;
          }
        }
        if (!found) count++;
      }
    }
    std::cerr << "The number of directed edges=" << count << std::endl;
  }

  // Calculate outdegree distance for the first 10 edges
  size_t d10count        = 0;
  long double distance10 = 0.0;
  size_t d10SkipCount    = 0;
  const size_t dcsize    = 10;
  for (size_t id = 1; id < graph.size(); id++) {
    if (repo[id] == 0) continue;
    NGT::GraphNode *n = nullptr;
    try {
      n = outGraph.getNode(id);
    } catch (NGT::Exception &err) {
      std::cerr << "ngt info: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
      continue;
    }
    if (n == nullptr) continue;
    NGT::GraphNode &node = *n;
    if (node.size() < dcsize) {
      d10SkipCount++;
      continue;
    }
    for (size_t i = 0; i < dcsize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      distance10 += node.at(i, graph.allocator).distance;
#else
      distance10 += node[i].distance;
#endif
      d10count++;
    }
  }
  if (d10count != 0) {
    distance10 /= static_cast<long double>(d10count);
  }

  // Calculate indegree distance for the first 10 edges
  size_t ind10count              = 0;
  long double indegreeDistance10 = 0.0;
  size_t ind10SkipCount          = 0;
  for (size_t id = 1; id < indegree.size(); id++) {
    std::vector<float> &node = indegree[id];
    if (node.size() < dcsize) {
      ind10SkipCount++;
      continue;
    }
    std::sort(node.begin(), node.end());
    for (size_t i = 0; i < dcsize; i++) {
      if (i > 0 && node[i - 1] > node[i]) {
        stringstream msg;
        msg << "NGT::GraphIndex::getGraphStatistics: Fatal inner error! Wrong distance order " << node[i - 1]
            << ":" << node[i];
        NGTThrowException(msg);
      }
      indegreeDistance10 += node[i];
      ind10count++;
    }
  }
  if (ind10count != 0) {
    indegreeDistance10 /= static_cast<long double>(ind10count);
  }

  // Calculate variance
  double averageNumberOfOutdegree = static_cast<double>(numberOfOutdegree) / numberOfNodes;
  double sumOfSquareOfOutdegree   = 0;
  double sumOfSquareOfIndegree    = 0;
  for (size_t id = 1; id < graph.size(); id++) {
    if (repo[id] == 0) continue;
    NGT::GraphNode *node = nullptr;
    try {
      node = outGraph.getNode(id);
    } catch (NGT::Exception &err) {
      std::cerr << "ngt info: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
      continue;
    }
    size_t esize = node->size();
    sumOfSquareOfOutdegree += (static_cast<double>(esize) - averageNumberOfOutdegree) *
                              (static_cast<double>(esize) - averageNumberOfOutdegree);
    sumOfSquareOfIndegree += (static_cast<double>(indegreeCount[id]) - averageNumberOfOutdegree) *
                             (static_cast<double>(indegreeCount[id]) - averageNumberOfOutdegree);
  }

  size_t numberOfNodesWithoutIndegree = 0;
  size_t maxNumberOfIndegree          = 0;
  size_t minNumberOfIndegree          = INT64_MAX;
  for (size_t id = 1; id < graph.size(); id++) {
    if (graph[id] == 0) continue;
    if (indegreeCount[id] == 0) {
      numberOfNodesWithoutIndegree++;
      std::cerr << "Warning! The node without incoming edges. " << id << std::endl;
      valid = false;
    }
    maxNumberOfIndegree = std::max(maxNumberOfIndegree, static_cast<size_t>(indegreeCount[id]));
    minNumberOfIndegree = std::min(minNumberOfIndegree, static_cast<size_t>(indegreeCount[id]));
    if (indegreeHistogram.size() <= static_cast<size_t>(indegreeCount[id])) {
      indegreeHistogram.resize(indegreeCount[id] + 1);
    }
    indegreeHistogram[indegreeCount[id]]++;
  }

  size_t count         = 0;
  int medianOutdegree  = -1;
  size_t modeOutdegree = 0;
  size_t max           = 0;
  double c95           = 0.0;
  double c99           = 0.0;
  for (size_t i = 0; i < outdegreeHistogram.size(); i++) {
    count += outdegreeHistogram[i];
    if (medianOutdegree == -1 && count >= numberOfNodes / 2) {
      medianOutdegree = i;
    }
    if (max < outdegreeHistogram[i]) {
      max           = outdegreeHistogram[i];
      modeOutdegree = i;
    }
    if (count > numberOfNodes * 0.95) {
      if (c95 == 0.0) {
        c95 += i * (count - numberOfNodes * 0.95);
      } else {
        c95 += i * outdegreeHistogram[i];
      }
    }
    if (count > numberOfNodes * 0.99) {
      if (c99 == 0.0) {
        c99 += i * (count - numberOfNodes * 0.99);
      } else {
        c99 += i * outdegreeHistogram[i];
      }
    }
  }
  c95 /= static_cast<double>(numberOfNodes) * 0.05;
  c99 /= static_cast<double>(numberOfNodes) * 0.01;

  count               = 0;
  int medianIndegree  = -1;
  size_t modeIndegree = 0;
  max                 = 0;
  double c5           = 0.0;
  double c1           = 0.0;
  for (size_t i = 0; i < indegreeHistogram.size(); i++) {
    if (count < numberOfNodes * 0.05) {
      if (count + indegreeHistogram[i] >= numberOfNodes * 0.05) {
        c5 += i * (numberOfNodes * 0.05 - count);
      } else {
        c5 += i * indegreeHistogram[i];
      }
    }
    if (count < numberOfNodes * 0.01) {
      if (count + indegreeHistogram[i] >= numberOfNodes * 0.01) {
        c1 += i * (numberOfNodes * 0.01 - count);
      } else {
        c1 += i * indegreeHistogram[i];
      }
    }
    count += indegreeHistogram[i];
    if (medianIndegree == -1 && count >= numberOfNodes / 2) {
      medianIndegree = i;
    }
    if (max < indegreeHistogram[i]) {
      max          = indegreeHistogram[i];
      modeIndegree = i;
    }
  }
  c5 /= static_cast<double>(numberOfNodes) * 0.05;
  c1 /= static_cast<double>(numberOfNodes) * 0.01;

  stats.setNumberOfObjects(outGraph.getNumberOfObjects());
  stats.setNumberOfIndexedObjects(outGraph.getNumberOfIndexedObjects());
  stats.setSizeOfObjectRepository(repo.size() == 0 ? 0 : repo.size() - 1);
#ifdef NGT_REFINEMENT
  stats.setSizeOfRefinementObjectRepository(rrepo.size() == 0 ? 0 : rrepo.size() - 1);
#endif
  stats.setNumberOfRemovedObjects(removedObjectCount);
  stats.setNumberOfNodes(numberOfNodes);
  stats.setNumberOfEdges(numberOfOutdegree);
  stats.setMeanEdgeLength(numberOfOutdegree != 0.0 ? distance / static_cast<double>(numberOfOutdegree) : 0.0);
  stats.setMeanNumberOfEdgesPerNode(numberOfNodes != 0.0 ? static_cast<double>(numberOfOutdegree) /
                                                               static_cast<double>(numberOfNodes)
                                                         : 0.0);
  stats.setNumberOfNodesWithoutEdges(numberOfNodesWithoutEdges);
  stats.setMaxNumberOfOutdegree(maxNumberOfOutdegree);
  stats.setMinNumberOfOutdegree(minNumberOfOutdegree == SIZE_MAX ? static_cast<size_t>(-1)
                                                                 : minNumberOfOutdegree);
  stats.setNumberOfNodesWithoutIndegree(numberOfNodesWithoutIndegree);
  stats.setMaxNumberOfIndegree(maxNumberOfIndegree);
  stats.setMinNumberOfIndegree(minNumberOfIndegree == INT64_MAX ? static_cast<size_t>(-1)
                                                                : minNumberOfIndegree);
  stats.setMeanEdgeLengthFor10Edges(distance10);
  stats.setNodesSkippedFor10Edges(d10SkipCount);
  stats.setMeanIndegreeDistanceFor10Edges(indegreeDistance10);
  stats.setNodesSkippedForIndegreeDistance(ind10SkipCount);
  stats.setVarianceOfOutdegree(sumOfSquareOfOutdegree / static_cast<double>(numberOfOutdegree));
  stats.setVarianceOfIndegree(sumOfSquareOfIndegree / static_cast<double>(numberOfOutdegree));
  stats.setMedianOutdegree(medianOutdegree);
  stats.setModeOutdegree(modeOutdegree);
  stats.setC95Outdegree(c95);
  stats.setC99Outdegree(c99);
  stats.setMedianIndegree(medianIndegree);
  stats.setModeIndegree(modeIndegree);
  stats.setC5Indegree(c5);
  stats.setC1Indegree(c1);
  stats.setIndegreeCount(std::move(indegreeCount));
  stats.setOutdegreeHistogram(std::move(outdegreeHistogram));
  stats.setIndegreeHistogram(std::move(indegreeHistogram));
  stats.setValid(valid);

  return stats;
}

void GraphAndTreeIndex::createIndexWithInsertionOrder(InsertionOrder &insertionOrder, size_t threadPoolSize,
                                                      size_t sizeOfRepository) {
  if (NeighborhoodGraph::property.edgeSizeForCreation == 0) {
    return;
  }
  if (!insertionOrder.empty()) {
    if (GraphIndex::objectSpace->getRepository().size() - 1 != insertionOrder.size()) {
      stringstream msg;
      msg << "Index::createIndex: The insertion order size is invalid. "
          << (GraphIndex::objectSpace->getRepository().size() - 1) << ":" << insertionOrder.size();
      NGTThrowException(msg);
    }
  }
  threadPoolSize = threadPoolSize == 0 ? std::thread::hardware_concurrency() : threadPoolSize;
  threadPoolSize = threadPoolSize == 0 ? 8 : threadPoolSize;
  Timer timer;
  size_t timerInterval = 100000;
  size_t timerCount    = timerInterval;
  size_t count         = 0;
  timer.start();
  size_t pathAdjustCount = property.pathAdjustmentInterval;
  CreateIndexThreadPool threads(threadPoolSize);

  CreateIndexSharedData sd(*this);

  threads.setSharedData(&sd);
  threads.create();
  CreateIndexThreadPool::OutputJobQueue &output = threads.getOutputJobQueue();

  BuildTimeController buildTimeController(*this, NeighborhoodGraph::property);

  try {
    CreateIndexJob job;
    NGT::ObjectID id = 1;
    for (;;) {
      size_t cnt = searchMultipleQueryForCreation(*this, id, job, threads, sizeOfRepository, insertionOrder);
      if (cnt == 0) {
        break;
      }
      threads.waitForFinish();

      if (output.size() != cnt) {
        cerr << "NNTGIndex::insertGraphIndexByThread: Warning!! Thread response size is wrong." << endl;
        cnt = output.size();
      }

      insertMultipleSearchResults(*this, output, insertionOrder.empty() ? 0 : (id - cnt), cnt);

      for (size_t i = 0; i < cnt; i++) {
        CreateIndexJob &job = output[i];
        if (GraphIndex::objectSpace->isNormalizedDistance()) {
          if (job.results->size() > 0) {
            auto *o                    = GraphIndex::getObjectRepository().get((*job.results)[0].id);
            (*job.results)[0].distance = GraphIndex::objectSpace->compareWithL1(*job.object, *o);
          }
        }
        if (((job.results->size() > 0) && ((*job.results)[0].distance != 0.0)) ||
            (job.results->size() == 0)) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          Object *f = GraphIndex::objectSpace->allocateObject(*job.object);
          DVPTree::InsertContainer tiobj(*f, job.id);
#else
          DVPTree::InsertContainer tiobj(*job.object, job.id);
#endif
          try {
            DVPTree::insert(tiobj);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
            GraphIndex::objectSpace->deleteObject(f);
#endif
          } catch (Exception &err) {
            cerr << "NGT::createIndex: Fatal error. ID=" << job.id << ":";
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
            GraphIndex::objectSpace->deleteObject(f);
#endif
            if (NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeKNNG) {
              cerr << err.what() << " continue.." << endl;
            } else {
              throw err;
            }
          }
        }
      } // for

      while (!output.empty()) {
        delete output.front().results;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        GraphIndex::objectSpace->deleteObject(output.front().object);
#endif
        output.pop_front();
      }

      count += cnt;
      if (timerCount <= count) {
        timer.stop();
        cerr << "GraphAndTreeIndex::createIndexWithInsertionOrder: ";
        cerr << "Processed " << timerCount << " objects. time= " << timer
             << " vm size=" << NGT::Common::getProcessVmSizeStr() << ":" << NGT::Common::getProcessVmPeakStr()
             << endl;
        timerCount += timerInterval;
        timer.restart();
      }
      buildTimeController.adjustEdgeSize(count);
      if (pathAdjustCount > 0 && pathAdjustCount <= count) {
        GraphReconstructor::adjustPathsEffectively(static_cast<GraphIndex &>(*this));
        pathAdjustCount += property.pathAdjustmentInterval;
      }
    }
  } catch (Exception &err) {
    threads.terminate();
    throw err;
  }
  threads.terminate();
}

void GraphAndTreeIndex::createIndex(const vector<pair<NGT::Object *, size_t>> &objects,
                                    vector<InsertionResult> &ids, float range, size_t threadPoolSize) {
  Timer timer;
  size_t timerInterval = 100000;
  size_t timerCount    = timerInterval;
  size_t count         = 0;
  timer.start();
  if (threadPoolSize <= 0) {
    cerr << "Not implemented!!" << endl;
    abort();
  } else {
    CreateIndexThreadPool threads(threadPoolSize);
    CreateIndexSharedData sd(*this);
    threads.setSharedData(&sd);
    threads.create();
    CreateIndexThreadPool::OutputJobQueue &output = threads.getOutputJobQueue();
    try {
      CreateIndexJob job;
      size_t idx = 0;
      for (;;) {
        size_t cnt = 0;
        {
          for (; idx < objects.size(); idx++) {
            if (objects[idx].first == 0) {
              ids.push_back(InsertionResult());
              continue;
            }
            job.id       = 0;
            job.results  = 0;
            job.object   = objects[idx].first;
            job.batchIdx = ids.size();
            // insert an empty entry to prepare.
            ids.push_back(InsertionResult(job.id, false, 0.0));
            threads.pushInputQueue(job);
            cnt++;
            if (cnt >= (size_t)NeighborhoodGraph::property.batchSizeForCreation) {
              idx++;
              break;
            }
          }
        }
        if (cnt == 0) {
          break;
        }
        threads.waitForFinish();
        if (output.size() != cnt) {
          cerr << "NNTGIndex::insertGraphIndexByThread: Warning!! Thread response size is wrong." << endl;
          cnt = output.size();
        }
        {
          size_t size = NeighborhoodGraph::property.edgeSizeForCreation;
          sort(output.begin(), output.end());
          for (size_t idxi = 0; idxi < cnt; idxi++) {
            // add distances
            ObjectDistances &objs = *output[idxi].results;
            for (size_t idxj = 0; idxj < idxi; idxj++) {
              if (output[idxi].batchIdx == output[idxj].batchIdx) {
                continue;
              }
              if (output[idxj].id == 0) {
                continue;
              }
              ObjectDistance r;
              r.distance =
                  GraphIndex::objectSpace->getComparator()(*output[idxi].object, *output[idxj].object);
              r.id = output[idxj].id;
              objs.emplace_back(r);
            }
            std::sort(objs.begin(), objs.end());
            if (objs.size() > size) {
              objs.resize(size);
            }
            if ((objs.size() > 0) && (range >= 0.0) && (objs[0].distance <= range)) {
              // The line below was replaced by the line above to consider EPSILON for float comparison. 170702
              // if ((objs.size() > 0) && (range < 0.0 || (objs[0].distance <= range))) {
              // An identical or similar object already exits
              ids[output[idxi].batchIdx].identical = true;
              ids[output[idxi].batchIdx].id        = objs[0].id;
              ids[output[idxi].batchIdx].distance  = objs[0].distance;
              output[idxi].id                      = 0;
            } else {
              assert(output[idxi].id == 0);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
              PersistentObject *obj = GraphIndex::objectSpace->allocatePersistentObject(*output[idxi].object);
              output[idxi].id       = GraphIndex::objectSpace->insert(obj);
#else
              output[idxi].id = GraphIndex::objectSpace->insert(output[idxi].object);
#endif
              ids[output[idxi].batchIdx].id = output[idxi].id;
            }
          }
        }
        // insert resultant objects into the graph as edges
        for (size_t i = 0; i < cnt; i++) {
          CreateIndexJob &job = output.front();
          if (job.id != 0) {
            if (property.indexType == NGT::Property::GraphAndTree) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
              Object *f = GraphIndex::objectSpace->allocateObject(*job.object);
              DVPTree::InsertContainer tiobj(*f, job.id);
#else
              DVPTree::InsertContainer tiobj(*job.object, job.id);
#endif
              try {
                DVPTree::insert(tiobj);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
                GraphIndex::objectSpace->deleteObject(f);
#endif
              } catch (Exception &err) {
                cerr << "NGT::createIndex: Fatal error. ID=" << job.id << ":" << err.what();
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
                GraphIndex::objectSpace->deleteObject(f);
#endif
                if (NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeKNNG) {
                  cerr << err.what() << " continue.." << endl;
                } else {
                  throw err;
                }
              }
            }
            if (((*job.results).size() == 0) && (job.id != 1)) {
              cerr << "insert warning!! No searched nodes!. If the first time, no problem. " << job.id
                   << endl;
            }
            try {
              GraphIndex::insertNode(job.id, *job.results);
            } catch (NGT::Exception &err) {
              std::stringstream msg;
              msg << " Cannot insert the node. " << job.id << ". " << err.what();
              NGTThrowException(msg);
            }
          }
          if (job.results != 0) {
            delete job.results;
          }
          output.pop_front();
        }

        count += cnt;
        if (timerCount <= count) {
          timer.stop();
          cerr << "Processed " << timerCount << " time= " << timer << endl;
          timerCount += timerInterval;
          timer.start();
        }
      }
    } catch (Exception &err) {
      cerr << "thread terminate!" << endl;
      threads.terminate();
      throw err;
    }
    threads.terminate();
  }
}

static bool findPathAmongIdenticalObjects(GraphAndTreeIndex &graph, size_t srcid, size_t dstid) {
  stack<size_t> nodes;
  unordered_set<size_t> done;
  nodes.push(srcid);
  while (!nodes.empty()) {
    auto tid = nodes.top();
    nodes.pop();
    done.insert(tid);
    GraphNode &node = *graph.GraphIndex::getNode(tid);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    for (auto i = node.begin(graph.GraphIndex::repository.allocator);
         i != node.end(graph.GraphIndex::repository.allocator); ++i) {
#else
    for (auto i = node.begin(); i != node.end(); ++i) {
#endif
      if ((*i).distance != 0.0) {
        break;
      }
      if ((*i).id == dstid) {
        return true;
      }
      if (done.count((*i).id) == 0) {
        nodes.push((*i).id);
      }
    }
  }
  return false;
}

bool GraphAndTreeIndex::verify(vector<uint8_t> &status, bool info, char mode) {
  bool valid = GraphIndex::verify(status, info);
  if (!valid) {
    cerr << "The graph or object is invalid!" << endl;
  }
  bool treeValid = DVPTree::verify(GraphIndex::objectSpace->getRepository().size(), status);
  if (!treeValid) {
    cerr << "The tree is invalid" << endl;
  }
  valid = valid && treeValid;
  // status: tree|graph|object
  cerr << "Started checking consistency..." << endl;
  for (size_t id = 1; id < status.size(); id++) {
    if (id % 100000 == 0) {
      cerr << "The number of processed objects=" << id << endl;
    }
    if (status[id] != 0x00 && status[id] != 0x07) {
      if (status[id] == 0x03) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        NGT::Object *po = GraphIndex::objectSpace->allocateObject(*GraphIndex::getObjectRepository().get(id));
        NGT::SearchContainer sc(*po);
#else
        NGT::SearchContainer sc(*GraphIndex::getObjectRepository().get(id));
#endif
        NGT::ObjectDistances objects;
        sc.setResults(&objects);
        sc.id                     = 0;
        sc.radius                 = 0.0;
        sc.explorationCoefficient = 1.1;
        sc.edgeSize               = 0;
        ObjectDistances seeds;
        seeds.push_back(ObjectDistance(id, 0.0));
        objects.clear();
        try {
          GraphIndex::search(sc, seeds);
        } catch (Exception &err) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          GraphIndex::objectSpace->deleteObject(po);
#endif
          cerr << "Fatal Error!: Cannot search! " << err.what() << endl;
          objects.clear();
        }
        size_t n                       = 0;
        bool registeredIdenticalObject = false;
        for (; n < objects.size(); n++) {
          if (objects[n].id != id && status[objects[n].id] == 0x07) {
            registeredIdenticalObject = true;
            break;
          }
        }
        if (!registeredIdenticalObject) {
          if (info) {
            cerr << "info: not found the registered same objects. id=" << id << " size=" << objects.size()
                 << endl;
          }
          sc.id                     = 0;
          sc.radius                 = FLT_MAX;
          sc.explorationCoefficient = 1.2;
          sc.edgeSize               = 0;
          sc.size                   = objects.size() < 100 ? 100 : objects.size() * 2;
          ObjectDistances seeds;
          seeds.push_back(ObjectDistance(id, 0.0));
          objects.clear();
          try {
            GraphIndex::search(sc, seeds);
          } catch (Exception &err) {
            cerr << "Fatal Error!: Cannot search! " << err.what() << endl;
            objects.clear();
          }
          registeredIdenticalObject = false;
          for (n = 0; n < objects.size(); n++) {
            if (objects[n].distance != 0.0) break;
            if (objects[n].id != id && status[objects[n].id] == 0x07) {
              registeredIdenticalObject = true;
              if (info) {
                cerr << "info: found by using mode accurate search. " << objects[n].id << endl;
              }
              break;
            }
          }
        }
        if (!registeredIdenticalObject && mode != 's') {
          if (info) {
            cerr << "info: not found by using more accurate search." << endl;
          }
          sc.id                     = 0;
          sc.radius                 = 0.0;
          sc.explorationCoefficient = 1.1;
          sc.edgeSize               = 0;
          sc.size                   = SIZE_MAX;
          objects.clear();
          linearSearch(sc);
          n                         = 0;
          registeredIdenticalObject = false;
          for (; n < objects.size(); n++) {
            if (objects[n].distance != 0.0) break;
            if (objects[n].id != id && status[objects[n].id] == 0x07) {
              registeredIdenticalObject = true;
              if (info) {
                cerr << "info: found by using linear search. " << objects[n].id << endl;
              }
              break;
            }
          }
        }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        GraphIndex::objectSpace->deleteObject(po);
#endif
        if (registeredIdenticalObject) {
          if (info) {
            cerr << "Info ID=" << id << ":" << static_cast<int>(status[id]) << endl;
            cerr << "  found the valid same objects. " << objects[n].id << endl;
          }
          GraphNode &fromNode = *GraphIndex::getNode(id);
          bool fromFound      = false;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          for (auto i = fromNode.begin(GraphIndex::repository.allocator);
               i != fromNode.end(GraphIndex::repository.allocator); ++i) {
#else
          for (auto i = fromNode.begin(); i != fromNode.end(); ++i) {
#endif
            if ((*i).id == objects[n].id) {
              fromFound = true;
            }
          }
          GraphNode &toNode = *GraphIndex::getNode(objects[n].id);
          bool toFound      = false;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          for (auto i = toNode.begin(GraphIndex::repository.allocator);
               i != toNode.end(GraphIndex::repository.allocator); ++i) {
#else
          for (auto i = toNode.begin(); i != toNode.end(); ++i) {
#endif
            if ((*i).id == id) {
              toFound = true;
            }
          }
          if (!fromFound || !toFound) {
            if (info) {
              if (!fromFound && !toFound) {
                cerr << "Warning no undirected edge between " << id << "(" << fromNode.size() << ") and "
                     << objects[n].id << "(" << toNode.size() << ")." << endl;
              } else if (!fromFound) {
                cerr << "Warning no directed edge from " << id << "(" << fromNode.size() << ") to "
                     << objects[n].id << "(" << toNode.size() << ")." << endl;
              } else if (!toFound) {
                cerr << "Warning no reverse directed edge from " << id << "(" << fromNode.size() << ") to "
                     << objects[n].id << "(" << toNode.size() << ")." << endl;
              }
            }
            if (!findPathAmongIdenticalObjects(*this, id, objects[n].id)) {
              cerr << "Warning no path from " << id << " to " << objects[n].id << endl;
            }
            if (!findPathAmongIdenticalObjects(*this, objects[n].id, id)) {
              cerr << "Warning no reverse path from " << id << " to " << objects[n].id << endl;
            }
          }
        } else {
          if (mode == 's') {
            cerr << "Warning: not found the valid same object, but not try to use linear search." << endl;
            cerr << "Error! ID=" << id << ":" << static_cast<int>(status[id]) << endl;
          } else {
            cerr << "Warning: not found the valid same object even by using linear search." << endl;
            cerr << "Error! ID=" << id << ":" << static_cast<int>(status[id]) << endl;
            valid = false;
          }
        }
      } else if (status[id] == 0x01) {
        if (info) {
          cerr << "Warning! ID=" << id << ":" << static_cast<int>(status[id]) << endl;
          cerr << "  not inserted into the indexes" << endl;
        }
      } else {
        cerr << "Error! ID=" << id << ":" << static_cast<int>(status[id]) << endl;
        valid = false;
      }
    }
  }
  return valid;
}

#ifdef NGT_FOREST
namespace {
void getClustersFromTree(NGT::GraphAndTreeIndex &graphAndTreeIndex, std::vector<NGT::Node::ID> &leafIDs,
                         std::vector<NGT::ObjectDistances> &clusterIds, std::vector<bool> &isClustered) {
  graphAndTreeIndex.getAllLeafNodeIDs(leafIDs);
  clusterIds.reserve(leafIDs.size());
  size_t objcount = 0;
  size_t existed  = 0;
  for (auto &leafid : leafIDs) {
    NGT::LeafNode &leaf = *static_cast<NGT::LeafNode *>(graphAndTreeIndex.DVPTree::getNode(leafid));
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    NGT::ObjectDistances objs(leaf.getObjectIDs(graphAndTreeIndex.DVPTree::leafNodes.allocator),
                              leaf.getObjectIDs(graphAndTreeIndex.DVPTree::leafNodes.allocator) +
                                  leaf.getObjectSize());
#else
    NGT::ObjectDistances objs(leaf.getObjectIDs(), leaf.getObjectIDs() + leaf.getObjectSize());
#endif
    for (auto obj : objs) {
      if (isClustered[obj.id]) {
        std::cerr << "already extracted! " << obj.id << ":" << leafid.getID() << std::endl;
        existed++;
        continue;
      }
      isClustered[obj.id] = true;
      objcount++;
    }
    clusterIds.emplace_back(std::move(objs));
  }
  std::cerr << "Total objects in clusters from leaves: " << objcount << ":" << existed << std::endl;
}

void expandClustersBySearch(size_t clusterSize, float clusterSizeFactor, NGT::Index &anng,
                            std::vector<NGT::ObjectDistances> &clusterIds,
                            std::vector<NGT::ObjectDistances> &expandedClusterIds,
                            std::vector<bool> &isClustered) {
  if (clusterSize == 0 && clusterSizeFactor == 0.0) {
    return;
  }
  expandedClusterIds.resize(clusterIds.size());
#pragma omp parallel for
  for (size_t idx = 0; idx < clusterIds.size(); idx++) {
    if (clusterIds[idx].size() == 0) {
      std::cerr << "warning! no seeds! But continue. " << idx << std::endl;
      continue;
    }
    auto nodeId = clusterIds[idx][0];
    if (nodeId.id == 0) {
      std::cerr << "fatal error! node id is zero!" << std::endl;
      abort();
    }
    std::unordered_set<NGT::ObjectID> isMember;
    auto &cluster = clusterIds[idx];
    for (auto &object : cluster) {
      isMember.insert(object.id);
    }
    {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      NGT::Object *optr =
          anng.getObjectSpace().allocateObject(*anng.getObjectSpace().getRepository().get(nodeId.id));
      NGT::Object &queryObject = *optr;
#else
      NGT::Object &queryObject = *anng.getObjectSpace().getRepository().get(nodeId.id);
#endif
      NGT::ObjectDistances results;
      NGT::SearchContainer searchContainer(queryObject);
      searchContainer.setResults(&results);
      size_t size = clusterSize;
      if (clusterSizeFactor > 0.0) {
        size = clusterIds[idx].size() * clusterSizeFactor;
      }
      searchContainer.setSize(size);
      searchContainer.setEpsilon(0.1);
      anng.search(searchContainer);
      for (const auto &result : results) {
        if (isMember.count(result.id) != 0) continue;
        expandedClusterIds[idx].emplace_back(result);
        isClustered[result.id] = true;
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      anng.getObjectSpace().deleteObject(optr);
#endif
    }
  }
}

void expandClustersToKHops(NGT::Index &anng, std::vector<NGT::ObjectDistances> &clusterIds,
                           std::vector<NGT::ObjectDistances> &expandedClusterIds,
                           std::vector<bool> &isClustered, size_t k = 2, size_t nOfEdges = 32) {
  k--;
  size_t nOfAddedNodes          = 0;
  NGT::GraphAndTreeIndex &graph = dynamic_cast<NGT::GraphAndTreeIndex &>(anng.getIndex());
  expandedClusterIds.resize(clusterIds.size());
#pragma omp parallel for
  for (size_t idx = 0; idx < clusterIds.size(); idx++) {
    auto &cluster         = clusterIds[idx];
    auto &expandedCluster = expandedClusterIds[idx];
    if (cluster.size() == 0) {
      std::cerr << "warning! no objects! But continue. " << idx << std::endl;
      continue;
    }
    std::vector<bool> isMember(graph.getObjectSpace().getRepository().size(), false);
    for (auto &object : cluster) {
      isMember[object.id] = true;
    }
    NGT::ObjectDistances addedObjects;
    for (auto &object : cluster) {
      NGT::GraphNode &node = *graph.GraphIndex::getNode(object.id);
      size_t c             = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      auto &allocator = graph.getObjectSpace().getRepository().getAllocator();
      for (size_t i = 0; i < node.size() && c < nOfEdges; i++, c++) {
        if (!isMember[node.at(i, allocator).id]) {
          addedObjects.emplace_back(node.at(i, allocator));
          addedObjects.back().distance       = std::numeric_limits<float>::max();
          isMember[node.at(i, allocator).id] = true;
        }
      }
#else
      for (auto &o : node) {
        if (!isMember[o.id]) {
          addedObjects.emplace_back(o);
          addedObjects.back().distance = std::numeric_limits<float>::max();
          isMember[o.id]               = true;
        }
        if (++c >= nOfEdges) break;
      }
#endif
    }
    nOfAddedNodes += addedObjects.size();
    expandedCluster.insert(expandedCluster.end(), addedObjects.begin(), addedObjects.end());
    NGT::ObjectDistances nextAddedObjects;
    for (size_t cnt = 0; cnt < k; cnt++) {
      for (auto &object : addedObjects) {
        NGT::GraphNode &node = *graph.GraphIndex::getNode(object.id);
        size_t c             = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        auto &allocator = graph.getObjectSpace().getRepository().getAllocator();
        for (size_t i = 0; i < node.size() && c < nOfEdges; i++, c++) {
          if (!isMember[node.at(i, allocator).id]) {
            nextAddedObjects.emplace_back(node.at(i, allocator));
            addedObjects.back().distance       = std::numeric_limits<float>::max();
            isMember[node.at(i, allocator).id] = true;
          }
        }
#else
        for (auto &o : node) {
          if (!isMember[o.id]) {
            nextAddedObjects.emplace_back(o);
            addedObjects.back().distance = std::numeric_limits<float>::max();
            isMember[o.id]               = true;
          }
          if (++c >= nOfEdges) break;
        }
#endif
      }
      nOfAddedNodes += nextAddedObjects.size();
      expandedCluster.insert(expandedCluster.end(), nextAddedObjects.begin(), nextAddedObjects.end());
      addedObjects = std::move(nextAddedObjects);
      nextAddedObjects.clear();
    }
  }
  std::cerr << "# of added objects=" << nOfAddedNodes << std::endl;
}

void addLeakedNodes(NGT::Index &index, NGT::Index &anng, std::vector<std::vector<NGT::ObjectID>> &seedNodes,
                    std::vector<NGT::ObjectDistances> &clusterIds, std::vector<bool> &isClustered) {
  NGT::ObjectSpace &objectSpace           = index.getObjectSpace();
  NGT::ObjectRepository &objectRepository = objectSpace.getRepository();
  size_t repositorySize                   = objectRepository.size();
  size_t nOfAddedObjects                  = 0;
  for (size_t nodeId = 1; nodeId < repositorySize; nodeId++) {
    if (isClustered[nodeId]) {
      continue;
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    NGT::Object *optr =
        anng.getObjectSpace().allocateObject(*anng.getObjectSpace().getRepository().get(nodeId));
    NGT::Object &queryObject = *optr;
#else
    NGT::Object &queryObject = *anng.getObjectSpace().getRepository().get(nodeId);
#endif
    float minDistance          = std::numeric_limits<float>::max();
    size_t closestClusterIndex = 0;
    for (size_t j = 0; j < seedNodes.size(); j++) {
      if (seedNodes[j].size() == 0) {
        std::cerr << "warning! no seeds! But continue. " << j << std::endl;
        continue;
      }
      const auto &seedNodeId = seedNodes[j][0];
      float distance         = objectSpace.getComparator()(queryObject, *objectRepository.get(seedNodeId));
      if (distance < minDistance) {
        minDistance         = distance;
        closestClusterIndex = j;
      }
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    anng.getObjectSpace().deleteObject(optr);
#endif
    clusterIds[closestClusterIndex].emplace_back(nodeId, minDistance);
    nOfAddedObjects++;
    isClustered[nodeId] = true;
    std::cerr << "Node " << nodeId << " added to cluster of seed node " << seedNodes[closestClusterIndex][0]
              << " with distance " << minDistance << std::endl;
  }
  std::cerr << "Total nodes added to clusters: " << nOfAddedObjects << std::endl;
}

void growForest(NGT::Index &index, std::vector<std::vector<NGT::ObjectID>> &seedNodes,
                std::vector<NGT::ObjectDistances> &clusterIds, size_t incomingEdge, size_t outgoingEdge,
                float epsilon) {
  std::cerr << "growForest" << std::endl;
  NGT::ObjectSpace &objectSpace           = index.getObjectSpace();
  NGT::ObjectRepository &objectRepository = objectSpace.getRepository();
  NGT::GraphIndex &graphIndex             = static_cast<NGT::GraphIndex &>(index.getIndex());
  for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
    for (const auto &clusterNode : clusterIds[seedidx]) {
      if (objectRepository.isEmpty(clusterNode.id)) {
        std::cerr << "Warning! Cluster member " << clusterNode.id << " is empty, skipping." << std::endl;
        continue;
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      NGT::Object *qoptr       = objectSpace.allocateObject(*objectRepository.get(clusterNode.id));
      NGT::Object &queryObject = *qoptr;
#else
      NGT::Object &queryObject = *objectRepository.get(clusterNode.id);
#endif
      NGT::ObjectDistances results;
      NGT::SearchContainer searchContainer(queryObject);
      searchContainer.setResults(&results);
      searchContainer.setSize(std::max(incomingEdge, outgoingEdge) + 1);
      searchContainer.setEdgeSize(0);
      searchContainer.setEpsilon(epsilon);
      NGT::ObjectDistances seeds;
      seeds.reserve(seedNodes[seedidx].size());
      for (auto &seed : seedNodes[seedidx]) {
        seeds.emplace_back(NGT::ObjectDistance(seed, 0.0));
      }
      index.search(searchContainer, seeds);
      size_t edgeCount = 0;
      for (size_t idx = 0; idx < results.size(); idx++) {
        const auto &result = results[idx];
        if (result.id == clusterNode.id) {
          continue;
        }
        try {
          if (edgeCount < incomingEdge) {
            graphIndex.addEdge(result.id, clusterNode.id, result.distance, false);
          }
          if (edgeCount < outgoingEdge) {
            graphIndex.addEdge(clusterNode.id, result.id, result.distance, false);
          }
        } catch (...) {
        }
        edgeCount++;
      }
    }
  }
}

void growForestMultiThread(NGT::Index &index, std::vector<std::vector<NGT::ObjectID>> &seedNodes,
                           std::vector<NGT::ObjectDistances> &clusterIds, size_t incomingEdge,
                           size_t outgoingEdge, float epsilon) {
  std::cerr << "growForestMultiThread" << std::endl;
  NGT::ObjectSpace &objectSpace           = index.getObjectSpace();
  NGT::ObjectRepository &objectRepository = objectSpace.getRepository();
  NGT::GraphIndex &graphIndex             = static_cast<NGT::GraphIndex &>(index.getIndex());
#pragma omp parallel for
  for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
    for (const auto &clusterNode : clusterIds[seedidx]) {
      if (objectRepository.isEmpty(clusterNode.id)) {
        std::cerr << "Warning! Cluster member " << clusterNode.id << " is empty, skipping." << std::endl;
        continue;
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      NGT::Object *qoptr       = objectSpace.allocateObject(*objectRepository.get(clusterNode.id));
      NGT::Object &queryObject = *qoptr;
#else
      NGT::Object &queryObject = *objectRepository.get(clusterNode.id);
#endif
      NGT::ObjectDistances results;
      NGT::SearchContainer searchContainer(queryObject);
      searchContainer.setResults(&results);
      searchContainer.setSize(std::max(incomingEdge, outgoingEdge) + 1);
      searchContainer.setEdgeSize(0);
      searchContainer.setEpsilon(epsilon);
      NGT::ObjectDistances seeds;
      seeds.reserve(seedNodes[seedidx].size());
      for (auto &seed : seedNodes[seedidx]) {
        seeds.emplace_back(NGT::ObjectDistance(seed, 0.0));
      }
      index.search(searchContainer, seeds);
      size_t edgeCount = 0;
      for (size_t idx = 0; idx < results.size(); idx++) {
        const auto &result = results[idx];
        if (result.id == clusterNode.id) {
          continue;
        }
        try {
          if (edgeCount < incomingEdge) {
            graphIndex.addEdge(result.id, clusterNode.id, result.distance, false);
          }
          if (edgeCount < outgoingEdge) {
            graphIndex.addEdge(clusterNode.id, result.id, result.distance, false);
          }
        } catch (...) {
        }
        edgeCount++;
      }
    }
  }
}

void growForestParallel(NGT::Index &index, std::vector<std::vector<NGT::ObjectID>> &seedNodes,
                        std::vector<NGT::ObjectDistances> &clusterIds, size_t incomingEdge,
                        size_t outgoingEdge, float epsilon) {
  NGT::ObjectSpace &objectSpace           = index.getObjectSpace();
  NGT::ObjectRepository &objectRepository = objectSpace.getRepository();
  NGT::GraphIndex &graphIndex             = static_cast<NGT::GraphIndex &>(index.getIndex());
  bool done                               = false;
  for (size_t idx = 0; !done; idx++) {
    done = true;
    for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
      if (idx >= clusterIds[seedidx].size()) {
        continue;
      }
      done = false;
      std::cerr << clusterIds[seedidx][idx] << std::endl;
      const auto &clusterNode = clusterIds[seedidx][idx];
      if (objectRepository.isEmpty(clusterNode.id)) {
        std::cerr << "Warning! Cluster member " << clusterNode.id << " is empty, skipping." << std::endl;
        continue;
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      NGT::Object *qoptr       = objectSpace.allocateObject(*objectRepository.get(clusterNode.id));
      NGT::Object &queryObject = *qoptr;
#else
      NGT::Object &queryObject = *objectRepository.get(clusterNode.id);
#endif
      NGT::ObjectDistances results;
      NGT::SearchContainer searchContainer(queryObject);
      searchContainer.setResults(&results);
      searchContainer.setSize(std::max(incomingEdge, outgoingEdge) + 1);
      searchContainer.setEdgeSize(0);
      searchContainer.setEpsilon(epsilon);
      NGT::ObjectDistances seeds;
      seeds.emplace_back(clusterIds[seedidx][0]);
      index.search(searchContainer, seeds);
      size_t edgeCount = 0;

      for (size_t ridx = 0; ridx < results.size(); ridx++) {
        const auto &result = results[ridx];
        if (result.id == clusterNode.id) {
          continue;
        }
        try {
          if (edgeCount < incomingEdge) {
            graphIndex.addEdge(result.id, clusterNode.id, result.distance, false);
          }
          if (edgeCount < outgoingEdge) {
            graphIndex.addEdge(clusterNode.id, result.id, result.distance, false);
          }
        } catch (...) {
        }
        edgeCount++;
      }
    }
  }
}

void growForestParallelMultiThread(NGT::Index &index, std::vector<std::vector<NGT::ObjectID>> &seedNodes,
                                   std::vector<NGT::ObjectDistances> &clusterIds, size_t incomingEdge,
                                   size_t outgoingEdge, float epsilon) {

  NGT::ObjectSpace &objectSpace           = index.getObjectSpace();
  NGT::ObjectRepository &objectRepository = objectSpace.getRepository();
  NGT::GraphIndex &graphIndex             = static_cast<NGT::GraphIndex &>(index.getIndex());

  bool done = false;
  for (size_t idx = 0; !done; idx++) {
    done = true;
    std::vector<std::vector<std::tuple<NGT::ObjectID, NGT::ObjectID, float>>> edges(seedNodes.size());
#pragma omp parallel for
    for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
      if (idx >= clusterIds[seedidx].size()) {
        continue;
      }
      done                    = false;
      const auto &clusterNode = clusterIds[seedidx][idx];
      if (objectRepository.isEmpty(clusterNode.id)) {
        std::cerr << "Warning! Cluster member " << clusterNode.id << " is empty, skipping." << std::endl;
        continue;
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      NGT::Object *qoptr       = objectSpace.allocateObject(*objectRepository.get(clusterNode.id));
      NGT::Object &queryObject = *qoptr;
#else
      NGT::Object &queryObject = *objectRepository.get(clusterNode.id);
#endif
      NGT::ObjectDistances results;
      NGT::SearchContainer searchContainer(queryObject);
      searchContainer.setResults(&results);
      searchContainer.setSize(std::max(incomingEdge, outgoingEdge) + 1);
      searchContainer.setEdgeSize(0);
      searchContainer.setEpsilon(epsilon);
      NGT::ObjectDistances seeds;
      seeds.reserve(seedNodes[seedidx].size());
      for (auto &seed : seedNodes[seedidx]) {
        seeds.emplace_back(NGT::ObjectDistance(seed, 0.0));
      }
      index.search(searchContainer, seeds);
      size_t edgeCount = 0;
      // Add edges from seed node to each cluster member

      for (size_t ridx = 0; ridx < results.size(); ridx++) {
        const auto &result = results[ridx];
        if (result.id == clusterNode.id) {
          continue;
        }
        if (edgeCount < incomingEdge) {
          edges[seedidx].emplace_back(result.id, clusterNode.id, result.distance);
        }
        if (edgeCount < outgoingEdge) {
          edges[seedidx].emplace_back(clusterNode.id, result.id, result.distance);
        }
        edgeCount++;
      }
    }
    for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
      for (const auto &edge : edges[seedidx]) {
        try {
          graphIndex.addEdge(std::get<0>(edge), std::get<1>(edge), std::get<2>(edge), false);
        } catch (...) {
        }
      }
    }
  }
}

void growForestIncrementalParallelMultiThread(NGT::Index &index,
                                              std::vector<std::vector<NGT::ObjectID>> &seedNodes,
                                              std::vector<NGT::ObjectDistances> &clusterIds,
                                              size_t incomingEdge, size_t outgoingEdge, float epsilon) {
  NGT::ObjectSpace &objectSpace           = index.getObjectSpace();
  NGT::ObjectRepository &objectRepository = objectSpace.getRepository();
  NGT::GraphIndex &graphIndex             = static_cast<NGT::GraphIndex &>(index.getIndex());

  bool done        = false;
  size_t batchSize = 8;
  for (size_t idx = 0; !done; idx++) {
    std::cerr << "Processing the rank=" << idx << std::endl;
    done = true;
    std::vector<size_t> seedidxes;
    seedidxes.reserve(batchSize);
    for (size_t sidx = 0;; sidx++) {
      if (seedidxes.size() >= batchSize || sidx >= seedNodes.size()) {
        std::vector<std::vector<std::tuple<NGT::ObjectID, NGT::ObjectID, float>>> edges(seedidxes.size());
#pragma omp parallel for
        for (size_t seedidxesidx = 0; seedidxesidx < seedidxes.size(); seedidxesidx++) {
          size_t seedidx = seedidxes[seedidxesidx];
          if (seedidx >= seedNodes.size()) {
            continue;
          }
          if (idx >= clusterIds[seedidx].size()) {
            continue;
          }
          done                    = false;
          const auto &clusterNode = clusterIds[seedidx][idx];
          if (objectRepository.isEmpty(clusterNode.id)) {
            std::cerr << "Warning! Cluster member " << clusterNode.id << " is empty, skipping." << std::endl;
            continue;
          }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          NGT::Object *optr        = objectSpace.allocateObject(*objectRepository.get(clusterNode.id));
          NGT::Object &queryObject = *optr;
#else
          NGT::Object &queryObject = *objectRepository.get(clusterNode.id);
#endif
          NGT::ObjectDistances results;
          NGT::SearchContainer searchContainer(queryObject);
          searchContainer.setResults(&results);
          searchContainer.setSize(std::max(incomingEdge, outgoingEdge) + 1);
          searchContainer.setEdgeSize(0);
          searchContainer.setEpsilon(epsilon);
          NGT::ObjectDistances seeds;
          seeds.reserve(seedNodes[seedidx].size());
          for (auto &seed : seedNodes[seedidx]) {
            seeds.emplace_back(NGT::ObjectDistance(seed, 0.0));
          }
          index.search(searchContainer, seeds);
          size_t edgeCount = 0;
          for (size_t ridx = 0; ridx < results.size(); ridx++) {
            const auto &result = results[ridx];
            if (result.id == clusterNode.id) {
              continue;
            }
            if (edgeCount < incomingEdge) {
              edges[seedidxesidx].emplace_back(result.id, clusterNode.id, result.distance);
            }
            if (edgeCount < outgoingEdge) {
              edges[seedidxesidx].emplace_back(clusterNode.id, result.id, result.distance);
            }
            edgeCount++;
          }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
          objectSpace.deleteObject(optr);
#endif
        }
        for (auto &sdedge : edges) {
          for (const auto &edge : sdedge) {
            try {
              graphIndex.addEdge(std::get<0>(edge), std::get<1>(edge), std::get<2>(edge), false);
            } catch (...) {
            }
          }
        }
        seedidxes.clear();
        if (sidx >= seedNodes.size()) {
          break;
        }
      }
      if (idx >= clusterIds[sidx].size()) {
        continue;
      }
      seedidxes.emplace_back(sidx);
    }
  }
}

void growForestForExpandedNodesFromGraph(NGT::Index &index, NGT::Index &anng,
                                         std::vector<std::vector<NGT::ObjectID>> &seedNodes,
                                         std::vector<NGT::ObjectDistances> &clusterIds,
                                         std::vector<NGT::ObjectDistances> &expandedClusterIds,
                                         size_t incomingEdge, size_t outgoingEdge) {

  NGT::GraphIndex &graphIndex   = static_cast<NGT::GraphIndex &>(index.getIndex());
  NGT::GraphAndTreeIndex &graph = dynamic_cast<NGT::GraphAndTreeIndex &>(anng.getIndex());
  std::vector<std::vector<std::pair<NGT::ObjectID, NGT::ObjectDistance>>> addedEdges(clusterIds.size());
  size_t nOfEdges = std::max(incomingEdge, outgoingEdge);

#pragma omp parallel for
  for (size_t idx = 0; idx < clusterIds.size(); idx++) {
    auto &cluster         = clusterIds[idx];
    auto &expandedCluster = expandedClusterIds[idx];
    if (expandedCluster.size() == 0) {
      std::cerr << "warning! no objects in the expanded cluster! But continue. " << idx << std::endl;
      continue;
    }
    std::unordered_set<NGT::ObjectID> isMember;
    for (auto &object : cluster) {
      isMember.insert(object.id);
    }
    for (auto &object : seedNodes[idx]) {
      isMember.insert(object);
    }
    if (isMember.size() == 0) {
      std::cerr << "warning! no objects int the cluster! But continue. " << idx << std::endl;
      continue;
    }
    for (auto &object : expandedCluster) {
      NGT::GraphNode &node = *graph.GraphIndex::getNode(object.id);
      size_t c             = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      auto &allocator = graph.getObjectSpace().getRepository().getAllocator();
      for (size_t i = 0; i < node.size(); i++) {
        if (isMember.count(node.at(i, allocator).id) == 0) {
          continue;
        }
        if (c >= nOfEdges) break;
        if (c < incomingEdge) {
          try {
            graphIndex.addEdge(node.at(i, allocator).id, object.id, node.at(i, allocator).distance, true);
          } catch (...) {
          };
        }
        if (c < outgoingEdge) {
          addedEdges[idx].emplace_back(std::make_pair(object.id, node.at(i, allocator)));
        }
        c++;
      }
#else
      for (auto &o : node) {
        if (isMember.count(o.id) == 0) {
          continue;
        }
        if (c >= nOfEdges) break;
        if (c < incomingEdge) {
          try {
            graphIndex.addEdge(o.id, object.id, o.distance, true);
          } catch (...) {
          };
        }
        if (c < outgoingEdge) {
          addedEdges[idx].emplace_back(std::make_pair(object.id, o));
        }
        c++;
      }
#endif
    }
  }

  std::cerr << "end of extracting edges from anng" << std::endl;
  for (auto &addedEdgesInCluster : addedEdges) {
    for (size_t i = 0; i < addedEdgesInCluster.size(); i++) {
      auto &edge = addedEdgesInCluster[i];
      try {
        graphIndex.addEdge(edge.first, edge.second.id, edge.second.distance, true);
      } catch (...) {
      };
    }
  }
}

} // namespace

void NGT::Index::buildForest(const std::string &targetPath, const std::string &anngPath, size_t clusterSize,
                             float clusterSizeFactor, const std::string &mode, const std::string &emode,
                             size_t incomingEdge, size_t outgoingEdge, size_t incomingExternalEdge,
                             size_t outgoingExternalEdge, float epsilon, size_t nOfHops,
                             size_t nOfEdgesForHop, char clusterExpansion) {
  string indexPath      = targetPath;
  char clusterExpantion = clusterExpansion;

  NGT::Index::create(indexPath, anngPath, true);
  std::filesystem::copy_file(anngPath + "/obj", indexPath + "/obj",
                             std::filesystem::copy_options::overwrite_existing);
  std::filesystem::copy_file(anngPath + "/tre", indexPath + "/tre",
                             std::filesystem::copy_options::overwrite_existing);

  NGT::Index index(indexPath);
  size_t repositorySize = index.getObjectRepositorySize();

  NGT::GraphIndex &graphIndex = static_cast<NGT::GraphIndex &>(index.getIndex());

  NGT::ObjectSpace &objectSpace           = index.getObjectSpace();
  NGT::ObjectRepository &objectRepository = objectSpace.getRepository();
  NGT::Index anng(anngPath);
  std::vector<bool> isClustered(repositorySize, false);
  std::vector<NGT::ObjectDistances> clusterIds;
  std::vector<std::vector<NGT::ObjectID>> seedNodes;
  std::vector<Node::ID> leafIDs;
  NGT::GraphAndTreeIndex &graphAndTreeIndex = static_cast<NGT::GraphAndTreeIndex &>(index.getIndex());
  NGT::Timer timer;
  timer.start();
  getClustersFromTree(graphAndTreeIndex, leafIDs, clusterIds, isClustered);
  timer.stop();
  std::cerr << "Extracted all objects from the leaves. Timer=" << timer << std::endl;
  timer.start();
#define USE_SEEDS_FROM_TREE
#if defined(USE_SEEDS_FROM_TREE)
  {
    for (auto &cluster : clusterIds) {
      NGT::ObjectDistances seeds = cluster;
      graphAndTreeIndex.getSeedsFromObjects(36, seeds);
      std::vector<NGT::ObjectID> sds;
      sds.reserve(seeds.size());
      for (auto &seed : seeds) {
        sds.emplace_back(seed.id);
      }
      seedNodes.emplace_back(std::move(sds));
    }
  }
#elif defined(USE_CENTROID_SEEDS)
  {
    for (size_t idx = 0; clusterIds.size(); idx++) {
      size_t count = 0;
      std::vector<float> centroid;
      for (auto &obj : clusterIds[idx]) {
        std::vector<float> v;
        if (objectRepository.isEmpty(obj.id)) {
          continue;
        }
        objectSpace.getObject(obj.id, v);
        if (centroid.empty()) {
          centroid = v;
        } else {
          for (size_t i = 0; i < v.size(); ++i) {
            centroid[i] += v[i];
          }
        }
        count++;
      }
      for (auto &value : centroid) {
        value /= count; // Calculate the mean
      }
      NGT::Object *cent = objectSpace.allocateNormalizedObject(centroid);
      for (auto &obj : clusterIds[idx]) {
        NGT::Object &o = *objectRepository.get(obj.id);
        obj.distance   = objectSpace.getComparator()(*cent, o);
      }
      objectSpace.deleteObject(cent);
      std::sort(clusterIds.begin(), clusterIds.end());
    }
  }
#elif defined(USE_RANDOM_SEEDS)
  {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, repositorySize - 1);

    std::set<NGT::ObjectID> randomNodeIds;
    while (randomNodeIds.size() < 10) {
      NGT::ObjectID randomId = dis(gen);
      if (!graphRepository.isEmpty(randomId)) {
        randomNodeIds.insert(randomId);
      }
    }

    for (const auto &id : randomNodeIds) {
      seedNodes.emplace_back(id);
    }
  }
#endif
  timer.stop();
  std::cerr << "Extracted seads. Timer=" << timer << std::endl;

  timer.start();
  std::vector<NGT::ObjectDistances> expandedClusterIds;
  if (clusterExpantion == 's' || clusterExpantion == 'S') {
    expandClustersBySearch(clusterSize, clusterSizeFactor, anng, clusterIds, expandedClusterIds, isClustered);
  } else if (clusterExpantion == 'h' || clusterExpantion == 'H') {
    expandClustersToKHops(anng, clusterIds, expandedClusterIds, isClustered, nOfHops, nOfEdgesForHop);
  }
  if (clusterExpantion == 's' || clusterExpantion == 'h') {
    size_t nOfAddedNodes = 0;
    for (size_t i = 0; i < clusterIds.size(); i++) {
      nOfAddedNodes += expandedClusterIds[i].size();
      clusterIds[i].insert(clusterIds[i].end(), expandedClusterIds[i].begin(), expandedClusterIds[i].end());
    }
    expandedClusterIds.clear();
  }
  timer.stop();
  std::cerr << "Expanded the clusters. Timer=" << timer << std::endl;

  timer.start();
  addLeakedNodes(index, anng, seedNodes, clusterIds, isClustered);
  timer.stop();
  std::cerr << "added leadked nodes. Timer= " << timer << std::endl;

  timer.start();
  for (size_t i = 0; i < clusterIds.size(); i++) {
    std::unordered_set<NGT::ObjectID> seeds;
    for (auto &seed : seedNodes[i]) {
      seeds.insert(seed);
    }
    std::unordered_set<NGT::ObjectID> toDelete;
    for (auto &id : clusterIds[i]) {
      if (seeds.find(id.id) != seeds.end()) {
        toDelete.insert(id.id);
        seeds.erase(id.id);
        if (seeds.empty()) {
          break;
        }
      }
    }
    if (!seeds.empty()) {
      std::cerr << "Warning! Cannot find the seeds in the cluster. " << seeds.size() << ":" << i << std::endl;
    }
    if (!toDelete.empty()) {
      auto it = std::remove_if(
          clusterIds[i].begin(), clusterIds[i].end(),
          [&toDelete](const NGT::ObjectDistance &od) { return toDelete.find(od.id) != toDelete.end(); });
      clusterIds[i].erase(it, clusterIds[i].end());
    }
  }
  timer.stop();
  std::cerr << "removed seeds from the clusters. Timer=" << timer << std::endl;

  timer.start();
  for (size_t nodeId = repositorySize - 1; nodeId > 0; nodeId--) {
    if (objectRepository.isEmpty(nodeId)) {
      continue;
    }
    try {
      NGT::ObjectDistances empty;
      graphIndex.repository.insert(nodeId, empty);
    } catch (NGT::Exception &err) {
      std::cerr << "Error adding object " << nodeId << ": " << err.what() << std::endl;
      continue;
    }
    if (nodeId % 100000 == 0) {
      std::cerr << "Added object " << nodeId << " to graph repository." << std::endl;
    }
  }
  timer.stop();
  std::cerr << "All empty objects were added to the graph. Timer=" << timer << std::endl;

  timer.start();
  size_t nOfEmptyClusters = 0;
#pragma omp parallel for
  for (size_t seedidx = 0; seedidx < seedNodes.size(); seedidx++) {
    if (clusterIds[seedidx].empty()) {
      nOfEmptyClusters++;
      continue;
    }
    {
      std::sort(clusterIds[seedidx].begin(), clusterIds[seedidx].end());
    }
  }
  timer.stop();
  if (nOfEmptyClusters != 0) {
    std::cerr << "Warning! Found empty clusters. " << nOfEmptyClusters << std::endl;
  }
  std::cerr << "sorted objects to add to the graph. Timer=" << timer << std::endl;
  timer.start();
  if (mode == "s" || mode == "-") {
    if (expandedClusterIds.empty()) {
      growForest(index, seedNodes, clusterIds, incomingEdge, outgoingEdge, epsilon);
    } else {
      growForestMultiThread(index, seedNodes, clusterIds, incomingEdge, outgoingEdge, epsilon);
    }
  } else if (mode == "p") {
    growForestParallel(index, seedNodes, clusterIds, incomingEdge, outgoingEdge, epsilon);
  } else if (mode == "pm") {
    growForestParallelMultiThread(index, seedNodes, clusterIds, incomingEdge, outgoingEdge, epsilon);
  } else if (mode == "ipm") {
    growForestIncrementalParallelMultiThread(index, seedNodes, clusterIds, incomingEdge, outgoingEdge,
                                             epsilon);
  } else {
  }
  timer.stop();
  std::cerr << "grew the forest. Timer=" << timer << std::endl;
  timer.start();
  if (!expandedClusterIds.empty() && (mode == "s" || mode == "-")) {
    if (emode == "s") {
      growForest(index, seedNodes, expandedClusterIds, incomingExternalEdge, outgoingExternalEdge, epsilon);
    } else if (emode == "ipm") {
      growForestIncrementalParallelMultiThread(index, seedNodes, expandedClusterIds, incomingExternalEdge,
                                               outgoingExternalEdge, epsilon);
    } else if (emode == "g") {
      growForestForExpandedNodesFromGraph(index, anng, seedNodes, clusterIds, expandedClusterIds,
                                          incomingExternalEdge, outgoingExternalEdge);
    }
  }
  timer.stop();
  std::cerr << "grew the forest outside clusters. Timer=" << timer << std::endl;
  index.save();
}

#endif
#ifdef NGT_ADVANCED_FOREST
void NGT::Index::buildAdvancedForest(const std::string &indexPath, char mode, size_t seedSize,
                                     bool rebuildTree) {

  if (rebuildTree) {
    std::cerr << "Tree will be deleted and rebuilt with GraphAndTreeIndex::createIndexWithInsertionOrder."
              << std::endl;
  }

  NGT::Property prop;
  prop.load(indexPath);
  prop.indexType = NGT::Index::Property::IndexType::GraphAndTree;

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  NGT::GraphAndTreeIndex graphAndTreeIndex(indexPath, prop, false);
#else
  NGT::GraphAndTreeIndex graphAndTreeIndex(prop);
  graphAndTreeIndex.getObjectSpace().deserialize(indexPath + "/obj");
#endif

  std::cerr << "Creating tree index..." << std::endl;
  std::unordered_map<NGT::ObjectID, NGT::ObjectID> duplicateMap;
  NGT::ObjectRepository &objectRepository = graphAndTreeIndex.getObjectSpace().getRepository();
  for (size_t id = 1; id < objectRepository.size(); id++) {
    if (id % 100000 == 0) {
      std::cerr << " Processed id=" << id << std::endl;
    }
    if (objectRepository.isEmpty(id)) {
      continue;
    }
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    NGT::Object *f = graphAndTreeIndex.getObjectSpace().allocateObject(*objectRepository.get(id));
    NGT::DVPTree::InsertContainer tiobj(*f, id);
#else
    NGT::DVPTree::InsertContainer tiobj(*objectRepository.get(id), id);
#endif
    try {
      NGT::ObjectID existingId = graphAndTreeIndex.DVPTree::insert(tiobj);
      if (existingId != 0) {
        duplicateMap[id] = existingId;
      }
    } catch (NGT::Exception &err) {
      std::cerr << "constructAdvancedForest: Warning. ID=" << id << ":";
      std::cerr << err.what() << " continue.." << std::endl;
    }
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    graphAndTreeIndex.getObjectSpace().deleteObject(f);
#endif
  }
  std::cerr << "Tree index created. # of duplicates=" << duplicateMap.size() << std::endl;

  std::cerr << "Creating insertion order list from leaves..." << std::endl;
  NGT::Index::InsertionOrder insertionOrder;

  if (mode == 'r') {
    std::cerr << "Randomizing insertion order within leaves." << std::endl;
  } else if (mode == 'f') {
    std::cerr << "Randomizing insertion order within leaves (first object fixed)." << std::endl;
  } else if (mode == 'X') {
    std::cerr << "No insertion order for debug." << std::endl;
    NGT::ObjectRepository &objectRepository = graphAndTreeIndex.getObjectSpace().getRepository();
    for (size_t id = 1; id < objectRepository.size(); id++) {
      if (id % 100000 == 0) {
        std::cerr << " Processed id=" << id << std::endl;
      }
      if (objectRepository.isEmpty(id)) {
        continue;
      }
      insertionOrder.push_back(id);
    }
    auto originalSeedType = graphAndTreeIndex.NeighborhoodGraph::property.seedType;
    auto originalSeedSize = graphAndTreeIndex.NeighborhoodGraph::property.seedSize;
    graphAndTreeIndex.NeighborhoodGraph::property.seedType = NGT::NeighborhoodGraph::SeedTypeNone;
    graphAndTreeIndex.NeighborhoodGraph::property.seedSize =
        originalSeedSize == 0 ? NGT_SEED_SIZE : (originalSeedSize < 0 ? -originalSeedSize : originalSeedSize);
    graphAndTreeIndex.createIndexWithInsertionOrder(insertionOrder);
    graphAndTreeIndex.NeighborhoodGraph::property.seedType = originalSeedType;
    graphAndTreeIndex.NeighborhoodGraph::property.seedSize = originalSeedSize;
    graphAndTreeIndex.saveIndex(indexPath);
    return;
  } else if (mode == 's') {
    std::cerr << "Placing seeds from tree at the head of the insertion order (seedSize=" << seedSize << ")."
              << std::endl;
  }

  std::vector<NGT::ObjectID> seedOrder;
  std::unordered_set<NGT::ObjectID> seedIdSet;
  if (mode == 's') {
    for (size_t li = 0; li < graphAndTreeIndex.leafNodes.size(); li++) {
      NGT::LeafNode *leaf = graphAndTreeIndex.leafNodes[li];
      if (leaf == 0 || leaf->getObjectSize() == 0) {
        continue;
      }
      NGT::ObjectDistances seeds;
      graphAndTreeIndex.getSeedsFromTree(leaf->id, seedSize, seeds);
      for (auto &seed : seeds) {
        if (seedIdSet.insert(seed.id).second) {
          seedOrder.push_back(seed.id);
        }
      }
    }
  }

  std::vector<std::vector<NGT::ObjectID>> shuffledIndices(graphAndTreeIndex.leafNodes.size());
  std::mt19937 g;
  if (mode == 'r' || mode == 'f' || mode == 's') {
    std::random_device rd;
    g.seed(rd());
  }

  for (size_t li = 0; li < graphAndTreeIndex.leafNodes.size(); li++) {
    NGT::LeafNode *leaf = graphAndTreeIndex.leafNodes[li];
    if (leaf == 0) {
      continue;
    }
    size_t size = leaf->getObjectSize();
    shuffledIndices[li].reserve(size);
    for (size_t i = 0; i < size; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      NGT::ObjectID id = leaf->getObjectIDs(graphAndTreeIndex.leafNodes.allocator)[i].id;
#else
      NGT::ObjectID id = leaf->getObjectIDs()[i].id;
#endif
      if (mode == 's' && seedIdSet.count(id) > 0) {
        continue;
      }
      shuffledIndices[li].push_back(id);
    }
    if (mode == 'r' && shuffledIndices[li].size() > 1) {
      std::shuffle(shuffledIndices[li].begin(), shuffledIndices[li].end(), g);
    } else if (mode == 'f' && shuffledIndices[li].size() > 2) {
      std::shuffle(shuffledIndices[li].begin() + 1, shuffledIndices[li].end(), g);
    } else if (mode == 's' && shuffledIndices[li].size() > 1) {
      std::shuffle(shuffledIndices[li].begin(), shuffledIndices[li].end(), g);
    }
  }

  if (mode == 's') {
    insertionOrder.insert(insertionOrder.end(), seedOrder.begin(), seedOrder.end());
  }

  size_t idx       = 0;
  size_t scanStart = 0;
  size_t scanEnd   = graphAndTreeIndex.leafNodes.size();
  for (;;) {
    bool added         = false;
    size_t nextScanEnd = 0;
    for (size_t li = scanStart; li < scanEnd; li++) {
      NGT::LeafNode *leaf = graphAndTreeIndex.leafNodes[li];
      if (leaf == 0) {
        continue;
      }
      if (idx >= shuffledIndices[li].size()) {
        continue;
      }
      NGT::ObjectID id = shuffledIndices[li][idx];
      if (!added) {
        scanStart = li;
        added     = true;
      }
      nextScanEnd = li + 1;
      insertionOrder.push_back(id);
    }
    if (!added) {
      break;
    }
    scanEnd = nextScanEnd;
    idx++;
  }
  std::cerr << "Insertion order list created. size=" << insertionOrder.size() << std::endl;

  std::cerr << "Adding duplicated objects..." << std::endl;
  for (const auto &duplicate : duplicateMap) {
    insertionOrder.push_back(duplicate.first);
  }
  std::cerr << "Duplicated objects added. total size=" << insertionOrder.size() << std::endl;

  std::cerr << "Creating graph index with insertion order..." << std::endl;
  {
    auto originalSeedType = graphAndTreeIndex.NeighborhoodGraph::property.seedType;
    auto originalSeedSize = graphAndTreeIndex.NeighborhoodGraph::property.seedSize;
    graphAndTreeIndex.NeighborhoodGraph::property.seedType = NGT::NeighborhoodGraph::SeedTypeNone;
    graphAndTreeIndex.NeighborhoodGraph::property.seedSize =
        originalSeedSize == 0 ? NGT_SEED_SIZE : (originalSeedSize < 0 ? -originalSeedSize : originalSeedSize);
    if (rebuildTree) {
      std::cerr << "Saving the current index to back up the built tree..." << std::endl;
      graphAndTreeIndex.saveIndex(indexPath);
      std::cerr << "Backing up the tree..." << std::endl;
      std::filesystem::copy_file(indexPath + "/tre", indexPath + "/tre.orig",
                                 std::filesystem::copy_options::overwrite_existing);
      std::cerr << "Deleting tree data for rebuild..." << std::endl;
      graphAndTreeIndex.DVPTree::deleteAll();
      graphAndTreeIndex.DVPTree::insertNode(graphAndTreeIndex.DVPTree::allocateLeafNode());
      std::cerr << "Tree data deleted. Rebuilding graph and tree..." << std::endl;
      graphAndTreeIndex.createIndexWithInsertionOrder(insertionOrder);
    } else {
      graphAndTreeIndex.GraphIndex::createIndexWithInsertionOrder(insertionOrder);
    }
    graphAndTreeIndex.NeighborhoodGraph::property.seedType = originalSeedType;
    graphAndTreeIndex.NeighborhoodGraph::property.seedSize = originalSeedSize;
  }
  std::cerr << "Graph index created." << std::endl;

  graphAndTreeIndex.saveIndex(indexPath);

  if (rebuildTree) {
    std::cerr << "Restoring original tree..." << std::endl;
    std::filesystem::copy_file(indexPath + "/tre.orig", indexPath + "/tre",
                               std::filesystem::copy_options::overwrite_existing);
    std::filesystem::remove(indexPath + "/tre.orig");
    std::cerr << "Original tree restored." << std::endl;
  }
}

#endif
