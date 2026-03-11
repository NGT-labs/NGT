//
// Copyright (C) 2020 Yahoo Japan Corporation
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

#include "NGT/NGTQ/QuantizedGraph.h"
#include "NGT/NGTQ/QuantizedBlobGraph.h"
#include "NGT/NGTQ/Optimizer.h"

#ifdef NGTQG_X86SIMDSORT
#warning "*** X86simdsort is enabled for QG. ***"
#endif

#ifdef NGTQ_QBG
void NGTQG::QuantizedGraphRepository::construct(NGT::GraphRepository &graphRepository,
                                                NGTQ::Index &quantizedIndex, size_t maxNoOfEdges) {
  NGTQ::InvertedIndexEntry<uint16_t> invertedIndexObjects(numOfSubspaces);
  quantizedIndex.getQuantizer().extractInvertedIndexObject(invertedIndexObjects);
  std::cerr << "inverted index object size=" << invertedIndexObjects.size() << std::endl;
  std::cerr << "  vmsize==" << NGT::Common::getProcessVmSizeStr() << std::endl;
  std::cerr << "  peak vmsize==" << NGT::Common::getProcessVmPeakStr() << std::endl;
  quantizedIndex.getQuantizer().eraseInvertedIndexObject();
  if (graphRepository.size() != invertedIndexObjects.size()) {
    std::cerr << "QuantizedGraph::construct Warning! The sizes of the graphRepository and the "
                 "invertedIndexObjects are not identical."
              << graphRepository.size() << ":" << invertedIndexObjects.size() << std::endl;
  }

  PARENT::resize(graphRepository.size());

  for (size_t id = 1; id < graphRepository.size(); id++) {
    if ((graphRepository.size() > 100) && ((id % ((graphRepository.size() - 1) / 100)) == 0)) {
      std::cerr << "# of processed objects=" << id << "/" << (graphRepository.size() - 1) << "("
                << id * 100 / (graphRepository.size() - 1) << "%)" << std::endl;
    }
    if (graphRepository.isEmpty(id)) {
      continue;
    }
    if (id >= (*this).size()) {
      std::stringstream msg;
      msg << "Fatal inner error! ID exceeds the size of the inverted index. " << id << " " << (*this).size();
      NGTThrowException(msg);
    }
    NGT::GraphNode &node = *graphRepository.VECTOR::get(id);
    size_t numOfEdges    = node.size() < maxNoOfEdges ? node.size() : maxNoOfEdges;
    std::vector<uint32_t> ids;
    ids.reserve(numOfEdges);
    (*this)[id].nOfObjects = numOfEdges;
    NGTQ::QuantizedObjectProcessingStream quantizedStream(quantizedIndex.getQuantizer().divisionNo,
                                                          numOfEdges);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    for (auto i = node.begin(graphRepository.allocator); i != node.end(graphRepository.allocator); ++i) {
      if (distance(node.begin(graphRepository.allocator), i) >= static_cast<int64_t>(numOfEdges)) {
#else
    for (auto i = node.begin(); i != node.end(); i++) {
      if (distance(node.begin(), i) >= static_cast<int64_t>(numOfEdges)) {
#endif
        break;
      }
      if ((*i).id == 0) {
        std::cerr << "something strange" << std::endl;
        abort();
        continue;
      }
      ids.emplace_back((*i).id);
      for (size_t idx = 0; idx < numOfSubspaces; idx++) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
#else
        size_t dataNo = distance(node.begin(), i);
#endif
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        abort();
#else
        if (invertedIndexObjects.size() <= (*i).id) {
          std::stringstream msg;
          msg << "Fatal inner error! Invalid inverted index ID. ID=" << (*i).id << "/"
              << invertedIndexObjects.size();
          NGTThrowException(msg);
        }
        if (invertedIndexObjects[(*i).id].localID[idx] < 1 ||
            invertedIndexObjects[(*i).id].localID[idx] > 16) {
          std::stringstream msg;
          msg << "Fatal inner error! Invalid local centroid ID. ID=" << (*i).id << ":"
              << invertedIndexObjects[(*i).id].localID[idx];
          NGTThrowException(msg);
        }
        quantizedStream.arrangeQuantizedObject(dataNo, idx, invertedIndexObjects[(*i).id].localID[idx] - 1);
#endif
      }
    }

    size_t streamSize = quantizedStream.getUint4StreamSize();
    if (streamSize != quantizedStream.getUint4StreamSize(numOfEdges)) {
      std::stringstream msg;
      msg << "Inner fatal error! Stream size is wrong. " << streamSize << ":"
          << quantizedStream.getUint4StreamSize(numOfEdges);
      NGTThrowException(msg);
    }
    size_t idsSize      = sizeof(uint32_t) * numOfEdges;
    (*this)[id].objects = reinterpret_cast<uint8_t *>(NGT::alignedAlloc64(streamSize + idsSize));
    (*this)[id].ids = reinterpret_cast<uint32_t *>(static_cast<uint8_t *>((*this)[id].objects) + streamSize);
    auto *qobjs     = quantizedStream.compressIntoUint4();
    memcpy((*this)[id].objects, qobjs, quantizedStream.getUint4StreamSize());
    NGT::alignedFree64(qobjs);
    memcpy((*this)[id].ids, ids.data(), sizeof(uint32_t) * ids.size());
  }
}

void NGTQG::QuantizedGraphRepository::serialize(std::ofstream &os, NGT::ObjectSpace *objspace) {
#ifndef NGT_IVI
  NGTQ::QuantizedObjectProcessingStream quantizedObjectProcessingStream(numOfSubspaces);
#endif
  uint64_t n = numOfSubspaces;
  NGT::Serializer::write(os, n);
  n = PARENT::size();
  NGT::Serializer::write(os, n);
  for (auto i = PARENT::begin(); i != PARENT::end(); ++i) {
    uint32_t sid = (*i).subspaceID;
    NGT::Serializer::write(os, sid);

    unsigned int nobjs = (*i).nOfObjects;
    NGT::Serializer::write(os, nobjs);
    NGT::Serializer::write(os, reinterpret_cast<uint8_t *>((*i).ids), nobjs * sizeof(uint32_t));
#ifdef NGTQ_OBGRAPH
    if (graphType == NGTQ::GraphTypeObjectBlobGraph) {
      NGT::Serializer::write(os, (*i).blobIDs);
    }
#endif
#ifdef NGT_IVI
    size_t streamSize = quantizer.getQuantizedObjectDistance().getSizeOfCluster((*i).ids.size());
#else
    size_t streamSize = quantizedObjectProcessingStream.getUint4StreamSize((*i).nOfObjects);
#endif
    NGT::Serializer::write(os, static_cast<uint8_t *>((*i).objects), streamSize);
  }
}

void NGTQG::QuantizedGraphRepository::deserialize(std::ifstream &is, NGT::ObjectSpace *objectspace) {
  try {
#ifdef NGT_IVI
#else
    NGTQ::QuantizedObjectProcessingStream quantizedObjectProcessingStream(numOfSubspaces);
#endif
    uint64_t n;
    NGT::Serializer::read(is, n);
    numOfSubspaces = n;
    NGT::Serializer::read(is, n);
    PARENT::resize(n);
    for (auto i = PARENT::begin(); i != PARENT::end(); ++i) {
      uint32_t sid;
      NGT::Serializer::read(is, sid);
      (*i).subspaceID = sid;
      unsigned int nobjs;
      NGT::Serializer::read(is, nobjs);
#ifdef NGT_IVI
      size_t streamSize = quantizer.getQuantizedObjectDistance().getSizeOfCluster(nobjs);
#else
      size_t streamSize = quantizedObjectProcessingStream.getUint4StreamSize(nobjs);
#endif
      size_t idsSize = sizeof(uint32_t) * nobjs;
#ifdef NGTQG_RESIZED_NODE
      (*i).nOfObjects = nobjs > 32 ? (nobjs & ~(16 - 1)) : nobjs;
      uint8_t objects[streamSize];
      uint32_t ids[nobjs];
      NGT::Serializer::read(is, reinterpret_cast<uint8_t *>(ids), idsSize);
#ifdef NGTQ_OBGRAPH
      if (graphType == NGTQ::GraphTypeObjectBlobGraph) {
        NGT::Serializer::read(is, (*i).blobIDs);
      }
#endif
      NGT::Serializer::read(is, objects, streamSize);
#ifdef NGT_IVI
      size_t packedStreamSize = quantizer.getQuantizedObjectDistance().getSizeOfCluster((*i).nOfObjects);
#else
      size_t packedStreamSize = quantizedObjectProcessingStream.getUint4StreamSize((*i).nOfObjects);
#endif
      size_t packedIdsSize = sizeof(uint32_t) * (*i).nOfObjects;
      (*i).objects = reinterpret_cast<uint8_t *>(NGT::alignedAlloc64(packedStreamSize + packedIdsSize));
      (*i).ids     = reinterpret_cast<uint32_t *>(static_cast<uint8_t *>((*i).objects) + packedStreamSize);
      memcpy((*i).objects, &objects[0], packedStreamSize);
      memcpy(reinterpret_cast<uint8_t *>((*i).ids), &ids[0], packedIdsSize);
#else
      (*i).nOfObjects = nobjs;
      (*i).objects    = reinterpret_cast<uint8_t *>(NGT::alignedAlloc64(streamSize + idsSize));
      (*i).ids        = reinterpret_cast<uint32_t *>(static_cast<uint8_t *>((*i).objects) + streamSize);
      NGT::Serializer::read(is, reinterpret_cast<uint8_t *>((*i).ids), idsSize);
#ifdef NGTQ_OBGRAPH
      if (graphType == NGTQ::GraphTypeObjectBlobGraph) {
        NGT::Serializer::read(is, (*i).blobIDs);
      }
#endif
      NGT::Serializer::read(is, static_cast<uint8_t *>((*i).objects), streamSize);
#endif
    }
  } catch (NGT::Exception &err) {
    std::stringstream msg;
    msg << "QuantizedGraph::deserialize: Fatal error. " << err.what();
    NGTThrowException(msg);
  }
}

void NGTQG::Index::quantize(const std::string indexPath, size_t dimensionOfSubvector, size_t maxNumOfEdges,
                            bool verbose) {
  {
    NGT::Index index(indexPath);
    const std::string quantizedIndexPath = indexPath + "/qg";
    struct stat st;
    if (stat(quantizedIndexPath.c_str(), &st) != 0) {
      NGT::Property ngtProperty;
      index.getProperty(ngtProperty);
      QBG::BuildParameters buildParameters;
      buildParameters.creation.dimensionOfSubvector = dimensionOfSubvector;
      buildParameters.setVerbose(verbose);

      NGTQG::Index::create(indexPath, buildParameters);

      NGTQG::Index::append(indexPath, buildParameters);

      QBG::Optimizer optimizer(buildParameters);
#ifdef NGTQG_NO_ROTATION
      if (optimizer.rotation || optimizer.repositioning) {
        std::cerr << "build-qg: Warning! Although rotation or repositioning is specified, turn off rotation "
                     "and repositioning because of unavailable options."
                  << std::endl;
        optimizer.rotation      = false;
        optimizer.repositioning = false;
      }
#endif

      if (optimizer.globalType == QBG::Optimizer::GlobalTypeNone) {
        if (verbose)
          std::cerr
              << "build-qg: Warning! None is unavailable for the global type. Zero is set to the global type."
              << std::endl;
        optimizer.globalType = QBG::Optimizer::GlobalTypeZero;
      }

      optimizer.optimize(quantizedIndexPath);

      QBG::Index::buildNGTQ(quantizedIndexPath, verbose);

      NGTQG::Index::realign(indexPath, maxNumOfEdges, verbose);
    }
  }
}

void NGTQG::Index::create(const std::string indexPath, QBG::BuildParameters &buildParameters) {
  auto dimensionOfSubvector = buildParameters.creation.dimensionOfSubvector;
  auto dimension            = buildParameters.creation.dimension;
  if (dimension != 0 && buildParameters.creation.numOfSubvectors != 0) {
    if (dimension % buildParameters.creation.numOfSubvectors != 0) {
      std::stringstream msg;
      msg << "NGTQBG:Index::create: Invalid dimension and local division No. " << dimension << ":"
          << buildParameters.creation.numOfSubvectors;
      NGTThrowException(msg);
    }
    dimensionOfSubvector = dimension / buildParameters.creation.numOfSubvectors;
  }
  create(indexPath, dimensionOfSubvector, dimension);
  std::string quantizedIndexPath = indexPath + "/" + getQGDirectoryName();
  NGTQ::Index quantizedIndex(quantizedIndexPath);
  quantizedIndex.getQuantizer().property.orderedSearchRanking = buildParameters.creation.orderedSearchRanking;
  quantizedIndex.save();
}

void NGTQG::Index::append(const std::string indexPath, QBG::BuildParameters &buildParameters) {
  QBG::Index::appendFromObjectRepository(indexPath, indexPath + "/qg", buildParameters.verbose);
}
#endif
