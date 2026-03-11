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
#include "NGT/Common.h"
#include "NGT/Node.h"

#include <string>
#include <vector>
#include <stack>
#include <set>
#include <unordered_map>

namespace NGT {
class Property;

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
template <class TYPE> class TreeNodes : public PersistentRepository<TYPE> {
 public:
  void deserialize(std::ifstream &is, size_t nOfObjects, ObjectSpace *objectspace = 0) {
    if (!is.is_open()) {
      NGTThrowException("NGT::Common: Not open the specified stream yet.");
    }
    PersistentRepository<TYPE>::deleteAll();
    (*this).push_back((TYPE *)0);
    size_t s;
    NGT::Serializer::read(is, s);
    for (size_t i = 0; i < s; i++) {
      char type;
      NGT::Serializer::read(is, type);
      switch (type) {
      case '-': {
        (*this).push_back((TYPE *)0);
#ifdef ADVANCED_USE_REMOVED_LIST
        if (i != 0) {
          PersistentRepository<TYPE>::removedListPush(i);
        }
#endif
      } break;
      case '+': {
        if (objectspace == 0) {
          TYPE *v = new (PersistentRepository<TYPE>::allocator)
              TYPE(nOfObjects, PersistentRepository<TYPE>::allocator);
          assert(0);
          (*this).push_back(v);
        } else {
          TYPE *v = new (PersistentRepository<TYPE>::allocator)
              TYPE(nOfObjects, PersistentRepository<TYPE>::allocator, objectspace);
          assert(0);
          (*this).push_back(v);
        }
      } break;
      default: {
        assert(type == '-' || type == '+');
        break;
      }
      }
    }
  }
  void deserializeAsText(std::ifstream &is, size_t nOfObjects, ObjectSpace *objectspace = 0) {
    if (!is.is_open()) {
      NGTThrowException("NGT::Common: Not open the specified stream yet.");
    }
    PersistentRepository<TYPE>::deleteAll();
    size_t s;
    NGT::Serializer::readAsText(is, s);
    (*this).reserve(s);
    for (size_t i = 0; i < s; i++) {
      size_t idx;
      NGT::Serializer::readAsText(is, idx);
      if (i != idx) {
        std::cerr << "PersistentRepository: Error. index of a specified import file is invalid. " << idx
                  << ":" << i << std::endl;
      }
      char type;
      NGT::Serializer::readAsText(is, type);
      switch (type) {
      case '-': {
        (*this).push_back((TYPE *)0);
#ifdef ADVANCED_USE_REMOVED_LIST
        if (i != 0) {
          PersistentRepository<TYPE>::removedListPush(i);
        }
#endif
      } break;
      case '+': {
        if (objectspace == 0) {
          TYPE *v = new (PersistentRepository<TYPE>::allocator)
              TYPE(nOfObjects, PersistentRepository<TYPE>::allocator);
          v->deserializeAsText(is, PersistentRepository<TYPE>::allocator);
          (*this).push_back(v);
        } else {
          TYPE *v = new (PersistentRepository<TYPE>::allocator)
              TYPE(nOfObjects, PersistentRepository<TYPE>::allocator, objectspace);
          v->deserializeAsText(is, PersistentRepository<TYPE>::allocator, objectspace);
          (*this).push_back(v);
        }
      } break;
      default: {
        assert(type == '-' || type == '+');
        break;
      }
      }
    }
  }
};
#else
template <class TYPE> class TreeNodes : public Repository<TYPE> {
 public:
  void deserialize(std::ifstream &is, size_t nOfObjects, ObjectSpace *objectspace = 0) {
    if (!is.is_open()) {
      NGTThrowException("NGT::Common: Not open the specified stream yet.");
    }
    Repository<TYPE>::deleteAll();
    size_t s;
    NGT::Serializer::read(is, s);
    std::vector<TYPE *>::reserve(s);
    for (size_t i = 0; i < s; i++) {
      char type;
      NGT::Serializer::read(is, type);
      switch (type) {
      case '-': {
        std::vector<TYPE *>::push_back(0);
#ifdef ADVANCED_USE_REMOVED_LIST
        if (i != 0) {
          Repository<TYPE>::removedList.push(i);
        }
#endif
      } break;
      case '+': {
        if (objectspace == 0) {
          TYPE *v = new TYPE(nOfObjects);
          v->deserialize(is);
          std::vector<TYPE *>::push_back(v);
        } else {
          TYPE *v = new TYPE(nOfObjects, objectspace);
          v->deserialize(is, objectspace);
          std::vector<TYPE *>::push_back(v);
        }
      } break;
      default: {
        assert(type == '-' || type == '+');
        break;
      }
      }
    }
  }
  void deserializeAsText(std::ifstream &is, size_t nOfObjects, ObjectSpace *objectspace = 0) {
    if (!is.is_open()) {
      NGTThrowException("NGT::Common: Not open the specified stream yet.");
    }
    Repository<TYPE>::deleteAll();
    size_t s;
    NGT::Serializer::readAsText(is, s);
    std::vector<TYPE *>::reserve(s);
    for (size_t i = 0; i < s; i++) {
      size_t idx;
      NGT::Serializer::readAsText(is, idx);
      if (i != idx) {
        std::cerr << "Repository: Error. index of a specified import file is invalid. " << idx << ":" << i
                  << std::endl;
      }
      char type;
      NGT::Serializer::readAsText(is, type);
      switch (type) {
      case '-': {
        std::vector<TYPE *>::push_back(0);
#ifdef ADVANCED_USE_REMOVED_LIST
        if (i != 0) {
          Repository<TYPE>::removedList.push(i);
        }
#endif
      } break;
      case '+': {
        if (objectspace == 0) {
          TYPE *v = new TYPE(nOfObjects);
          v->deserializeAsText(is);
          std::vector<TYPE *>::push_back(v);
        } else {
          TYPE *v = new TYPE(nOfObjects, objectspace);
          v->deserializeAsText(is, objectspace);
          std::vector<TYPE *>::push_back(v);
        }
      } break;
      default: {
        assert(type == '-' || type == '+');
        break;
      }
      }
    }
  }
};
#endif

class DVPTree {

 public:
  enum SplitMode { MaxDistance = 0, MaxVariance = 1 };

  typedef std::vector<Node::ID> IDVector;

  class Container : public NGT::Container {
   public:
    Container(Object &f, ObjectID i) : NGT::Container(f, i) {}
    DVPTree *vptree;
  };

  class SearchContainer : public NGT::SearchContainer {
   public:
    enum Mode { SearchLeaf = 0, SearchObject = 1 };

    SearchContainer(Object &f, ObjectID i) : NGT::SearchContainer(f, i) {}
    SearchContainer(Object &f) : NGT::SearchContainer(f, 0) {}

    DVPTree *vptree;

    Mode mode;
    Node::ID nodeID;
  };
  class InsertContainer : public Container {
   public:
    InsertContainer(Object &f, ObjectID i) : Container(f, i) {}
  };

  class RemoveContainer : public Container {
   public:
    RemoveContainer(Object &f, ObjectID i) : Container(f, i) {}
  };

  DVPTree();
  DVPTree(Property &prop);

  virtual ~DVPTree() {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
    deleteAll();
#endif
  }

  void initialize(Property &prop);

  void deleteAll() {
    for (size_t i = 0; i < leafNodes.size(); i++) {
      if (leafNodes[i] != 0) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        leafNodes[i]->deletePivot(*objectSpace, leafNodes.allocator);
#else
        leafNodes[i]->deletePivot(*objectSpace);
#endif
        delete leafNodes[i];
      }
    }
    leafNodes.clear();
    for (size_t i = 0; i < internalNodes.size(); i++) {
      if (internalNodes[i] != 0) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        internalNodes[i]->deletePivot(*objectSpace, internalNodes.allocator);
#else
        internalNodes[i]->deletePivot(*objectSpace);
#endif
        delete internalNodes[i];
      }
    }
    internalNodes.clear();
  }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  void open(const std::string &f, size_t sharedMemorySize) {
    // If no file, then create a new file.
    leafNodes.open(f + "l", sharedMemorySize);
    internalNodes.open(f + "i", sharedMemorySize);
    if (leafNodes.size() == 0) {
      if (internalNodes.size() != 0) {
        NGTThrowException("Tree::Open: Internal error. Internal and leaf are inconsistent.");
      }
      insertNode(allocateLeafNode());
    }
  }
#endif // NGT_SHARED_MEMORY_ALLOCATOR

  void insert(InsertContainer &iobj);

  void insert(InsertContainer &iobj, LeafNode *n);

  Node::ID split(InsertContainer &iobj, LeafNode &leaf);

  Node::ID recombineNodes(InsertContainer &ic, Node::Objects &fs, LeafNode &leaf);

  void insertObject(InsertContainer &obj, LeafNode &leaf);

  typedef std::stack<Node::ID> UncheckedNode;

  void search(SearchContainer &so);
  void search(SearchContainer &so, InternalNode &node, UncheckedNode &uncheckedNode);
  void search(SearchContainer &so, LeafNode &node, UncheckedNode &uncheckedNode);

  bool searchObject(ObjectID id) {
    LeafNode &ln = getLeaf(id);
    for (size_t i = 0; i < ln.getObjectSize(); i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      if (ln.getObjectIDs(leafNodes.allocator)[i].id == id) {
#else
      if (ln.getObjectIDs()[i].id == id) {
#endif
        return true;
      }
    }
    return false;
  }

  LeafNode &getLeaf(ObjectID id) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    Object *qobject = objectSpace->allocateObject(*getObjectRepository().get(id));
    SearchContainer q(*qobject);
#else
    SearchContainer q(*getObjectRepository().get(id));
#endif
    q.mode   = SearchContainer::SearchLeaf;
    q.vptree = this;
    q.radius = 0.0;
    q.size   = 1;

    search(q);

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    objectSpace->deleteObject(qobject);
#endif

    return *(LeafNode *)getNode(q.nodeID);
  }

  void replace(ObjectID id, ObjectID replacedId) { remove(id, replacedId); }

  // remove the specified object.
  void remove(ObjectID id, ObjectID replaceId = 0) {
    LeafNode &ln = getLeaf(id);
    try {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      ln.removeObject(id, replaceId, leafNodes.allocator);
#else
      ln.removeObject(id, replaceId);
#endif
    } catch (Exception &err) {
      std::stringstream msg;
      msg << "VpTree::remove: Inner error. Cannot remove object. leafNode=" << ln.id.getID() << ":"
          << err.what();
      NGTThrowException(msg);
    }
    if (ln.getObjectSize() == 0) {
      if (ln.parent.getID() != 0) {
        InternalNode &inode = *(InternalNode *)getNode(ln.parent);
        removeEmptyNodes(inode);
      }
    }

    return;
  }

  void removeNaively(ObjectID id, ObjectID replaceId = 0) {
    for (size_t i = 0; i < leafNodes.size(); i++) {
      if (leafNodes[i] != 0) {
        try {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          leafNodes[i]->removeObject(id, replaceId, leafNodes.allocator);
#else
          leafNodes[i]->removeObject(id, replaceId);
#endif
          break;
        } catch (...) {
        }
      }
    }
  }

  Node *getRootNode() {
    if (internalNodes.size() == 0 && leafNodes.size() == 0) {
      NGTThrowException("DVPTree::getRootNode: Inner error. No leaf root node.");
    }
    size_t nid = 1;
    Node *root;
    try {
      root = internalNodes.get(nid);
    } catch (Exception &err) {
      try {
        root = leafNodes.get(nid);
      } catch (Exception &e) {
        std::stringstream msg;
        msg << "DVPTree::getRootNode: Inner error. Cannot get a leaf root node. " << nid << ":" << e.what();
        NGTThrowException(msg);
      }
    }

    return root;
  }

  InternalNode *createInternalNode() {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    InternalNode *n =
        new (internalNodes.allocator) InternalNode(internalChildrenSize, internalNodes.allocator);
#else
    InternalNode *n = new InternalNode(internalChildrenSize);
#endif
    insertNode(n);
    return n;
  }

  void removeNode(Node::ID id) {
    size_t idx = id.getID();
    if (id.getType() == Node::ID::Leaf) {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
      LeafNode &n = *static_cast<LeafNode *>(getNode(id));
      delete n.pivot;
#endif
      leafNodes.remove(idx);
    } else {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
      InternalNode &n = *static_cast<InternalNode *>(getNode(id));
      delete n.pivot;
#endif
      internalNodes.remove(idx);
    }
  }

  void removeEmptyNodes(InternalNode &node);

  Node::Objects *getObjects(LeafNode &n, Container &iobj);

  void getObjectIDsFromLeaf(Node::ID nid, ObjectDistances &rl) {
    LeafNode &ln = *(LeafNode *)getNode(nid);
    rl.clear();
    ObjectDistance r;
    for (size_t i = 0; i < ln.getObjectSize(); i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      r.id       = ln.getObjectIDs(leafNodes.allocator)[i].id;
      r.distance = ln.getObjectIDs(leafNodes.allocator)[i].distance;
#else
      r.id       = ln.getObjectIDs()[i].id;
      r.distance = ln.getObjectIDs()[i].distance;
#endif
      rl.push_back(r);
    }
    return;
  }
  void insertNode(LeafNode *n) {
    size_t id = leafNodes.insert(n);
    n->id.setID(id);
    n->id.setType(Node::ID::Leaf);
  }

  // replace
  void replaceNode(LeafNode *n) {
    int id = n->id.getID();
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    leafNodes.set(id, n);
#else
    leafNodes[id] = n;
#endif
  }

  void insertNode(InternalNode *n) {
    size_t id = internalNodes.insert(n);
    n->id.setID(id);
    n->id.setType(Node::ID::Internal);
  }

  Node *getNode(Node::ID &id) {
    Node *n          = 0;
    Node::NodeID idx = id.getID();
    if (id.getType() == Node::ID::Leaf) {
      n = leafNodes.get(idx);
    } else {
      n = internalNodes.get(idx);
    }
    return n;
  }

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  LeafNode *allocateLeafNode(NGT::ObjectSpace *os = 0) {
    return new (leafNodes.allocator) LeafNode(leafObjectsSize, leafNodes.allocator);
  }
#else
  LeafNode *allocateLeafNode(NGT::ObjectSpace *os = 0) { return new LeafNode(leafObjectsSize, os); }
#endif

  void getAllLeafNodeIDs(std::vector<Node::ID> &leafIDs) {
    leafIDs.clear();
    Node *root = getRootNode();
    if (root->id.getType() == Node::ID::Leaf) {
      leafIDs.push_back(root->id);
      return;
    }
    UncheckedNode uncheckedNode;
    uncheckedNode.push(root->id);
    while (!uncheckedNode.empty()) {
      Node::ID nodeid = uncheckedNode.top();
      uncheckedNode.pop();
      Node *cnode = getNode(nodeid);
      if (cnode->id.getType() == Node::ID::Internal) {
        InternalNode &inode = static_cast<InternalNode &>(*cnode);
        for (size_t ci = 0; ci < internalChildrenSize; ci++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
          uncheckedNode.push(inode.getChildren(internalNodes.allocator)[ci]);
#else
          uncheckedNode.push(inode.getChildren()[ci]);
#endif
        }
      } else if (cnode->id.getType() == Node::ID::Leaf) {
        leafIDs.push_back(static_cast<LeafNode &>(*cnode).id);
      } else {
        std::cerr << "Tree: Inner fatal error!: Node type error!" << std::endl;
        abort();
      }
    }
  }

  void serialize(std::ofstream &os) {
    leafNodes.serialize(os, objectSpace);
    internalNodes.serialize(os, objectSpace);
  }

  void deserialize(std::ifstream &is) {
    leafNodes.deserialize(is, leafObjectsSize, objectSpace);
    internalNodes.deserialize(is, objectSpace);
  }

  void serializeAsText(std::ofstream &os) {
    leafNodes.serializeAsText(os, objectSpace);
    internalNodes.serializeAsText(os, objectSpace);
  }

  void deserializeAsText(std::ifstream &is) {
    leafNodes.deserializeAsText(is, leafObjectsSize, objectSpace);
    internalNodes.deserializeAsText(is, objectSpace);
  }

  void show() {
    std::cout << "Show tree " << std::endl;
    for (size_t i = 0; i < leafNodes.size(); i++) {
      if (leafNodes[i] != 0) {
        std::cout << i << ":";
        (*leafNodes[i]).show();
      }
    }
    for (size_t i = 0; i < internalNodes.size(); i++) {
      if (internalNodes[i] != 0) {
        std::cout << i << ":";
        (*internalNodes[i]).show();
      }
    }
  }

  bool verify(size_t objCount, std::vector<uint8_t> &status) {
    std::cerr << "Started verifying internal nodes. size=" << internalNodes.size() << "..." << std::endl;
    bool valid = true;
    for (size_t i = 0; i < internalNodes.size(); i++) {
      if (internalNodes[i] != 0) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        valid = valid && (*internalNodes[i]).verify(internalNodes, leafNodes, internalNodes.allocator);
#else
        valid = valid && (*internalNodes[i]).verify(internalNodes, leafNodes);
#endif
      }
    }
    std::cerr << "Started verifying leaf nodes. size=" << leafNodes.size() << " ..." << std::endl;
    for (size_t i = 0; i < leafNodes.size(); i++) {
      if (leafNodes[i] != 0) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
        valid = valid && (*leafNodes[i]).verify(objCount, status, leafNodes.allocator);
#else
        valid = valid && (*leafNodes[i]).verify(objCount, status);
#endif
      }
    }
    return valid;
  }

  void deleteInMemory() {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    assert(0);
#else
    for (std::vector<NGT::LeafNode *>::iterator i = leafNodes.begin(); i != leafNodes.end(); i++) {
      if ((*i) != 0) {
        delete (*i);
      }
    }
    leafNodes.clear();
    for (std::vector<NGT::InternalNode *>::iterator i = internalNodes.begin(); i != internalNodes.end();
         i++) {
      if ((*i) != 0) {
        delete (*i);
      }
    }
    internalNodes.clear();
#endif
  }

  ObjectRepository &getObjectRepository() { return objectSpace->getRepository(); }

  size_t getSharedMemorySize(std::ostream &os, SharedMemoryAllocator::GetMemorySizeType t) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    size_t isize = internalNodes.getAllocator().getMemorySize(t);
    os << "internal=" << isize << std::endl;
    size_t lsize = leafNodes.getAllocator().getMemorySize(t);
    os << "leaf=" << lsize << std::endl;
    return isize + lsize;
#else
    return 0;
#endif
  }

  void getAllObjectIDs(std::set<ObjectID> &ids, bool duplicationCheck = false) {
    std::unordered_map<ObjectID, size_t> duplicatedIDs;
    for (size_t i = 0; i < leafNodes.size(); i++) {
      if (leafNodes[i] != 0) {
        LeafNode &ln = *leafNodes[i];
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        auto objs = ln.getObjectIDs(leafNodes.allocator);
#else
        auto objs = ln.getObjectIDs();
#endif
        for (size_t idx = 0; idx < ln.objectSize; ++idx) {
          auto ret = ids.insert(objs[idx].id);
          if (ret.second == false) {
            duplicatedIDs[objs[idx].id]++;
          }
        }
      }
    }
    for (const auto &id : duplicatedIDs) {
      std::cerr << "Warning! Duplicated ID in the tree. ID=" << id.first << " X " << id.second << std::endl;
    }
    if (!duplicationCheck || duplicatedIDs.empty()) {
      return;
    }
    for (size_t i = 0; i < leafNodes.size(); i++) {
      if (leafNodes[i] != 0) {
        LeafNode &ln = *leafNodes[i];
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        auto objs = ln.getObjectIDs(leafNodes.allocator);
#else
        auto objs = ln.getObjectIDs();
#endif
        for (size_t idx = 0; idx < ln.objectSize; ++idx) {
          if (duplicatedIDs.count(objs[idx].id) != 0) {
            std::cerr << "Duplicated ID=" << objs[idx].id << ", leaf ID=" << i
                      << ", index in the leaf=" << idx << std::endl;
          }
        }
      }
    }
  }

 public:
  size_t internalChildrenSize;
  size_t leafObjectsSize;

  SplitMode splitMode;

  std::string name;

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  TreeNodes<LeafNode> leafNodes;
  PersistentRepository<InternalNode> internalNodes;
#else
  TreeNodes<LeafNode> leafNodes;
  Repository<InternalNode> internalNodes;
#endif

  ObjectSpace *objectSpace;
};
} // namespace NGT
