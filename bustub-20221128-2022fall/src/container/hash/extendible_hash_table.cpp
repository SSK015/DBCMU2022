//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Find the value associated with the given key.
   *
   * Use IndexOf(key) to find the directory index the key hashes to.
   *
   * @param key The key to be searched.
   * @param[out] value The value associated with the key.
   * @return True if the key is found, false otherwise.
   */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
    auto index = IndexOf(key);
    auto target_bucket = dir_[index];
    if (target_bucket->Find(key, value)) {
      return true;
    } else {
      return false;
    }
//  UNREACHABLE("not implemented");
}
 /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Given the key, remove the corresponding key-value pair in the bucket.
     * @param key The key to be deleted.
     * @return True if the key exists, false otherwise.
     */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
    auto index = IndexOf(key);
    auto target_bucket = dir_[index];
    if (target_bucket->Remove(key)) {
      return true;
    } else {
      return false;
    }
//  UNREACHABLE("not implemented");
}
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Insert the given key-value pair into the hash table.
   * If a key already exists, the value should be updated.
   * If the bucket is full and can't be inserted, do the following steps before retrying:
   *    1. If the local depth of the bucket is equal to the global depth,
   *        increment the global depth and double the size of the directory.
   *    2. Increment the local depth of the bucket.
   *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
   *
   * @param key The key to be inserted.
   * @param value The value to be inserted.
   */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
    
    while (dir_[IndexOf(key)]->IsFull()) {
      auto index = IndexOf(key);
      auto target_bucket = dir_[index];
      if (target_bucket->GetDepth() == GetGlobalDepthInternal()) {
        global_depth_++;
        int length = dir_.size();
        dir_.resize(length << 1);
        for (int i = 0; i < length; ++i) {
          dir_[i + length] = dir_[i];
        }
// why can not I commit?       
      }
      auto mask = 1 << target_bucket->GetDepth();
      auto bucket_0 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
      auto bucket_1 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
      num_buckets_++;
      for (const auto &item : target_bucket->GetItems()) {
        auto hash_key = std::hash<K>()(item.first);
        if ((hash_key & mask) == 0) {
          bucket_0->Insert(item.first, item.second);

        } else {
          bucket_1->Insert(item.first, item.second);
        }
      }

      for (size_t i = 0; i < dir_.size(); ++i) {
        if (dir_[i] == target_bucket) {
          if ((i & mask) == 0) {
            dir_[i] = bucket_0;
          } else {
            dir_[i] = bucket_1;
          }
        }
      }
    }
    auto index = IndexOf(key);
    auto target_bucket = dir_[index];
    
    target_bucket->Insert(key, value);
/*  if (key is existed) {
    old_value = value;
  } else {
    while (bucket_size_ == ) {
      if (GetLocalDepth() == ) {

      }
      Incre the local depth;


  //3.
      split the bucket;
      redistribute;
   }
  }*/
//  UNREACHABLE("not implemented");
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
    
  auto iter = std::find_if(list_.begin(), list_.end(), [&key, &value](const auto &item) {
        return item.first == key;
    });
    if (iter != list_.end()) {
        return true;
        //std::cout << "The second element of the pair is: " << iter->second << std::endl;
    } else {
        return false;  
//      std::cout << "Element not found." << std::endl;
    }
  //UNREACHABLE("not implemented");
}
/**
     *
     * TODO(P1): Add implementation
     *
     * @brief Given the key, remove the corresponding key-value pair in the bucket.
     * @param key The key to be deleted.
     * @return True if the key exists, false otherwise.
     */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  V value;
  if (Find(key, value)) {
    auto iter = std::find_if(list_.begin(), list_.end(), [&key, &value](const auto& p) {
      return p.first == key;
    });
    list_.erase(iter);
    return true;
  }
  return false; 
  //UNREACHABLE("not implemented");
}
/**
     *
     * TODO(P1): Add implementation
     *
     * @brief Insert the given key-value pair into the bucket.
     *      1. If a key already exists, the value should be updated.
     *      2. If the bucket is full, do nothing and return false.
     * @param key The key to be inserted.
     * @param value The value to be inserted.
     * @return True if the key-value pair is inserted, false otherwise.
     */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  //auto new_value = value;
//  if (Find(key, value)) {
    //Remove(key);
//    list_.emplace_back(key, value);
    //value = new_value;
//  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;

  //UNREACHABLE("not implemented");
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
