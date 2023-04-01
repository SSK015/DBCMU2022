//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
/**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    std::scoped_lock<std::mutex> lock(latch_);
    if (curr_size_ == 0) return false;

    for (auto iter = outer_list_.rbegin(); iter != outer_list_.rend(); iter++) {
        auto frame = *iter;
        if (Evictable_[frame]) {
            access_record_[frame] = 0;
            outer_list_.erase(outer_index_[frame]);
            outer_index_.erase(frame);
            *frame_id = frame;
            curr_size_--;
            Evictable_[frame] = false;
            return true;
        }
    }

    for (auto iter = pool_cache_list_.rbegin(); iter != pool_cache_list_.rend(); iter++) {
        auto frame = *iter;

        if (Evictable_[frame]) {
            access_record_[frame] = 0;
            pool_cache_list_.erase(pool_cache_index_[frame]);
            pool_cache_index_.erase(frame);
            *frame_id = frame;
            curr_size_--;
            Evictable_[frame] = false;
            return true;
        }
    }

    return false;
}
  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);

    if (frame_id > static_cast<int>(replacer_size_)) {
        throw std::exception();
    }
    access_record_[frame_id]++;

    if (access_record_[frame_id] < k_) {
        //if (access_record_[frame_id] == 0) {
        if (outer_index_.count(frame_id) == 0U) {
            outer_list_.push_front(frame_id);
            outer_index_[frame_id] = outer_list_.begin();
        }
    } else if (access_record_[frame_id] == k_) {
        auto iter = outer_index_[frame_id];
        outer_list_.erase(iter);
        outer_index_.erase(frame_id);
        pool_cache_list_.push_front(frame_id);
        pool_cache_index_[frame_id] = pool_cache_list_.begin();
    } else {
        if (pool_cache_index_.count(frame_id) != 0U) {
            auto iter = pool_cache_index_[frame_id];
            pool_cache_list_.erase(iter);
        }
        pool_cache_list_.push_front(frame_id);
        pool_cache_index_[frame_id] = pool_cache_list_.begin();
    }
}
/**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);

    if (frame_id > static_cast<int>(replacer_size_)) {
        throw std::exception();
    }
    if (access_record_[frame_id] == 0) return;
    if (Evictable_[frame_id] && set_evictable == false) {
        curr_size_--;
        Evictable_[frame_id] = false;
    } else if (Evictable_[frame_id] == false && set_evictable == true){ 
        curr_size_++;
        Evictable_[frame_id] = true;
    }
    return ;
        //Evictable_[frame_id] = set_evictable;

}
  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);

    if (frame_id > static_cast<int>(replacer_size_)) {
        throw std::exception();
    }
    auto cnt = access_record_[frame_id];
    if (cnt == 0) {
        return ;
    }
    if (Evictable_[frame_id] == false) {
        throw std::exception();
    }
    if (cnt < k_) {
        outer_list_.erase(outer_index_[frame_id]);
        outer_index_.erase(frame_id);
    } else {
        pool_cache_list_.erase(pool_cache_index_[frame_id]);
        pool_cache_index_.erase(frame_id);
    }

    curr_size_--;
    access_record_[frame_id] = 0;
    Evictable_[frame_id] = false;

}

auto LRUKReplacer::Size() -> size_t { 
    std::scoped_lock<std::mutex> lock(latch_);
    return curr_size_; 
    }

}  // namespace bustub
