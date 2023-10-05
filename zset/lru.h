
 // coldcolacos@gmail.com

#ifndef __LRU_H__
#define __LRU_H__

#include <string_view>
#include <vector>

#include "tsl/robin_map.h"

#include "settings.h"

namespace ZSET {

using lru_size_t = uint32_t;
using lru_map_t = tsl::robin_map<std::string_view, lru_size_t>;

enum LRUState {
  LRU_OK = 0,   // Persisted
  LRU_DIRTY,    // Wait to be persisted to rocksdb
  LRU_EXPIRED,  // Wait to be deleted from rocksdb
  LRU_RECOVERY  // Recovery from existing rocksdb
};

template <typename _T>
class LRU {
 public:
  LRU(lru_size_t cap): root_(0), count_(0), iterator_valid_(false) {
    capacity_ = cap;
    nodes_.resize(capacity_ + 1);
  }
  ~LRU() {
    for (int i = 0; i < capacity_; i ++) {
      delete nodes_[i].ptr;
    }
  }
  LRU(const LRU& lru) = delete;
  LRU& operator=(const LRU& lru) = delete;

  /*
    If lru is nearly full, we need to persist some data
    from memory to disk. After that, data in lru can be
    eliminated safely.
  */
  bool      Full() const;
  bool      Has(const char* s);
  _T*       Refresh(const char* s);
  void      Remove(const char* s);
  void      Resize(uint32_t zset_card);

 private:
  struct Node {
    lru_size_t prev;
    lru_size_t next;
    _T* ptr;
    Node(): prev(0), next(0), ptr(nullptr) {}
  };

  lru_size_t capacity_;
  std::vector<Node> nodes_;
  std::vector<lru_size_t> free_list_;
  lru_size_t root_;
  lru_size_t count_;

  lru_map_t kv_;
  lru_map_t::iterator iterator_;
  bool iterator_valid_;
};

template <typename _T>
inline bool LRU<_T>::Full() const {
  return count_ == capacity_ &&
    nodes_[nodes_[root_].prev].ptr->get_lru_state();
}

template <typename _T>
bool LRU<_T>::Has(const char* s) {
  iterator_ = kv_.find(s);
  iterator_valid_ = iterator_ != kv_.end();
  return iterator_valid_;
}

template <typename _T>
_T* LRU<_T>::Refresh(const char* s) {
  auto it = iterator_valid_ ? iterator_ : kv_.find(s);
  // Already in lru
  if (it != kv_.end()) {
    iterator_valid_ = false;
    lru_size_t cur = it->second;
    auto node = nodes_[cur];
    if (cur == nodes_[root_].next) {
      return nodes_[cur].ptr;
    }
    lru_size_t prev = nodes_[cur].prev;
    lru_size_t next = nodes_[cur].next;
    lru_size_t head = nodes_[root_].next;
    nodes_[root_].next = cur;
    nodes_[cur].prev = root_;
    nodes_[cur].next = head;
    nodes_[head].prev = cur;
    nodes_[prev].next = next;
    nodes_[next].prev = prev;
    return nodes_[cur].ptr;
  }
  // Not in lru, two cases:
  //   (1) and lru is full
  if (count_ == capacity_) {
    lru_size_t tail = nodes_[root_].prev;
    _T* old_ptr = nodes_[tail].ptr;
    char* old_key = old_ptr->get_key_string();
    kv_.erase(kv_.find(old_key));
    old_ptr->set_key_string(s);
    kv_[old_key] = tail;

    lru_size_t tail_prev = nodes_[tail].prev;
    lru_size_t head = nodes_[root_].next;
    nodes_[tail_prev].next = root_;
    nodes_[root_].prev = tail_prev;
    nodes_[root_].next = tail;
    nodes_[tail].prev = root_;
    nodes_[tail].next = head;
    nodes_[head].prev = tail;
    return old_ptr;
  }
  //   (2) and lru is not full
  lru_size_t cur = ++ count_;
  if (!free_list_.empty()) {
    cur = free_list_.back();
    free_list_.pop_back();
  } else {
    nodes_[cur].ptr = new _T();
  }
  lru_size_t head = nodes_[root_].next;
  nodes_[root_].next = cur;
  nodes_[cur].prev = root_;
  nodes_[cur].next = head;
  nodes_[head].prev = cur;

  nodes_[cur].ptr->set_key_string(s);
  kv_[nodes_[cur].ptr->get_key_string()] = cur;
  return nodes_[cur].ptr;
}

template <typename _T>
void LRU<_T>::Remove(const char* s) {
  auto it = kv_.find(s);
  if (it == kv_.end()) {
    return;
  }
  if (iterator_valid_ && iterator_ == it) {
    iterator_valid_ = false;
  }
  lru_size_t cur = it->second;
  -- count_;
  kv_.erase(it);
  free_list_.push_back(cur);
  lru_size_t prev = nodes_[cur].prev;
  lru_size_t next = nodes_[cur].next;
  nodes_[prev].next = next;
  nodes_[next].prev = prev;
}

template <typename _T>
inline void LRU<_T>::Resize(uint32_t zset_card) {
  if ((zset_card >> 3) > capacity_) {
    capacity_ <<= 1;
    nodes_.resize(capacity_ + 1);
  }
}

} // namespace ZSET

#endif // __LRU_H__
