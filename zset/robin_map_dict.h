
 // coldcolacos@gmail.com

#ifndef __ROBIN_MAP_DICT__
#define __ROBIN_MAP_DICT__

#include <map>
#include <string_view>

#include "dict_interface.h"

namespace ZSET {

template<typename _T>
class RobinMapDict: public DictInterface<_T> {
 public:
  RobinMapDict() = default;
  ~RobinMapDict();

  // Memory operations
  _T*                   Find(const char* key) override;
  void                  Erase(_T* t) override;
  [[nodiscard]] _T*     NewKeyBuffer(const char* key, bool is_root = false) override;

  // No persist operations

 private:
  tsl::robin_map<std::string_view, _T*> data_;
  // Memory pool to reuse MemberScore objects
  std::vector<_T*> memory_pool_;
  static constexpr int kMaxMemoryPoolSize = 100000;
};

template<typename _T>
RobinMapDict<_T>::~RobinMapDict() {
  for (auto& it : memory_pool_) {
    if (it) delete it;
  }
}

template<typename _T>
_T* RobinMapDict<_T>::Find(const char* key) {
  auto it = data_.find(key);
  return it == data_.end() ? nullptr : it->second;
}

template<typename _T>
void RobinMapDict<_T>::Erase(_T* t) {
  auto it = data_.find(t->get_key_string());
  if (memory_pool_.size() >= kMaxMemoryPoolSize) {
    delete t;
  } else {
    memory_pool_.push_back(t);
  }
  if (it != data_.end()) {
    data_.erase(it);
  }
}

template<typename _T>
_T* RobinMapDict<_T>::NewKeyBuffer(const char* key, bool is_root) {
  if(memory_pool_.empty()) {
    for (int i = 0; i < 16; i ++) {
      memory_pool_.push_back(new _T());
    }
  }
  auto buffer = memory_pool_.back();
  memory_pool_.pop_back();
  buffer->set_key_string(key);
  data_[buffer->get_key_string_view()] = buffer;
  return buffer;
}

} // namespace ZSET

#endif // __ROBIN_MAP_DICT__
