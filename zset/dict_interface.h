
 // coldcolacos@gmail.com

#ifndef __DICT_INTERFACE_H__
#define __DICT_INTERFACE_H__

#include <cstring>
#include <string>
#include <vector>

#include "tsl/robin_map.h"
#include "tsl/robin_set.h"

#include "lru.h"
#include "settings.h"

namespace ZSET {

const char* kZsetRoot = "";

template<typename _T>
class DictInterface {
 public:
  DictInterface() {}
  virtual ~DictInterface() {}
  DictInterface(const DictInterface& d) = delete;
  DictInterface& operator=(const DictInterface& d) = delete;

  // Memory operations
  virtual void                  Erase(_T* t) = 0;
  virtual _T*                   Find(const char* key) = 0;
  [[nodiscard]] virtual _T*     NewKeyBuffer(const char* key,
                                             bool is_root = false) = 0;
  virtual void                  ResizeLRUCapacity(uint32_t card) {}

  // Persist operations
  virtual void Persist(_T* t) {}
  virtual void BatchAdd(_T* t) {}
  virtual void BatchDelete(_T* t) {}
  virtual void BatchPersist(bool force = false) {}

  //   Begin
  virtual bool IterBegin(const char* key) { return false; }
  //   Store the current key to std::string
  virtual void IterKey(std::string& key) {}
  //   Return true if iterator is not at the end
  virtual bool IterValid() { return false; }
  //   Step to the next key
  virtual void IterNext() {}

};

} // namespace ZSET

#endif // __DICT_H__
