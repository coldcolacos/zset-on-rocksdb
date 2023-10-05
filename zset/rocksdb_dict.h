
 // coldcolacos@gmail.com

#ifndef __ROCKSDB_DICT_H__
#define __ROCKSDB_DICT_H__

#include <cassert>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

#include "dict_interface.h"

namespace ZSET {

template<typename _T>
class RocksdbDict: public DictInterface<_T> {
 public:
  RocksdbDict(std::string db_path, bool error_if_exists);
  ~RocksdbDict() override;

  // Memory operations
  void                  Erase(_T* t) override {}
  _T*                   Find(const char* key) override;
  [[nodiscard]] _T*     NewKeyBuffer(const char* key, bool is_root = false) override;
  void                  ResizeLRUCapacity(uint32_t zset_card) override;

  // Persist operations
  //  1) Persist single key
  void Persist(_T* t) override;
  //  2) Append Put operation to WriteBatch
  void BatchAdd(_T* t) override;
  //  3) Append Delete operation to WriteBatch
  void BatchDelete(_T* t) override;
  //  4) Persist a batch of Put/Delete operations
  void BatchPersist(bool force = false) override;

  bool IterBegin(const char* key) override;
  void IterKey(std::string& key) override;
  bool IterValid() override;
  void IterNext() override;

 private:
  // Use lru as write buffer
  std::unique_ptr<LRU<_T>> lru_;
  _T root_;
  // Batch write
  ROCKSDB_NAMESPACE::WriteBatch write_batch_;
  std::vector<_T*> updated_ptrs_;
  // Iterator
  std::unique_ptr<ROCKSDB_NAMESPACE::Iterator> iterator_;
  // String buffer to Get from rocksdb
  std::string string_buffer_;
  // Rocksdb options
  std::unique_ptr<ROCKSDB_NAMESPACE::DB> rocksdb_;
  ROCKSDB_NAMESPACE::Options options_;
  ROCKSDB_NAMESPACE::ReadOptions read_options_;
  ROCKSDB_NAMESPACE::Status status_;
  ROCKSDB_NAMESPACE::WriteOptions write_options_;
};

template<typename _T>
RocksdbDict<_T>::RocksdbDict(std::string db_path, bool error_if_exists) {
  // Read options
  read_options_.verify_checksums = false;
  // Db options
  // options_.compression = ROCKSDB_NAMESPACE::kNoCompression;
  options_.create_if_missing = true;
  options_.error_if_exists = error_if_exists;
  options_.bytes_per_sync = 1 << 20;
  options_.max_background_compactions = 4;
  options_.max_background_flushes = 2;
  // Table options
  ROCKSDB_NAMESPACE::BlockBasedTableOptions table_options;
  //   1) Bloom filter
  table_options.filter_policy.reset(
    ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10, false));
  //   2) Block cache
  table_options.block_cache = ROCKSDB_NAMESPACE::NewLRUCache(64 << 20);
  options_.table_factory.reset(
    ROCKSDB_NAMESPACE::NewBlockBasedTableFactory(table_options));
  // Open rocksdb
  ROCKSDB_NAMESPACE::DB* db_ptr = nullptr;
  status_ = ROCKSDB_NAMESPACE::DB::Open(options_, db_path, &db_ptr);
  assert(status_.ok());
  rocksdb_.reset(db_ptr);
  // Do recovery if db dir already exists
  status_ = rocksdb_->Get(read_options_, kZsetRoot, &string_buffer_);
  if (status_.ok()) {
    // Load root key from rocksdb
    root_.set_value_string(string_buffer_);
    root_.set_lru_state(LRU_RECOVERY);
  } else {
    root_.set_lru_state(LRU_OK);
  }
  // LRU
  lru_.reset(new LRU<_T>(1 << 10));
}

template<typename _T>
RocksdbDict<_T>::~RocksdbDict() {

#ifdef ROCKSDB_BULK_WRITE_SIZE
  BatchPersist(true);
#endif
  iterator_.release();
}

template<typename _T>
_T* RocksdbDict<_T>::Find(const char* key) {
  assert(*key != '\0');

#ifdef ROCKSDB_BULK_WRITE_SIZE
  BatchPersist();
#endif

  // Lookup key in lru
  if (lru_->Has(key)) {
    _T* t = lru_->Refresh(key);
    return t->get_lru_state() == LRU_EXPIRED ? nullptr : t;
  }
  // Lookup key in rocksdb
  status_ = rocksdb_->Get(read_options_, key, &string_buffer_);
  if (status_.ok()) {
    _T* t = lru_->Refresh(key);
    t->set_value_string(string_buffer_);
    return t;
  }
  // Not found
  return nullptr;
}

template<typename _T>
_T* RocksdbDict<_T>::NewKeyBuffer(const char* key, bool is_root) {
  if (!is_root) {

#ifdef ROCKSDB_BULK_WRITE_SIZE
    BatchPersist();
#endif

    return lru_->Refresh(key);
  }
  return &root_;
}

template<typename _T>
void RocksdbDict<_T>::ResizeLRUCapacity(uint32_t zset_card) {
  lru_->Resize(zset_card);
}

template<typename _T>
void RocksdbDict<_T>::Persist(_T* t) {
  rocksdb_->Put(write_options_, t->get_key_string(),
                                t->get_value_string_view());
}

template<typename _T>
void RocksdbDict<_T>::BatchAdd(_T* t) {
  auto state = t->get_lru_state();
  if (state != LRU_DIRTY) {
    t->set_lru_state(LRU_DIRTY);
    if (!state) {
      updated_ptrs_.push_back(t);
    }
  }
}

template<typename _T>
void RocksdbDict<_T>::BatchDelete(_T* t) {
  auto state = t->get_lru_state();
  if (state != LRU_EXPIRED) {
    t->set_lru_state(LRU_EXPIRED);
    if (!state) {
      updated_ptrs_.push_back(t);
    }
  }
}

template<typename _T>
void RocksdbDict<_T>::BatchPersist(bool force) {

#ifdef ROCKSDB_BULK_WRITE_SIZE
  if (!force && !lru_->Full() &&
      updated_ptrs_.size() < ROCKSDB_BULK_WRITE_SIZE) {
    return;
  }
#endif

  // Flush dirty data in write buffer
  for (auto t : updated_ptrs_) {
    auto key = t->get_key_string();
    auto lru_state = t->get_lru_state();
    if (lru_state == LRU_DIRTY) {
      t->set_lru_state(LRU_OK);
      write_batch_.Put(key, t->get_value_string_view());
    } else if (lru_state == LRU_EXPIRED) {
      t->set_lru_state(LRU_OK);
      write_batch_.Delete(key);
      lru_->Remove(key);
    }
  }
  // Persist to disk
  rocksdb_->Write(write_options_, &write_batch_);
  updated_ptrs_.clear();
  write_batch_.Clear();
}


////////////////////////////// BEGIN Iterator //////////////////////////////
template<typename _T>
bool RocksdbDict<_T>::IterBegin(const char* key) {

#ifdef ROCKSDB_BULK_WRITE_SIZE
    BatchPersist(true);
#endif

  iterator_.reset(rocksdb_->NewIterator(read_options_));
  iterator_->Seek(key);
  // Skip root key
  if (IterValid() && iterator_->key().size() == 0) {
    IterNext();
  }
  return IterValid();
}

template<typename _T>
void RocksdbDict<_T>::IterKey(std::string& key) {
  key.assign(iterator_->key().data(), iterator_->key().size());
}

template<typename _T>
bool RocksdbDict<_T>::IterValid() {
  return iterator_->Valid();
}

template<typename _T>
void RocksdbDict<_T>::IterNext() {
  iterator_->Next();
}
////////////////////////////// END Iterator //////////////////////////////


} // namespace ZSET

#endif // __ROCKSDB_DICT_H__
