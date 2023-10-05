
 // coldcolacos@gmail.com

#ifndef __ZSET_H__
#define __ZSET_H__

#include <cstdio>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "robin_map_dict.h"

#ifndef NO_ROCKSDB
#include "rocksdb_dict.h"
#endif

namespace ZSET {

using strs = std::vector<std::string>;

template <typename _T>
using pairs = std::vector<std::pair<std::string, _T>>;

#ifdef NO_ROCKSDB
enum ZsetDictType { ROBIN_MAP_DICT };
static constexpr auto ZSET_DEFAULT_DICT = ROBIN_MAP_DICT;
#else
enum ZsetDictType { ROBIN_MAP_DICT, ROCKSDB_DICT };
static constexpr auto ZSET_DEFAULT_DICT = ROCKSDB_DICT;
#endif



int GetRandLevel(int level_limit) {
  static constexpr int kRandThreshold = RAND_MAX * SKIPLIST_P;
  int x = 1;
  while (rand() < kRandThreshold  && x < level_limit) ++ x;
  return x;
}


#define ZSET_TEMPLATE   template <typename _T, int _MaxMemberLen, int _MaxLevel>
#define ZSET_TYPE       Zset<_T, _MaxMemberLen, _MaxLevel>

template <typename _T, int _MaxMemberLen = 10, int _MaxLevel = 15>
class Zset {
 public:
  ////////////////////////////// BEGIN class MemberScore //////////////////////////////
  class MemberScore {
   public:
    MemberScore() = default;
    explicit MemberScore(int level) {
      Clear();
      set_level(level);
    }
    MemberScore(const char* member, _T score, int level) {
      Clear();
      set_member(0, member);
      set_score(0, score);
      set_level(level);
    }
    ~MemberScore() = default;
    MemberScore(const MemberScore& ms) = delete;
    MemberScore& operator=(const MemberScore& ms) = delete;
    // getters & setters
    //   score
    inline _T get_score(int lvl = 0) {
      return *get_score_addr(lvl);
    }
    inline void set_score(int lvl, _T score) {
      *get_score_addr(lvl) = score;
    }
    //   member
    inline char* get_member(int lvl = 0) {
      return get_member_addr(lvl);
    }
    inline void set_member(int lvl, const char* member) {
      memcpy(get_member_addr(lvl), member, strlen(member) + 1);
    }
    //   level
    inline uint8_t get_level() {
      return *get_level_addr();
    }
    inline void set_level(uint8_t level) {
      *get_level_addr() = level;
    }
    //   lru state
    inline uint8_t get_lru_state() {
      return *get_lru_state_addr();
    }
    inline void set_lru_state(uint8_t deleted) {
      *get_lru_state_addr() = deleted;
    }
    //   step
    inline uint32_t get_step(int lvl = 0) {
      return lvl <= get_level() ? *get_step_addr(lvl) : 0;
    }
    inline void set_step(int lvl, uint32_t step) {
      *get_step_addr(lvl) = step;
    }
    inline void inc_step(int lvl = 0) {
      ++ *get_step_addr(lvl);
    }
    inline void dec_step(int lvl = 0) {
      -- *get_step_addr(lvl);
    }
    // comparison functions
    inline int Compare(int lvl, const _T& score, const char* member) {
      char* mbr = get_member_addr(lvl);
      _T* scr = get_score_addr(lvl);
      if (*mbr == '\0' || score < *scr) return 1;
      if (*scr < score) return -1;
      return strcmp(mbr, member);
    }
    inline int MemberCompare(int lvl, const char* member) {
      char* mbr = get_member_addr(lvl);
      return *mbr == '\0' ? 1 : strcmp(mbr, member);
    }
    inline int ScoreCompare(int lvl, const _T& score) {
      char* mbr = get_member_addr(lvl);
      _T* scr = get_score_addr(lvl);
      if (*mbr == '\0' || score < *scr) return 1;
      if (*scr < score) return -1;
      return 0;
    }
    // functions to parse from/to key/value
    inline char* get_key_string() {
      return get_member();
    }
    inline void set_key_string(const char* s) {
      set_member(0, s);
    }
    inline std::string_view get_key_string_view() {
      char* mbr = get_member();
      return {mbr, strlen(mbr)};
    }
    inline std::string_view get_value_string_view() {
      static constexpr size_t offset = 4 + kTupleSize;
      return {buffer_, offset + kTupleSize * get_level()};
    }
    inline void set_value_string(std::string& s) {
      memcpy(buffer_, s.data(), s.size());
    }

    /*
    void Debug(bool ignore = true) {
      printf("   [level %d] score=%d member=%s step=%u\n", get_level(), get_score(), get_member(), get_step());
      for (int i = 1; i <= get_level(); i ++) {
        if (ignore && *get_member(i) == '\0') continue;
        printf("=> [level %d] score=%d member=%s step=%u\n", i, get_score(i), get_member(i), get_step(i));
      }
      puts("");
    }
    */

   private:
    static constexpr int kScoreSize = sizeof(_T);
    // tuple = (score, member, step)
    static constexpr int kTupleSize = kScoreSize + _MaxMemberLen + 1 + 4;
    static constexpr int kBufferSize = 4 + kTupleSize * (_MaxLevel + 1);
    /*
      buffer contains:
        - lru state   (1 byte : uint8_t )
        - level       (1 byte : uint8_t )
        - score size  (2 bytes: uint16_t)
        - member score list
            - (score, member, step)
            - (score 1, member 1, step 1)
            - (score 2, member 2, step 2)
            - ...
            - (score L, member L, step L)
    */
    char buffer_[kBufferSize];

    inline void Clear() {
      memset(buffer_, 0, sizeof buffer_);
      *get_score_size_addr() = kScoreSize;
    }
    inline _T* get_score_addr(int lvl = 0) {
      return reinterpret_cast<_T*>(buffer_ + 4 + kTupleSize * lvl);
    }
    inline char* get_member_addr(int lvl = 0) {
      static constexpr int offset = 4 + kScoreSize;
      return buffer_ + offset + kTupleSize * lvl;
    }
    inline uint32_t* get_step_addr(int lvl = 0) {
      static constexpr int offset = 4 + kScoreSize + _MaxMemberLen + 1;
      return reinterpret_cast<uint32_t*>(buffer_ + offset + kTupleSize * lvl);
    }
    inline uint8_t* get_level_addr() {
      return reinterpret_cast<uint8_t*>(buffer_ + 1);
    }
    inline uint8_t* get_lru_state_addr() {
      return reinterpret_cast<uint8_t*>(buffer_);
    }
    inline uint16_t* get_score_size_addr() {
      return reinterpret_cast<uint16_t*>(buffer_ + 2);
    }
  };
  ////////////////////////////// END class MemberScore //////////////////////////////

  Zset(std::string key,
       ZsetDictType dict_type = ZSET_DEFAULT_DICT,
       bool error_if_exists = false)
    : key_(key), max_level_(0), card_(0) {

    static_assert(std::is_pod<_T>::value);

    if (dict_type == ROBIN_MAP_DICT) {
      dict_.reset(new RobinMapDict<MemberScore>());
    }

#ifndef NO_ROCKSDB
    else if (dict_type == ROCKSDB_DICT) {
      dict_.reset(new RocksdbDict<MemberScore>(key, error_if_exists));
    }
#endif

    root_ = dict_->NewKeyBuffer(kZsetRoot, true);
    if (root_->get_lru_state() == LRU_OK) {
      new (root_) MemberScore(kZsetRoot, _T(), 0);
    } else {
      max_level_ = root_->get_level();
      card_ = FindLast();
      root_->set_lru_state(LRU_OK);
    }
    dict_->Persist(root_);
  }
  ~Zset() = default;
  Zset(const Zset& z) = delete;
  Zset& operator=(const Zset& z) = delete;

  ////////////////////////////// BEGIN Definition of Zset APIs //////////////////////////////

  uint32_t                      Zadd(const char* member, const _T& score);
  uint32_t                      Zadd(const std::string& member, const _T& score);
  uint32_t                      Zcard() const;
  uint32_t                      Zcount(const _T& min_score, const _T& max_score) const;
  _T                            Zincrby(const char* member, _T increment);
  _T                            Zincrby(const std::string& member, _T increment);
  std::unique_ptr<ZSET_TYPE>    Zinterstore(ZSET_TYPE* b,
                                            const std::string& inter_zset_name,
                                            ZsetDictType dict_type = ZSET_DEFAULT_DICT);
  uint32_t                      Zlexcount(const char* start, bool with_start,
                                          const char* stop, bool with_stop) const;
  uint32_t                      Zlexcount(const std::string& start, bool with_start,
                                          const std::string& stop, bool with_stop) const;
  uint32_t                      Zpopmax(strs* members, uint32_t count = 1);
  uint32_t                      Zpopmax(pairs<_T>* members_and_scores, uint32_t count = 1);
  uint32_t                      Zpopmin(strs* members, uint32_t count = 1);
  uint32_t                      Zpopmin(pairs<_T>* members_and_scores, uint32_t count = 1);
  uint32_t                      Zrange(strs* members,
                                       uint32_t start, uint32_t stop, uint32_t limit = 0) const;
  uint32_t                      Zrange(pairs<_T>* members_and_scores,
                                       uint32_t start, uint32_t stop, uint32_t limit = 0) const;
  uint32_t                      Zrangebylex(strs* members,
                                            const char* start, bool with_start,
                                            const char* stop, bool with_stop,
                                            uint32_t limit = 0) const;
  uint32_t                      Zrangebylex(pairs<_T>* members_and_scores,
                                            const char* start, bool with_start,
                                            const char* stop, bool with_stop,
                                            uint32_t limit = 0) const;
  uint32_t                      Zrangebyscore(strs* members,
                                              const _T& min_score, const _T& max_score,
                                              uint32_t limit = 0) const;
  uint32_t                      Zrangebyscore(pairs<_T>* members_and_scores,
                                              const _T& min_score, const _T& max_score,
                                              uint32_t limit = 0) const;
  uint32_t                      Zrank(const char* member) const;
  uint32_t                      Zrank(const std::string& member) const;
  uint32_t                      Zrem(const char* member);
  uint32_t                      Zrem(const std::string& member);
  uint32_t                      Zremrangebylex(const char* start, bool with_start,
                                               const char* stop, bool with_stop);
  uint32_t                      Zremrangebyrank(uint32_t start, uint32_t stop);
  uint32_t                      Zremrangebyscore(const _T& min_score, const _T& max_score);
  uint32_t                      Zrevrange(strs* members, uint32_t start, uint32_t stop,
                                          uint32_t limit = 0) const;
  uint32_t                      Zrevrangebyscore(strs* members,
                                                 const _T& max_score, const _T& min_score,
                                                 uint32_t limit = 0) const;
  uint32_t                      Zrevrank(const char* member) const;
  uint32_t                      Zrevrank(const std::string& member) const;
  std::pair<bool, _T>           Zscore(const char* member) const;
  std::pair<bool, _T>           Zscore(const std::string& member) const;
  std::unique_ptr<ZSET_TYPE>    Zunionstore(ZSET_TYPE* b,
                                            const std::string& union_zset_name,
                                            ZsetDictType dict_type = ZSET_DEFAULT_DICT);

  ////////////////////////////// END Declaration of Zset APIs //////////////////////////////

 private:
  ////////////////////////////// BEGIN Declaration of Zset Internal Implementations //////////////////////////////

  MemberScore*                  FindByLex(const char* member) const;
  MemberScore*                  FindByRank(uint32_t rank) const;
  MemberScore*                  FindByScore(_T score) const;
  uint32_t                      FindLast() const;
  void                          ImplZadd(const char* member, _T score);
  uint32_t                      ImplZcount(const _T& score, bool equal_ok) const;
  uint32_t                      ImplZrank(const char* member, _T score) const;
  MemberScore*                  ImplZrem(const char* member, _T score);

  ////////////////////////////// END Declaration of Zset Internal Implementations //////////////////////////////

  // Database in memory/rocksdb
  std::unique_ptr<DictInterface<MemberScore>> dict_;
  // Key of zset, used as db path
  std::string key_;
  // The first node of skiplist
  MemberScore* root_;
  // The max level in the skiplist
  int max_level_;
  // The number of members
  uint32_t card_;
  // Buffer array for zadd
  MemberScore* prev_[_MaxLevel + 1];
  uint32_t prev_step_[_MaxLevel + 1];
};

////////////////////////////// BEGIN Zset APIs //////////////////////////////

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zadd(const char* member, const _T& score) {
  int len = strlen(member);
  if (len > _MaxMemberLen) {
    throw std::length_error("member length exceeds limit");
  }
  if (len == 0) {
    throw std::length_error("member cannot be empty string");
  }

  auto ms = dict_->Find(member);
  if (ms != nullptr) {
    if(ms->ScoreCompare(0, score) == 0) {
      return 0;
    }
    ImplZrem(member, ms->get_score());
  }
  ImplZadd(member, score);
  if (ms == nullptr) {
    dict_->ResizeLRUCapacity(card_);
  }
  return uint32_t(ms == nullptr);
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zadd(const std::string& member, const _T& score) {
  return Zadd(member.data(), score);
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zcard() const {
  return card_;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zcount(const _T& min_score, const _T& max_score) const {
  if (min_score > max_score) {
    return 0;
  }
  return ImplZcount(max_score, true) - ImplZcount(min_score, false);
}

ZSET_TEMPLATE
_T ZSET_TYPE::Zincrby(const char* member, _T increment) {
  if (*member == '\0') {
    throw std::length_error("member cannot be empty string");
  }
  auto ms = dict_->Find(member);
  if (ms != nullptr) {
    increment += ms->get_score();
  }
  Zadd(member, increment);
  return increment;
}

ZSET_TEMPLATE
_T ZSET_TYPE::Zincrby(const std::string& member, _T increment) {
  return Zincrby(member.data(), increment);
}

ZSET_TEMPLATE
std::unique_ptr<ZSET_TYPE> ZSET_TYPE::Zinterstore(ZSET_TYPE* b,
                                                  const std::string& inter_zset_name,
                                                  ZsetDictType dict_type) {
  if (Zcard() > b->Zcard()) {
    return std::move(b->Zinterstore(this, inter_zset_name, dict_type));
  }
  auto inter_zset =
    std::unique_ptr<ZSET_TYPE>(new ZSET_TYPE(inter_zset_name, dict_type, true));
  for (auto ms = root_; ; ) {
    char* mbr = ms->get_member(1);
    if (*mbr == '\0') {
      break;
    }
    _T score_a = ms->get_score(1);
    auto [found_b, score_b] = b->Zscore(mbr);
    if (found_b) {
      score_a += score_b;
      inter_zset->Zadd(mbr, score_a);
    }
    ms = dict_->Find(mbr);
  }
  return std::move(inter_zset);
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zlexcount(const char* start, bool with_start,
                              const char* stop, bool with_stop) const {
  if (card_ == 0 || strcmp(start, stop) > 0) {
    return 0;
  }

  std::string mbr;
  uint32_t start_rank = 0;
  uint32_t stop_rank = card_;
  bool start_found = false;
  bool stop_found = false;

  mbr = FindByLex(start)->get_member(1);
  if (mbr.size() == 0) {
    return 0;
  }
  start_found = (strcmp(mbr.data(), start) == 0);
  start_rank = Zrank(mbr.data());

  mbr = FindByLex(stop)->get_member(1);
  if (mbr.size() > 0) {
    int cmp = strcmp(mbr.data(), stop);
    stop_rank = Zrank(mbr.data()) - (cmp > 0);
    stop_found = (cmp == 0);
  }

  uint32_t count = stop_rank + 1 - start_rank;
  if (start_found && !with_start) {
    count --;
  }
  if (stop_found && !with_stop) {
    count --;
  }
  return count;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zlexcount(const std::string& start, bool with_start,
                              const std::string& stop, bool with_stop) const {
  return Zlexcount(start.data(), with_start, stop.data(), with_stop);
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zpopmax(strs* members, uint32_t count) {
  members->clear();
  if (count == 0) {
    return 0;
  }
  uint32_t prev_rank = card_ > count ? card_ - count : 0;
  MemberScore* prev = FindByRank(prev_rank);
  uint32_t pop_count = card_ - prev_rank;
  for (uint32_t i = 0; i < pop_count; i ++) {
    members->push_back(prev->get_member(1));
    ImplZrem(prev->get_member(1), prev->get_score(1));
  }
  if (pop_count) {
    std::reverse(members->begin(), members->end());
  }
  return pop_count;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zpopmax(pairs<_T>* members_and_scores, uint32_t count) {
  members_and_scores->clear();
  if (count == 0) {
    return 0;
  }
  uint32_t prev_rank = card_ > count ? card_ - count : 0;
  MemberScore* prev = FindByRank(prev_rank);
  uint32_t pop_count = card_ - prev_rank;
  for (uint32_t i = 0; i < pop_count; i ++) {
    members_and_scores->emplace_back(prev->get_member(1), prev->get_score(1));
    ImplZrem(prev->get_member(1), prev->get_score(1));
  }
  if (pop_count) {
    std::reverse(members_and_scores->begin(), members_and_scores->end());
  }
  return pop_count;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zpopmin(strs* members, uint32_t count) {
  members->clear();
  if (count == 0) {
    return 0;
  }
  MemberScore* prev = root_;
  uint32_t pop_count = card_ > count ? count : card_;
  for (uint32_t i = 0; i < pop_count; i ++) {
    members->push_back(prev->get_member(1));
    ImplZrem(prev->get_member(1), prev->get_score(1));
  }
  return pop_count;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zpopmin(pairs<_T>* members_and_scores, uint32_t count) {
  members_and_scores->clear();
  if (count == 0) {
    return 0;
  }
  MemberScore* prev = root_;
  uint32_t pop_count = card_ > count ? count : card_;
  for (uint32_t i = 0; i < pop_count; i ++) {
    members_and_scores->emplace_back(prev->get_member(1), prev->get_score(1));
    ImplZrem(prev->get_member(1), prev->get_score(1));
  }
  return pop_count;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrange(strs* members, uint32_t start, uint32_t stop,
                           uint32_t limit) const {
  members->clear();
  start = std::max(1u, start);
  stop = std::min(card_, stop);
  if (start > stop) {
    return 0;
  }
  auto ms = FindByRank(start - 1);
  uint32_t count = 0;
  for (int i = start; i <= stop; i ++) {
    ms = dict_->Find(ms->get_member(1));
    members->push_back(ms->get_member());
    if (++ count == limit) {
      return count;
    }
  }
  return count;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrange(pairs<_T>* members_and_scores,
                       uint32_t start, uint32_t stop,
                       uint32_t limit) const {
  members_and_scores->clear();
  start = std::max(1u, start);
  stop = std::min(card_, stop);
  if (start > stop) {
    return 0;
  }
  auto ms = FindByRank(start - 1);
  uint32_t count = 0;
  for (int i = start; i <= stop; i ++) {
    ms = dict_->Find(ms->get_member(1));
    members_and_scores->push_back(
      std::make_pair(ms->get_member(), ms->get_score()));
    if (++ count == limit) {
      return count;
    }
  }
  return count;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrangebylex(
  strs* members,
  const char* start, bool with_start,
  const char* stop, bool with_stop,
  uint32_t limit) const {

  members->clear();
  uint32_t count = Zlexcount(start, with_start, stop, with_stop);
  if (count == 0) {
    return 0;
  }
  auto ms = FindByLex(start);
  if (!with_start && strcmp(ms->get_member(1), start) == 0) {
    ms = dict_->Find(ms->get_member(1));
  }
  if (limit != 0 && limit < count) {
    count = limit;
  }
  for (int i = 1; i <= count; i ++) {
    members->push_back(ms->get_member(1));
    if (i < count) {
      ms = dict_->Find(ms->get_member(1));
    }
  }
  return members->size();
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrangebylex(
  pairs<_T>* members_and_scores,
  const char* start, bool with_start,
  const char* stop, bool with_stop,
  uint32_t limit) const {

  members_and_scores->clear();
  uint32_t count = Zlexcount(start, with_start, stop, with_stop);
  if (count == 0) {
    return 0;
  }
  auto ms = FindByLex(start);
  if (!with_start && strcmp(ms->get_member(1), start) == 0) {
    ms = dict_->Find(ms->get_member(1));
  }
  if (limit != 0 && limit < count) {
    count = limit;
  }
  for (int i = 1; i <= count; i ++) {
    members_and_scores->emplace_back(ms->get_member(1), ms->get_score(1));
    if (i < count) {
      ms = dict_->Find(ms->get_member(1));
    }
  }
  return members_and_scores->size();
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrangebyscore(
  strs* members, const _T& min_score, const _T& max_score,
  uint32_t limit) const {

  members->clear();
  if (min_score > max_score) {
    return 0;
  }
  MemberScore* ms = FindByScore(min_score);
  uint32_t count = 0;
  for (;;) {
    std::string mbr = ms->get_member(1);
    _T scr = ms->get_score(1);
    if (mbr.size() != 0 && scr <= max_score) {
      ms = dict_->Find(mbr.data());
      members->emplace_back(std::move(mbr));
      if (++ count == limit) {
        return count;
      }
    } else {
      break;
    }
  }
  return count;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrangebyscore(
  pairs<_T>* members_and_scores, const _T& min_score, const _T& max_score,
  uint32_t limit) const {

  members_and_scores->clear();
  if (min_score > max_score) {
    return 0;
  }
  MemberScore* ms = FindByScore(min_score);
  uint32_t count = 0;
  for (;;) {
    std::string mbr = ms->get_member(1);
    _T scr = ms->get_score(1);
    if (mbr.size() != 0 && scr <= max_score) {
      ms = dict_->Find(mbr.data());
      members_and_scores->emplace_back(std::move(mbr), scr);
      if (++ count == limit) {
        return count;
      }
    } else {
      break;
    }
  }
  return count;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrank(const char* member) const {
  if (*member == '\0') {
    return 0;
  }
  auto ms = dict_->Find(member);
  if (ms == nullptr) {
    return 0;
  }
  return ImplZrank(member, ms->get_score());
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrank(const std::string& member) const {
  return Zrank(member.data());
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrem(const char* member) {
  if (*member == '\0') {
    return 0;
  }
  auto ms = dict_->Find(member);
  if (ms == nullptr) {
    return 0;
  }
  ImplZrem(member, ms->get_score());
  return 1;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrem(const std::string& member) {
  return Zrem(member.data());
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zremrangebylex(const char* start, bool with_start,
                                   const char* stop, bool with_stop) {

  uint32_t removed = Zlexcount(start, with_start, stop, with_stop);
  if (removed == 0) {
    return 0;
  }
  auto ms = FindByLex(start);
  if (!with_start && strcmp(ms->get_member(1), start) == 0) {
    ms = dict_->Find(ms->get_member(1));
  }
  for (int i = 0; i < removed; i ++) {
    ImplZrem(ms->get_member(1), ms->get_score(1));
  }
  return removed;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zremrangebyrank(uint32_t start, uint32_t stop) {
  start = std::max(1u, start);
  stop = std::min(card_, stop);
  if (start > stop) {
    return 0;
  }
  auto ms = FindByRank(start - 1);
  std::string mbr;
  for (uint32_t i = start; i <= stop; i ++) {
    mbr = ms->get_member(1);
    _T scr = ms->get_score(1);
    ImplZrem(mbr.data(), scr);
  }
  return stop - start + 1;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zremrangebyscore(const _T& min_score, const _T& max_score) {
  if (min_score > max_score) {
    return 0;
  }
  MemberScore* ms = FindByScore(min_score);
  uint32_t removed = 0;
  for (;;) {
    std::string mbr = ms->get_member(1);
    _T scr = ms->get_score(1);
    if (mbr.size() != 0 && scr <= max_score) {
      ImplZrem(mbr.data(), scr);
      removed ++;
    } else {
      break;
    }
  }
  return removed;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrevrange(
  strs* members, uint32_t start, uint32_t stop, uint32_t limit) const {

  start = std::max(1u, start);
  stop = std::min(card_, stop);
  if (start > stop) {
    return 0;
  }
  if (stop - start + 1 > limit) {
    start = stop - limit + 1;
  }
  Zrange(start, stop, members);
  if (!members->empty()) {
    std::reverse(members->begin(), members->end());
  }
  return members->size();
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrevrangebyscore(
  strs* members, const _T& max_score, const _T& min_score,
  uint32_t limit) const {

  Zrangebyscore(min_score, max_score, members);
  if (!members->empty()) {
    std::reverse(members->begin(), members->end());
    if (limit < members->size()) {
      *members = {members->begin(), members->begin() + limit};
    }
  }
  return members->size();
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrevrank(const char* member) const {
  if (*member == '\0') {
    return 0;
  }
  auto ms = dict_->Find(member);
  if (ms == nullptr) {
    return 0;
  }
  return card_ + 1 - ImplZrank(member, ms->get_score());
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::Zrevrank(const std::string& member) const {
  return Zrevrank(member.data());
}

ZSET_TEMPLATE
std::pair<bool, _T> ZSET_TYPE::Zscore(const char* member) const {
  if (*member == '\0') {
    return std::make_pair(false, _T());
  }
  auto ms = dict_->Find(member);
  if (ms == nullptr) {
    return std::make_pair(false, _T());
  }
  return std::make_pair(true, ms->get_score());
}

ZSET_TEMPLATE
std::pair<bool, _T> ZSET_TYPE::Zscore(const std::string& member) const {
  return Zscore(member.data());
}

ZSET_TEMPLATE
std::unique_ptr<ZSET_TYPE> ZSET_TYPE::Zunionstore(ZSET_TYPE* b,
                                                  const std::string& union_zset_name,
                                                  ZsetDictType dict_type) {
  if (Zcard() > b->Zcard()) {
    return std::move(b->Zunionstore(this, union_zset_name, dict_type));
  }
  auto union_zset =
    std::unique_ptr<ZSET_TYPE>(new ZSET_TYPE(union_zset_name, dict_type, true));
  // iterate over zset a
  for (auto ms = root_; ; ) {
    char* mbr_a = ms->get_member(1);
    if (*mbr_a == '\0') {
      break;
    }
    _T score_a = ms->get_score(1);
    union_zset->Zadd(mbr_a, score_a);
    ms = dict_->Find(mbr_a);
  }
  // iterate over zset b
  for (auto ms = b->root_; ; ) {
    char* mbr_b = ms->get_member(1);
    if (*mbr_b == '\0') {
      break;
    }
    _T score_b = ms->get_score(1);
    union_zset->Zincrby(mbr_b, score_b);
    ms = b->dict_->Find(mbr_b);
  }
  return std::move(union_zset);
}

////////////////////////////// END Zset APIs //////////////////////////////



////////////////////////////// BEGIN Zset Internal Implementations //////////////////////////////

ZSET_TEMPLATE typename
ZSET_TYPE::MemberScore* ZSET_TYPE::FindByLex(const char* member) const {
  MemberScore* ms = root_;
  for (int i = max_level_; i > 0; -- i) {
    while (ms->MemberCompare(i, member) < 0) {
      char* mbr = ms->get_member(i);
      ms = dict_->Find(mbr);
    }
  }
  return ms;
}

ZSET_TEMPLATE typename
ZSET_TYPE::MemberScore* ZSET_TYPE::FindByRank(uint32_t rank) const {
  if (rank > card_) {
    return nullptr;
  }
  if (rank == 0) {
    return root_;
  }
  MemberScore* ms = root_;
  for (int i = max_level_; i > 0; -- i) {
    for (;;) {
      char* mbr = ms->get_member(i);
      if (*mbr == '\0') {
        break;
      }
      int cur_step = ms->get_step(i);
      if (cur_step > rank) {
        break;
      }
      ms = dict_->Find(mbr);
      rank -= cur_step;
      if (rank == 0) {
        return ms;
      }
    }
  }
  return nullptr;
}

ZSET_TEMPLATE typename
ZSET_TYPE::MemberScore* ZSET_TYPE::FindByScore(_T score) const {
  MemberScore* ms = root_;
  for (int i = max_level_; i > 0; -- i) {
    while (ms->ScoreCompare(i, score) < 0) {
      char* mbr = ms->get_member(i);
      ms = dict_->Find(mbr);
    }
  }
  return ms;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::FindLast() const {
  uint32_t total_step = 0;
  MemberScore* ms = root_;
  for (int i = max_level_; i > 0; -- i) {
    while (true) {
      char* mbr = ms->get_member(i);
      if (*mbr == '\0') {
        break;
      }
      total_step += ms->get_step(i);
      ms = dict_->Find(mbr);
    }
  }
  return total_step;
}

ZSET_TEMPLATE
void ZSET_TYPE::ImplZadd(const char* member, _T score) {
  int rand_level = GetRandLevel(_MaxLevel);
  MemberScore* ms = root_;
  prev_step_[1] = 0;
  uint32_t total_step = 0;
  for (int i = max_level_; i > 0; -- i) {
    while (ms->Compare(i, score, member) < 0) {
      total_step += ms->get_step(i);
      ms = dict_->Find(ms->get_member(i));
    }
    prev_step_[i] = total_step;
    prev_[i] = ms;
  }

  MemberScore* new_ms = new (dict_->NewKeyBuffer(member))
                        MemberScore(member, score, rand_level);
  for (int i = 1; i <= rand_level; ++ i) {
    if (i <= max_level_) {
      char* mbr = prev_[i]->get_member(i);
      uint32_t left_size = prev_step_[1] - prev_step_[i];
      if (*mbr != '\0') {
        new_ms->set_member(i, mbr);
        new_ms->set_score(i, prev_[i]->get_score(i));
        new_ms->set_step(i, prev_[i]->get_step(i) - left_size);
      }
      prev_[i]->set_step(i, left_size + 1);
    } else {
      prev_[i] = root_;
      prev_[i]->set_step(i, prev_step_[1] + 1);
    }
    prev_[i]->set_member(i, member);
    prev_[i]->set_score(i, score);
  }
  int updated_level = rand_level;
  for (int i = rand_level + 1; i <= max_level_; ++ i) {
    if (*prev_[i]->get_member(i) == '\0') {
      break;
    }
    prev_[i]->inc_step(i);
    updated_level = i;
  }
  // Persist to ROCKSDB_DICT
  for (int i = 1; i <= updated_level; i ++) {
    if (i == 1 || prev_[i] != prev_[i-1]) {
      dict_->BatchAdd(prev_[i]);
    }
  }
  dict_->BatchAdd(new_ms);
  // Update card and max level
  card_ ++;
  max_level_ = std::max(max_level_, rand_level);
  root_->set_level(max_level_);
  dict_->BatchPersist();
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::ImplZcount(const _T& score, bool equal_ok) const {
  MemberScore* ms = root_;
  uint32_t total_step = 0;
  int cmp_result = equal_ok ? 0 : -1;
  for (int i = max_level_; i > 0; -- i) {
    while (ms->ScoreCompare(i, score) <= cmp_result) {
      total_step += ms->get_step(i);
      char* mbr = ms->get_member(i);
      ms = dict_->Find(mbr);
    }
  }
  return total_step;
}

ZSET_TEMPLATE
uint32_t ZSET_TYPE::ImplZrank(const char* member, _T score) const {
  MemberScore* ms = root_;
  uint32_t total_step = 0;
  for (int i = max_level_; i > 0; -- i) {
    while (ms->Compare(i, score, member) <= 0) {
      total_step += ms->get_step(i);
      char* mbr = ms->get_member(i);
      ms = dict_->Find(mbr);
    }
    if (ms->Compare(0, score, member) == 0) {
      return total_step;
    }
  }
  return 0;
}

ZSET_TEMPLATE typename
ZSET_TYPE::MemberScore* ZSET_TYPE::ImplZrem(const char* member, _T score) {
  MemberScore* ms = root_;
  int cmp = -1;
  for (int i = max_level_; i > 0; -- i) {
    for (;;) {
      cmp = ms->Compare(i, score, member);
      if (cmp >= 0) {
        break;
      }
      ms = dict_->Find(ms->get_member(i));
    }
    prev_[i] = ms;
  }
  if (cmp != 0) {
    return nullptr;
  }
  auto next = dict_->Find(ms->get_member(1));
  int level = next->get_level();

  for (int i = 1; i <= level; ++ i) {
    char* mbr = next->get_member(i);
    if (*mbr == '\0') {
      prev_[i]->set_member(i, "");
      prev_[i]->set_step(i, 0);
    } else {
      auto next2 = dict_->Find(mbr);
      prev_[i]->set_member(i, next2->get_member());
      prev_[i]->set_score(i, next2->get_score());
      prev_[i]->set_step(i,
        prev_[i]->get_step(i) + next->get_step(i) - 1);
    }
  }
  int updated_level = level;
  for (int i = level + 1; i <= max_level_; ++ i) {
    char* mbr = prev_[i]->get_member(i);
    if (*mbr != '\0') {
      prev_[i]->dec_step(i);
      updated_level = i;
    } else {
      break;
    }
  }
  // Persist to rocksdb
  for (int i = 1; i <= updated_level; ++ i) {
    if (i == 1 || prev_[i] != prev_[i-1]) {
      dict_->BatchAdd(prev_[i]);
    }
  }
  dict_->BatchDelete(next);
  // Erase from memory
  dict_->Erase(next);
  // Update card and max level
  card_ --;
  while (max_level_ && *root_->get_member(max_level_) == '\0') {
    max_level_ --;
  }
  root_->set_level(max_level_);
  dict_->BatchPersist();
  return ms;
}

////////////////////////////// END Zset Interval Implementations //////////////////////////////

#undef ZSET_TYPE
#undef ZSET_TEMPLATE

} // namespace ZSET

#endif // __ZSET_H__
