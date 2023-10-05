# Zset on rocksdb

The header-only zset is implemented in C++17, runs in single thread, and provides persistence on the basis of Facebook [Rocksdb](https://github.com/facebook/rocksdb). Atomic persistence is guaranteed for zadd and zrem operations.

# Example

```cpp
#include "zset/zset.h"

int main() {
  ZSET::Zset<int> z("simple-example");
  for (int i = 1; i <= 1000; i ++) {
    z.Zadd(std::to_string(i), i * i - i * 100);
  }

  auto [found, score] = z.Zscore("101");
  assert(found && 101 == score);

  assert(1000 == z.Zrank("1000"));

  ZSET::strs top_members;
  z.Zrange(&top_members, 1, 3);
  ZSET::strs ans = {"50", "49", "51"};
  assert(ans == top_members);

  ZSET::pairs<int> top_pairs;
  z.Zrange(&top_pairs, 1, 3);
  ZSET::pairs<int> ans_ = {{"50", -2500}, {"49", -2499}, {"51", -2499}};
  assert(ans_ == top_pairs);
}
```

# Custom Score Type

A custom score type should satisfy `boost::has_less`, `boost::has_plus_assign` and `boost::is_pod`. See examples/ for details.

# Benchmark

The main bottleneck of single-thread zset is synchronization read from dict. Benchmark of Operation Per Second (OPS) is as follows, set up on 1 million random key-value pairs.

| Dict Type         | Storage Medium    | Zadd OPS  | Zscore OPS    |
| :---              | :---              | ---:      | ---:          |
| ROBIN\_MAP\_DICT  | Memory            | 127,846   | 2,223,265     |
| ROCKSDB\_DICT     | Disk              |  31,105   | 183,907       |

# Installation

* Copy zset and third_party/tsl to the include directory in your project

* Install the dependent library rocksdb

```
cd install_scripts && bash install_rocksdb.sh
```

Of course, you can just use zset in memory if you do not want to install rocksdb.

```
$ vi CMakeLists.txt

# -L/usr/local/lib -lrocksdb -lpthread -lz -llz4 -lsnappy -lbz2

$ vi your_code.cc

#define NO_ROCKSDB
#include "zset/zset.h"
```

# API

Note: The rank index starts from 1, differing from Redis where rank starts from 0.

1. zadd

```cpp
uint32_t Zadd(const char* member, const _T& score);
uint32_t Zadd(const std::string& member, const _T& score);
```

2. zcard

```cpp
uint32_t Zcard() const;
```

3. zcount

```cpp
uint32_t Zcount(const _T& min_score, const _T& max_score) const;
```

4. zincrby

```cpp
_T Zincrby(const char* member, _T increment);
_T Zincrby(const std::string& member, _T increment);
```

5. zinterstore

```cpp
std::unique_ptr<ZSET_TYPE> Zinterstore(ZSET_TYPE* b,
                                       const std::string& inter_zset_name,
                                       ZsetDictType dict_type = ZSET_DEFAULT_DICT);
```

6. zlexcount

```cpp
uint32_t Zlexcount(const char* start, bool with_start,
                   const char* stop, bool with_stop) const;
uint32_t Zlexcount(const std::string& start, bool with_start,
                   const std::string& stop, bool with_stop) const;
```

7. zpopmax

```cpp
uint32_t Zpopmax(strs* members, uint32_t count = 1);
uint32_t Zpopmax(pairs<_T>* members_and_scores, uint32_t count = 1);
```

8. zpopmin

```cpp
uint32_t Zpopmin(strs* members, uint32_t count = 1);
uint32_t Zpopmin(pairs<_T>* members_and_scores, uint32_t count = 1);
```

9. zrange

```cpp
uint32_t Zrange(strs* members,
                uint32_t start, uint32_t stop, uint32_t limit = 0) const;
uint32_t Zrange(pairs<_T>* members_and_scores,
                uint32_t start, uint32_t stop, uint32_t limit = 0) const;
```

10. zrangebylex

```cpp
uint32_t Zrangebylex(strs* members,
                     const char* start, bool with_start,
                     const char* stop, bool with_stop,
                     uint32_t limit = 0) const;
uint32_t Zrangebylex(pairs<_T>* members_and_scores,
                     const char* start, bool with_start,
                     const char* stop, bool with_stop,
                     uint32_t limit = 0) const;
```

11. zrangebyscore

```cpp
uint32_t Zrangebyscore(strs* members,
                       const _T& min_score, const _T& max_score,
                       uint32_t limit = 0) const;
uint32_t Zrangebyscore(pairs<_T>* members_and_scores,
                       const _T& min_score, const _T& max_score,
                       uint32_t limit = 0) const;
```

12. zrank

```cpp
uint32_t Zrank(const char* member) const;
uint32_t Zrank(const std::string& member) const;
```

13. zrem

```cpp
uint32_t Zrem(const char* member);
uint32_t Zrem(const std::string& member);
```

14. zremrangebylex

```cpp
uint32_t Zremrangebylex(const char* start, bool with_start,
                        const char* stop, bool with_stop);
```

15. zremrangebyrank

```cpp
uint32_t Zremrangebyrank(uint32_t start, uint32_t stop);
```

16. zremrangebyscore

```cpp
uint32_t Zremrangebyscore(const _T& min_score, const _T& max_score);
```

17. zrevrange

```cpp
uint32_t Zrevrange(strs* members, uint32_t start, uint32_t stop,
                   uint32_t limit = 0) const;
```

18. zrevrangebyscore

```cpp
uint32_t Zrevrangebyscore(strs* members,
                          const _T& max_score, const _T& min_score,
                          uint32_t limit = 0) const;
```

19. zrevrank

```cpp
uint32_t Zrevrank(const char* member) const;
uint32_t Zrevrank(const std::string& member) const;
```

20. zscore

```cpp
std::pair<bool, _T> Zscore(const char* member) const;
std::pair<bool, _T> Zscore(const std::string& member) const;
```

21. zunionstore

```cpp
std::unique_ptr<ZSET_TYPE> Zunionstore(ZSET_TYPE* b,
                                       const std::string& union_zset_name,
                                       ZsetDictType dict_type = ZSET_DEFAULT_DICT);
```
