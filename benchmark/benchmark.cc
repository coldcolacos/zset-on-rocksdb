#include <chrono>

#include "zset/zset.h"

using namespace ZSET;
using hrc = std::chrono::high_resolution_clock;

struct Timer {
  hrc::time_point start_time;

  void tick() {
    start_time = hrc::now();
  }

  double tock() {
    return std::chrono::duration_cast<std::chrono::microseconds>
      (hrc::now() - start_time).count() / 1e6;
  }
} timer;

pairs<int> rand_kv_list;

void prepare_data(int n) {
  for (int i = 1; i <= n; i ++) {
    rand_kv_list.emplace_back(std::to_string(rand()), rand());
  }
}

void benchmark(std::string name, ZsetDictType dict_type) {
  printf("\n\t===== Benchmark %s \t=====\n", name.data());
  timer.tick();
  Zset<int> z(name, dict_type);
  for (auto& kv: rand_kv_list) {
    z.Zadd(kv.first, kv.second);
  }
  printf("\tZadd \tOPS = \t%f\n", rand_kv_list.size() / timer.tock());

  timer.tick();
  uint32_t count = 0;
  for (auto& kv: rand_kv_list) {
    auto [found, score] = z.Zscore(kv.first);
    count += found;
  }
  assert(count >= z.Zcard());
  printf("\tZscore \tOPS = \t%f\n", rand_kv_list.size() / timer.tock());
}

using hrc = std::chrono::high_resolution_clock;

int main() {
  prepare_data(1000'000);
  benchmark("ROBIN_MAP_DICT", ROBIN_MAP_DICT);
  benchmark("ROCKSDB_DICT", ROCKSDB_DICT);
  puts("");
}
