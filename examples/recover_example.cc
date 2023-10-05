#include "zset/zset.h"

int main() {
  {
    ZSET::Zset<int> z("recover-example");
    for (int i = 1; i <= 1000; i ++) {
      z.Zadd(std::to_string(i), i * i - i * 100);
    }
  }

  ZSET::Zset<int> z("recover-example");
  auto [found, score] = z.Zscore("101");
  assert(found && 101 == score);

  assert(1000 == z.Zrank("1000"));
}
