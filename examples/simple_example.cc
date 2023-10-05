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
