#include <boost/type_traits.hpp>
#include <cassert>

#include "zset/zset.h"

#define eval(func) static_assert(boost::func<CustomScore>::value)

using namespace ZSET;

struct CustomScore {
  int x;
  double y;

  CustomScore() = default;
  CustomScore(int x, double y): x(x), y(y) {}
  bool operator<(const CustomScore& other) const {
    return x < other.x || x == other.x && y < other.y;
  }
  CustomScore& operator+=(const CustomScore& other) {
    x += other.x;
    y += other.y;
    return *this;
  }
};

int main() {
  eval(has_less);
  eval(has_plus_assign);
  eval(is_pod);

  Zset<CustomScore> z("custom-score-example", ROCKSDB_DICT);
  z.Zadd("A", CustomScore(1, 2.2));
  z.Zadd("B", CustomScore(1, 2.3));
  z.Zadd("C", CustomScore(4, 5.6));
  assert(2 == z.Zrank("B"));
}
