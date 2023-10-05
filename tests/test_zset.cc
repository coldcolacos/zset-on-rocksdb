
 // coldcolacos@gmail.com

#include "gtest/gtest.h"
#include "zset/zset.h"

using namespace ZSET;

class TestZset: public testing::TestWithParam<ZsetDictType>{};

void GetRandString(std::string& s) {
  int n = rand() % 5 + 6;
  s = "";
  for (int i = 0; i < n; i ++) {
    s += char('a' + rand() % 26);
  }
}

template <typename _Z>
void CheckZset(std::unordered_map<std::string, int>& std_map,
               _Z& test_zset) {
  EXPECT_EQ(std_map.size(), test_zset.Zcard());
  if (std_map.empty()) {
    return;
  }
  std::vector<std::pair<int, std::string>> std_result;
  for (const auto& [score, member] : std_map) {
    std_result.emplace_back(member, score);
  }
  std::sort(std_result.begin(), std_result.end());
  for (int i = 0; i < std_result.size(); i ++) {
    auto& [std_score, std_member] = std_result[i];
    auto [test_found, test_score] = test_zset.Zscore(std_member.data());
    EXPECT_EQ(true, test_found);
    EXPECT_EQ(std_score, test_score);
    EXPECT_EQ(i + 1, test_zset.Zrank(std_member.data()));
    EXPECT_EQ(std_result.size() - i,
              test_zset.Zrevrank(std_member.data()));
  }
}

TEST_P(TestZset, case_1_Zadd_Zcard_Zrank_Zrem_Zrevrank_Zscore){
  std::vector<std::string> members;
  for (int i = 0; i < 20000; i ++) {
    std::string s;
    GetRandString(s);
    members.push_back(std::move(s));
  }
  sort(members.begin(), members.end());
  members.erase(std::unique(members.begin(), members.end()), members.end());

  std::unordered_map<std::string, int> std_map;
  Zset<int> test_zset("test_case_1", GetParam());
  for (int i = 0; i < 100000; i ++) {
    std::string mbr = members[rand() % members.size()];
    if (rand() % 10 == 1) {
      std_map.erase(mbr);
      test_zset.Zrem(mbr.data());
    } else {
      int score = rand() * (i & 3) - rand() * (i & 7);
      std_map[mbr] = score;
      test_zset.Zadd(mbr.data(), score);
    }
    EXPECT_EQ(std_map.size(), test_zset.Zcard());
  }

  CheckZset(std_map, test_zset);
}

TEST_P(TestZset, case_2_Zincrby) {
  std::unordered_map<std::string, int> std_map;
  Zset<int> test_zset("test_case_2", GetParam());
  for (int i = 0; i < 100000; i ++) {
    std::string key = std::to_string(rand() % 1234);
    int value = rand() % 4321;
    if (rand() & 1) {
      std_map[key] = value;
      test_zset.Zadd(key.data(), value);
    } else {
      if (i & 1) value = - (i ^ value);
      std_map[key] += value;
      test_zset.Zincrby(key.data(), value);
    }
  }
  CheckZset(std_map, test_zset);
}

TEST_P(TestZset, case_3_Zcount) {
  Zset<int> test_zset("test_case_3", GetParam());
  int count = 0;
  for (int i = 1; i <= 100000; i ++) {
    test_zset.Zadd(std::to_string(i).data(), i / 2);
    count += (2233 <= i/2 && i/2 <= 3344);
    EXPECT_EQ(count, test_zset.Zcount(2233, 3344));
  }
}

TEST_P(TestZset, case_4_Zlexcount_Zrangebylex_Zremrangebylex) {
  Zset<int> test_zset("test_case_4", GetParam());
  char buffer[10];
  int count = 0;
  bool has_start = false, has_stop = false;
  for (int i = 1; i <= 10000; i ++) {
    int j = (i + 8642) % 10000 + 1;
    sprintf(buffer, "%06d", j);
    test_zset.Zadd(buffer, -12345678);
    count += (555 <= j && j <= 7777);
    has_start |= (j == 555);
    has_stop |= (j == 7777);
    for (int with_start = 0; with_start < 2; with_start ++) {
      for (int with_stop = 0; with_stop < 2; with_stop ++) {
        uint32_t std_count = count - (has_start && !with_start)
                                   - (has_stop && !with_stop);
        uint32_t test_count = test_zset.Zlexcount(
          "000555", with_start, "007777", with_stop);
        EXPECT_EQ(std_count, test_count);
      }
    }
  }

  ZSET::strs members_result;
  ZSET::pairs<int> members_and_scores_result;
  members_result.push_back("abc");

  test_zset.Zrangebylex(&members_result, "", true, "000010", false);
  EXPECT_EQ(9, members_result.size());
  for (int i = 0; i < members_result.size(); i ++) {
    sprintf(buffer, "%06d", i + 1);
    EXPECT_EQ(buffer, members_result[i]);
  }

  test_zset.Zrangebylex(&members_and_scores_result, "002045", false, "002325", true);
  EXPECT_EQ(280, members_and_scores_result.size());
  for (int i = 0; i < members_and_scores_result.size(); i ++) {
    sprintf(buffer, "%06d", i + 2046);
    EXPECT_EQ(buffer, members_and_scores_result[i].first);
    EXPECT_EQ(-12345678, members_and_scores_result[i].second);
  }

  test_zset.Zremrangebylex("009600", false, "009700", true);

  test_zset.Zrangebylex(&members_result, "009500", true, "1000000", false);
  EXPECT_EQ(401, members_result.size());
  for (int i = 0; i < members_result.size(); i ++) {
    if (i < 101) {
      sprintf(buffer, "%06d", i + 9500);
    } else {
      sprintf(buffer, "%06d", i + 9600);
    }
    EXPECT_EQ(buffer, members_result[i]);
  }
}

TEST_P(TestZset, case_5_Zrange_Zremrangebyrank) {
  Zset<int> test_zset("test_case_5", GetParam());
  for (int i = 1; i <= 100000; i ++) {
    test_zset.Zadd(std::to_string(i).data(), i * 3 - 5);
  }

  ZSET::strs result;
  test_zset.Zrange(&result, 0, 10);
  EXPECT_EQ(10, result.size());
  for (int i = 0; i < result.size(); i ++) {
    EXPECT_EQ(std::to_string(i + 1), result[i]);
  }

  test_zset.Zrange(&result, 5050, 506);
  EXPECT_EQ(0, result.size());

  test_zset.Zremrangebyrank(11, 20);
  test_zset.Zrange(&result, 5, 25);
  EXPECT_EQ(21, result.size());
  for (int i = 0; i < result.size(); i ++) {
    if (i < 6) {
      EXPECT_EQ(std::to_string(i + 5), result[i]);
    } else {
      EXPECT_EQ(std::to_string(i + 15), result[i]);
    }
  }

  EXPECT_EQ(99990, test_zset.Zcard());
  test_zset.Zremrangebyrank(3333, 4444);
  EXPECT_EQ(98878, test_zset.Zcard());
  test_zset.Zremrangebyrank(88800, 290939);
  EXPECT_EQ(88799, test_zset.Zcard());
}

TEST_P(TestZset, case_6_Zrangebyscore_Zremrangebyscore) {
  Zset<int> test_zset("test_case_6", GetParam());
  int count = 0;
  for (int i = 100000; i > 0; i --) {
    int score = (i + 12) / 13;
    test_zset.Zadd(std::to_string(i).data(), score);
    if (score > 500 && score < 7690) {
      count ++;
    }
  }

  ZSET::pairs<int> result;
  test_zset.Zrangebyscore(&result, 5, 7);
  EXPECT_EQ(39, result.size());
  for (int i = 0; i < result.size(); i ++) {
    EXPECT_EQ(std::to_string(53 + i), result[i].first);
    EXPECT_EQ((53 + i + 12) / 13, result[i].second);
  }
  test_zset.Zremrangebyscore(7690, 8000);
  test_zset.Zremrangebyscore(-30, 500);
  EXPECT_EQ(count, test_zset.Zcard());
}

TEST_P(TestZset, case_7_Zpopmax_Zpopmin) {
  Zset<int> test_zset("test_case_7", GetParam());
  char buffer[10];
  std::unordered_map<std::string, int> std_map;
  for (int i = 1; i <= 100000; i ++) {
    int x = rand() % 256;
    int y = rand() % 256;
    sprintf(buffer, "%06d", x);
    test_zset.Zadd(buffer, y);
    std_map[buffer] = y;
  }
  CheckZset(std_map, test_zset);

  std::vector<std::pair<int, std::string>> std_result;
  for (auto& p : std_map) {
    std_result.emplace_back(p.second, p.first);
  }
  std::sort(std_result.begin(), std_result.end());
  int total = std_result.size();
  // Zpopmax 100
  ZSET::pairs<int> result;
  EXPECT_EQ(100, test_zset.Zpopmax(&result, 100));
  EXPECT_EQ(100, result.size());
  for (int i = 0; i < result.size(); i ++) {
    EXPECT_EQ(std_result[total - 1 - i].second, result[i].first);
    EXPECT_EQ(std_result[total - 1 - i].first, result[i].second);
  }
  //Zpopmin 100
  EXPECT_EQ(100, test_zset.Zpopmin(&result, 100));
  EXPECT_EQ(100, result.size());
  for (int i = 0; i < result.size(); i ++) {
    EXPECT_EQ(std_result[i].second, result[i].first);
    EXPECT_EQ(std_result[i].first, result[i].second);
  }
}

TEST_P(TestZset, case_8_Zinterstore_Zunionstore) {
  Zset<int> a("test_case_8_a", GetParam());
  Zset<int> b("test_case_8_b", GetParam());
  std::unordered_map<std::string, int> inter_map, union_map;
  char buffer[10];
  for (int i = 1; i <= 12; i ++) {
    sprintf(buffer, "%06d", i);
    if (i % 2 == 0) {
      union_map[buffer] += i;
      a.Zadd(buffer, i);
    }
    if (i % 3 == 0) {
      union_map[buffer] += i;
      b.Zadd(buffer, i);
    }
    if (i % 6 == 0) {
      inter_map[buffer] = i * 2;
    }
  }
  auto inter_zset = a.Zinterstore(&b, "test_case_8_inter", GetParam());
  CheckZset(inter_map, *inter_zset);
  auto union_zset = a.Zunionstore(&b, "test_case_8_union", GetParam());
  CheckZset(union_map, *union_zset);
}

INSTANTIATE_TEST_CASE_P(Zset, TestZset, testing::Values(ROBIN_MAP_DICT, ROCKSDB_DICT));
