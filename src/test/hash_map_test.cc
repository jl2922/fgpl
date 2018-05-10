#include "../hash_map.h"

#include <gtest/gtest.h>
#include <string>
#include <unordered_map>

TEST(HashMapTest, Initialization) {
  fgpl::HashMap<std::string, int> m;
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(HashMapTest, SetGetAndCopyConstructor) {
  fgpl::HashMap<std::string, int> m;
  m.set("aa", 1);
  EXPECT_TRUE(m.has("aa"));
  m.set("bb", 2);
  EXPECT_TRUE(m.has("bb"));

  fgpl::HashMap<std::string, int> m2(m);
  EXPECT_EQ(m2.get("aa"), 1);
  EXPECT_EQ(m2.get("bb"), 2);
}

TEST(HashMapTest, Reserve) {
  fgpl::HashMap<std::string, int> m;
  m.reserve(100);
  EXPECT_GE(m.get_n_buckets(), 100);
}

TEST(HashMapTest, LargeReserve) {
  fgpl::HashMap<std::string, int> m;
  const int N_KEYS = 1000000;
  m.reserve(N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS);
}

TEST(HashMapTest, MaxLoadFactorAndAutoRehash) {
  fgpl::HashMap<int, int> m;
  constexpr int N_KEYS = 100;
  m.max_load_factor = 0.5;
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i, i);
  }
  EXPECT_GE(m.get_n_buckets(), N_KEYS / 0.5);
}

TEST(HashMapTest, LargeSetAndGetSTLComparison) {
  constexpr long long N_KEYS = 1000000;
  std::unordered_map<long long, int> m;
  m.reserve(N_KEYS);
  for (long long i = 0; i < N_KEYS; i++) m[i * i] = i;
  for (long long i = 0; i < N_KEYS; i += 10) EXPECT_EQ(m.at(i * i), i);
}

TEST(HashMapTest, LargeSetAndGet) {
  fgpl::HashMap<long long, int> m;
  constexpr long long N_KEYS = 1000000;
  m.reserve(N_KEYS);
  for (long long i = 0; i < N_KEYS; i++) m.set(i * i, i);
  for (long long i = 0; i < N_KEYS; i += 10) EXPECT_EQ(m.get(i * i), i);
}

TEST(HashMapTest, LargeSetAndGetSTLComparisonAutoRehash) {
  constexpr long long N_KEYS = 1000000;
  std::unordered_map<long long, int> m;
  for (long long i = 0; i < N_KEYS; i++) m[i * i] = i;
  for (long long i = 0; i < N_KEYS; i += 10) EXPECT_EQ(m.at(i * i), i);
}

TEST(HashMapTest, LargeSetAndGetAutoRehash) {
  fgpl::HashMap<long long, int> m;
  constexpr long long N_KEYS = 1000000;
  for (long long i = 0; i < N_KEYS; i++) m.set(i * i, i);
  for (long long i = 0; i < N_KEYS; i += 10) EXPECT_EQ(m.get(i * i), i);
}

TEST(HashMapTest, UnsetAndHas) {
  fgpl::HashMap<std::string, int> m;
  m.set("aa", 1);
  m.set("bbb", 2);
  EXPECT_TRUE(m.has("aa"));
  EXPECT_TRUE(m.has("bbb"));
  m.unset("aa");
  EXPECT_FALSE(m.has("aa"));
  EXPECT_EQ(m.get_n_keys(), 1);

  m.unset("not_exist_key");
  EXPECT_EQ(m.get_n_keys(), 1);

  m.unset("bbb");
  EXPECT_FALSE(m.has("aa"));
  EXPECT_FALSE(m.has("bbb"));
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(HashMapTest, ClearAndShrink) {
  fgpl::HashMap<int, int> m;
  constexpr int N_KEYS = 100;
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i * i, i);
  }
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS * m.max_load_factor);
  m.clear();
  EXPECT_EQ(m.get_n_keys(), 0);
  m.clear_and_shrink();
  EXPECT_LT(m.get_n_buckets(), N_KEYS * m.max_load_factor);
}
