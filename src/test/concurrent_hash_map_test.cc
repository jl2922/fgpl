#include "../concurrent_hash_map.h"

#include <gtest/gtest.h>
#include <string>
#include <unordered_map>
#include "../../vendor/hps/src/hps.h"
#include "../hash_map.h"

TEST(ConcurrentHashMapTest, Initialization) {
  fgpl::ConcurrentHashMap<std::string, int> m;
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(ConcurrentHashMapTest, Reserve) {
  fgpl::ConcurrentHashMap<std::string, int> m;
  m.reserve(1000);
  EXPECT_GE(m.get_n_buckets(), 1000);
}

TEST(ConcurrentHashMapTest, CopyConstructor) {
  fgpl::ConcurrentHashMap<std::string, int> m;
  m.set("aa", 1);
  EXPECT_TRUE(m.has("aa"));
  m.set("bb", 2);
  EXPECT_TRUE(m.has("bb"));

  fgpl::ConcurrentHashMap<std::string, int> m2(m);
  EXPECT_TRUE(m2.has("aa"));
  EXPECT_TRUE(m2.has("bb"));
}

TEST(ConcurrentHashMapTest, LargeReserve) {
  fgpl::ConcurrentHashMap<std::string, int> m;
  const size_t LARGE_N_BUCKETS = 1000000;
  m.reserve(LARGE_N_BUCKETS);
  const size_t n_buckets = m.get_n_buckets();
  EXPECT_GE(n_buckets, LARGE_N_BUCKETS);
}

TEST(ConcurrentHashMapTest, GetAndSetLoadFactor) {
  fgpl::ConcurrentHashMap<int, int> m;
  constexpr int N_KEYS = 100;
  m.set_max_load_factor(0.5);
  EXPECT_EQ(m.get_max_load_factor(), 0.5);
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i, i);
  }
  EXPECT_GE(m.get_n_buckets(), N_KEYS / 0.5);
}

TEST(ConcurrentHashMapTest, SetAndGet) {
  fgpl::ConcurrentHashMap<std::string, int> m;
  m.set("aa", 1);
  EXPECT_TRUE(m.has("aa"));
  m.set("aa", 2);
  EXPECT_TRUE(m.has("aa"));
  m.set("cc", 3);
  EXPECT_TRUE(m.has("cc"));
}

TEST(ConcurrentHashMapTest, LargeParallelSetIndependentSTLComparison) {
  const int n_threads = omp_get_max_threads();
  std::vector<std::unordered_map<long long, long long>> m(n_threads);
  constexpr int N_KEYS = 1000000;
#pragma omp parallel for
  for (long long i = 0; i < N_KEYS; i++) {
    const int thread_id = omp_get_thread_num();
    m[thread_id][i * i] = i;
  }
  int n_keys = 0;
  int n_buckets = 0;
  for (int i = 0; i < n_threads; i++) {
    n_keys += m[i].size();
    n_buckets += m[i].bucket_count();
  }
  EXPECT_EQ(n_keys, N_KEYS);
  EXPECT_GE(n_buckets, N_KEYS);
}

TEST(ConcurrentHashMapTest, LargeParallelSetIndependentComparison) {
  const int n_threads = omp_get_max_threads();
  std::vector<fgpl::HashMap<long long, long long>> m(n_threads);
  constexpr long long N_KEYS = 1000000;
#pragma omp parallel for
  for (long long i = 0; i < N_KEYS; i++) {
    const int thread_id = omp_get_thread_num();
    m[thread_id].set(i * i, i);
  }
  int n_keys = 0;
  int n_buckets = 0;
  for (int i = 0; i < n_threads; i++) {
    n_keys += m[i].get_n_keys();
    n_buckets += m[i].get_n_buckets();
  }
  EXPECT_EQ(n_keys, N_KEYS);
  EXPECT_GE(n_buckets, N_KEYS);
}

TEST(ConcurrentHashMapTest, LargeParallelSet) {
  fgpl::ConcurrentHashMap<long long, long long> m;
  constexpr long long N_KEYS = 1000000;
#pragma omp parallel for
  for (long long i = 0; i < N_KEYS; i++) {
    m.set(i * i, i);
  }
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS);
}

TEST(ConcurrentHashMapTest, LargeParallelAsyncSet) {
  fgpl::ConcurrentHashMap<long long, long long> m;
  constexpr long long N_KEYS = 1000000;
#pragma omp parallel for
  for (long long i = 0; i < N_KEYS; i++) {
    m.async_set(i * i, i);
  }
  m.sync();
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS);
}

TEST(ConcurrentHashMapTest, UnsetAndHas) {
  fgpl::ConcurrentHashMap<std::string, int> m;
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

TEST(ConcurrentHashMapTest, Clear) {
  fgpl::ConcurrentHashMap<std::string, int> m;
  m.set("aa", 0);
  m.set("bbb", 1);
  EXPECT_EQ(m.get_n_keys(), 2);
  m.clear();
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(ConcurrentHashMapTest, ClearAndShrink) {
  fgpl::ConcurrentHashMap<long long, long long> m;
  constexpr long long N_KEYS = 1000000;
#pragma omp parallel for schedule(static, 1)
  for (long long i = 0; i < N_KEYS; i++) {
    m.set(i * i, i);
  }
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS * m.get_max_load_factor());
  m.clear_and_shrink();
  EXPECT_EQ(m.get_n_keys(), 0);
  EXPECT_LT(m.get_n_buckets(), N_KEYS * m.get_max_load_factor());
}

TEST(ConcurrentHashMapTest, SerializeAndParse) {
  fgpl::ConcurrentHashMap<long long, long long> m;
  m.set(0, 0);
  m.set(1, 1);
  const auto& serialized = hps::to_string(m);
  auto parsed = hps::from_string<fgpl::ConcurrentHashMap<long long, long long>>(serialized);
  EXPECT_EQ(parsed.get_n_keys(), 2);
  EXPECT_TRUE(parsed.has(0));
  EXPECT_TRUE(parsed.has(1));
}
