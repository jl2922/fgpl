#include "../concurrent_hash_set.h"

#include <gtest/gtest.h>
#include <string>
#include <unordered_set>
#include "../../vendor/hps/src/hps.h"
#include "../hash_set.h"

TEST(ConcurrentHashSetTest, Initialization) {
  fgpl::ConcurrentHashSet<std::string> m;
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(ConcurrentHashSetTest, Reserve) {
  fgpl::ConcurrentHashSet<std::string> m;
  m.reserve(1000);
  EXPECT_GE(m.get_n_buckets(), 1000);
}

TEST(ConcurrentHashSetTest, CopyConstructor) {
  fgpl::ConcurrentHashSet<std::string> m;
  m.set("aa");
  EXPECT_TRUE(m.has("aa"));
  m.set("bb");
  EXPECT_TRUE(m.has("bb"));

  fgpl::ConcurrentHashSet<std::string> m2(m);
  EXPECT_TRUE(m2.has("aa"));
  EXPECT_TRUE(m2.has("bb"));
}

TEST(ConcurrentHashSetTest, LargeReserve) {
  fgpl::ConcurrentHashSet<std::string> m;
  const size_t LARGE_N_BUCKETS = 1000000;
  m.reserve(LARGE_N_BUCKETS);
  const size_t n_buckets = m.get_n_buckets();
  EXPECT_GE(n_buckets, LARGE_N_BUCKETS);
}

TEST(ConcurrentHashSetTest, GetAndSetLoadFactor) {
  fgpl::ConcurrentHashSet<int> m;
  constexpr int N_KEYS = 100;
  m.set_max_load_factor(0.5);
  EXPECT_EQ(m.get_max_load_factor(), 0.5);
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i);
  }
  EXPECT_GE(m.get_n_buckets(), N_KEYS / 0.5);
}

TEST(ConcurrentHashSetTest, SetAndGet) {
  fgpl::ConcurrentHashSet<std::string> m;
  m.set("aa");
  EXPECT_TRUE(m.has("aa"));
  m.set("aa");
  EXPECT_TRUE(m.has("aa"));
  m.set("cc");
  EXPECT_TRUE(m.has("cc"));
}

TEST(ConcurrentHashSetTest, LargeParallelSetIndependentSTLComparison) {
  const int n_threads = omp_get_max_threads();
  std::vector<std::unordered_set<long long>> m(n_threads);
  constexpr int N_KEYS = 1000000;
#pragma omp parallel for
  for (long long i = 0; i < N_KEYS; i++) {
    const int thread_id = omp_get_thread_num();
    m[thread_id].insert(i * i);
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

TEST(ConcurrentHashSetTest, LargeParallelSetIndependentComparison) {
  const int n_threads = omp_get_max_threads();
  std::vector<fgpl::HashSet<long long>> m(n_threads);
  constexpr long long N_KEYS = 1000000;
#pragma omp parallel for
  for (long long i = 0; i < N_KEYS; i++) {
    const int thread_id = omp_get_thread_num();
    m[thread_id].set(i * i);
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

TEST(ConcurrentHashSetTest, LargeParallelSet) {
  fgpl::ConcurrentHashSet<long long> m;
  constexpr long long N_KEYS = 1000000;
#pragma omp parallel for
  for (long long i = 0; i < N_KEYS; i++) {
    m.set(i * i);
  }
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS);
}

TEST(ConcurrentHashSetTest, LargeParallelAsyncSet) {
  fgpl::ConcurrentHashSet<long long> m;
  constexpr long long N_KEYS = 1000000;
#pragma omp parallel for
  for (long long i = 0; i < N_KEYS; i++) {
    m.async_set(i * i);
  }
  m.sync();
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS);
}

TEST(ConcurrentHashSetTest, UnsetAndHas) {
  fgpl::ConcurrentHashSet<std::string> m;
  m.set("aa");
  m.set("bbb");
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

TEST(ConcurrentHashSetTest, Clear) {
  fgpl::ConcurrentHashSet<std::string> m;
  m.set("aa");
  m.set("bbb");
  EXPECT_EQ(m.get_n_keys(), 2);
  m.clear();
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(ConcurrentHashSetTest, ClearAndShrink) {
  fgpl::ConcurrentHashSet<long long> m;
  constexpr long long N_KEYS = 1000000;
#pragma omp parallel for schedule(static, 1)
  for (long long i = 0; i < N_KEYS; i++) {
    m.set(i * i);
  }
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS * m.get_max_load_factor());
  m.clear_and_shrink();
  EXPECT_EQ(m.get_n_keys(), 0);
  EXPECT_LT(m.get_n_buckets(), N_KEYS * m.get_max_load_factor());
}

TEST(ConcurrentHashSetTest, SerializeAndParse) {
  fgpl::ConcurrentHashSet<long long> m;
  m.set(0);
  m.set(1);
  const auto& serialized = hps::to_string(m);
  auto parsed = hps::from_string<fgpl::ConcurrentHashSet<long long>>(serialized);
  EXPECT_EQ(parsed.get_n_keys(), 2);
  EXPECT_TRUE(parsed.has(0));
  EXPECT_TRUE(parsed.has(1));
}
