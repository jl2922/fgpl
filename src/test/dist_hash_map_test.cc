#include "../dist_hash_map.h"

#include <gtest/gtest.h>
#include <string>
#include "../dist_range.h"

TEST(DistHashMapTest, AsyncSetAndSyncTest) {
  const long long N_KEYS = 100;
  fgpl::DistHashMap<long long, long long> ds;
  fgpl::DistRange<long long> range(0, N_KEYS);
  long long sum = 0;
  range.for_each([&](const long long i) { ds.async_set(i * i, i); });
  ds.sync();
  ds.for_each_serial([&](const long long i, const size_t, const long long) { sum += i; });
  EXPECT_EQ(sum, N_KEYS * (N_KEYS - 1) * (2 * N_KEYS - 1) / 6);
}

TEST(DistHashMapTest, ForEach) {
  const long long N_KEYS = 100;
  fgpl::DistHashMap<long long, long long> ds;
  fgpl::DistRange<long long> range(0, N_KEYS);
  long long sum = 0;
  range.for_each([&](const long long i) { ds.async_set(i * i, i); });
  ds.sync();
  ds.for_each([&](const long long i, const size_t, const long long) {
#pragma omp atomic
    sum += i;
  });
  long long sum_global = 0;
  MPI_Allreduce(&sum, &sum_global, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
  EXPECT_EQ(sum_global, N_KEYS * (N_KEYS - 1) * (2 * N_KEYS - 1) / 6);
}
