#include "../dist_hash_set.h"

#include <gtest/gtest.h>
#include <string>
#include <unordered_set>
#include "../dist_range.h"

TEST(DistHashTest, AsyncSetAndSyncTest) {
  const long long N_KEYS = 100;
  fgpl::DistHashSet<long long> ds;
  fgpl::DistRange<long long> range(0, N_KEYS);
  long long sum = 0;
  range.for_each([&](const long long i) { ds.async_set(i * i); });
  ds.sync();
  ds.for_each_serial([&](const long long i, const size_t) { sum += i; });
  EXPECT_EQ(sum, N_KEYS * (N_KEYS - 1) * (2 * N_KEYS - 1) / 6);
}
