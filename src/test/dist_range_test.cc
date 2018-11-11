#include "../dist_range.h"
#include "../reducer.h"

#include <gtest/gtest.h>

TEST(DistRangeTest, ForEachTest) {
  const long long N_KEYS = 1000;
  fgpl::DistRange<long long> range(0, N_KEYS);
  long long sum_local = 0;
  const auto& handler = [&](const long long i) {
    EXPECT_GE(i, 0);
    EXPECT_LT(i, N_KEYS);
#pragma omp atomic
    sum_local += i;
  };
  range.for_each(handler);
  long long sum = 0;
  MPI_Allreduce(&sum_local, &sum, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
  EXPECT_EQ(sum, N_KEYS * (N_KEYS - 1) / 2);
}

TEST(DistRangeTest, MapreduceTest) {
  const int N_SAMPLES = 1000000;
  fgpl::DistRange<int> range(0, N_SAMPLES);
  const auto& mapper = [&](const int) {
    double x = double(rand()) / RAND_MAX;
    double y = double(rand()) / RAND_MAX;
    if (x * x + y * y < 1) return 1;
    return 0;
  };
  int cnt = range.mapreduce<int>(mapper, fgpl::Reducer<int>::sum, 0);
  EXPECT_NEAR(4.0 * cnt / N_SAMPLES, 3.1416, 0.1);
}
