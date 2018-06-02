#include "../concurrent_vector.h"

#include <gtest/gtest.h>

TEST(ConcurrentVectorTest, ResizeAndForEach) {
  fgpl::ConcurrentVector<double> vec;
  vec.resize(10, 1.0);
  double sum = 0.0;
  vec.for_each_serial([&](const size_t, const double value) { sum += value; });
  EXPECT_NEAR(sum, 10.0, 1.0e-10);

  vec.set(3, 2.0); 
  sum = 0.0;
  vec.for_each_serial([&](const size_t, const double value) { sum += value; });
  EXPECT_NEAR(sum, 11.0, 1.0e-10);
}
