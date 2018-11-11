#include "../dist_hash_map.h"

#include <gtest/gtest.h>
#include <string>
#include <fstream>
#include <iostream>
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

TEST(DistHashMapTest, Mapreduce) {
  const long long N_KEYS = 100;
  fgpl::DistHashMap<long long, long long> ds;
  fgpl::DistRange<long long> range(0, N_KEYS);
  range.for_each([&](const long long i) { ds.async_set(i * i, i); });
  ds.sync();
  long long sum = ds.mapreduce<long long>(
      [&](const long long key, const long long) { return key; }, fgpl::Reducer<long long>::sum, 0);
  EXPECT_EQ(sum, N_KEYS * (N_KEYS - 1) * (2 * N_KEYS - 1) / 6);
}

TEST(DistHashMapTest, MapreduceShakes) {
  std::ifstream file("/home/ec2-user/g1");
  std::vector<std::string> lines;
  lines.reserve(1e8);
  std::string line;
  while (std::getline(file, line)) {
    lines.push_back(line);
  }
  std::cout << lines.size() << std::endl;
  fgpl::DistRange<int> range(0, lines.size());
  fgpl::DistHashMap<std::string, int> dm;
  dm.reserve(50000);
  range.for_each([&](const int i) {
    const std::string& line = lines[i];
    std::string word;
    for (char c : line) {
      if (c != ' ') {
        word.push_back(c);
      } else if (!word.empty()) {
        dm.async_set(word, 1, fgpl::Reducer<int>::sum);
        word.clear();
      }
    }
    if (!word.empty()) {
      dm.async_set(word, 1, fgpl::Reducer<int>::sum);
    }
  });
  dm.sync(fgpl::Reducer<int>::sum);
  std::cout << dm.get_n_keys() << std::endl;
}
