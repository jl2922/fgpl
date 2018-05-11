#include "../gather.h"

#include <gtest/gtest.h>
#include <string>
#include <unordered_map>

TEST(GatherTest, Integer) {
  int a = fgpl::internal::MpiUtil::get_proc_id();
  const std::vector<int> res = fgpl::gather(a);
  const int n_procs = fgpl::internal::MpiUtil::get_n_procs();
  for (int i = 0; i < n_procs; i++) {
    EXPECT_EQ(res[i], i);
  }
}

TEST(GatherTest, UnorderedMap) {
  std::unordered_map<std::string, int> a;
  a["proc_id"] = fgpl::internal::MpiUtil::get_proc_id();
  const auto& res = fgpl::gather(a);
  const int n_procs = fgpl::internal::MpiUtil::get_n_procs();
  for (int i = 0; i < n_procs; i++) {
    EXPECT_EQ(res[i].size(), 1);
    EXPECT_EQ(res[i].find("proc_id")->second, i);
  }
}
