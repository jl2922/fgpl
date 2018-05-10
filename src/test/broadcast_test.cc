#include "../broadcast.h"

#include <gtest/gtest.h>
#include <string>
#include <unordered_map>

TEST(BroadcastTest, Integer) {
  int a = 0;
  if (fgpl::internal::MpiUtil::is_master()) {
    a = 3;
  }
  fgpl::broadcast(a);
  EXPECT_EQ(a, 3);
}

TEST(BroadcastTest, UnorderedMap) {
  std::unordered_map<std::string, int> a;
  if (fgpl::internal::MpiUtil::is_master()) {
    a["three"] = 3;
  }
  fgpl::broadcast(a);
  EXPECT_EQ(a["three"], 3);
}
