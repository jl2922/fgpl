#include "hash_entry.h"

#include <gtest/gtest.h>

TEST(HashEntryTest, StringKey) {
  fgpl::internal::hash::HashEntry<std::string, double> a;
  a.fill("test", 33, 4.0);
  EXPECT_EQ(a.key, "test");
  EXPECT_EQ(a.get_hash_value(), 33);
  EXPECT_EQ(a.value, 4.0);

  EXPECT_FALSE(a.key_equal("test", 44));
}

TEST(HashEntryTest, NumericKey) {
  fgpl::internal::hash::HashEntry<int, double> a;
  a.fill(22, 3, 4.0);
  EXPECT_EQ(a.key, 22);
  EXPECT_EQ(a.get_hash_value(), std::hash<int>()(22));
  EXPECT_EQ(a.value, 4.0);

  EXPECT_TRUE(a.key_equal(22, 4));
}
