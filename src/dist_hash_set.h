#pragma once

#include "internal/hash/dist_hash_set.h"
#include "reducer.h"

namespace fgpl {
template <class K, class H = std::hash<K>>
class DistHashSet : public internal::hash::DistHashSet<K, H> {
 public:
  void async_set(const K& key) { internal::hash::DistHashSet<K, H>::async_set(key, hasher(key)); }

 private:
  H hasher;

  using internal::hash::DistHashSet<K, H>::async_set;
};
}  // namespace fgpl
