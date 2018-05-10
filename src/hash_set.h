#pragma once

#include "internal/hash/hash_set.h"
#include "reducer.h"

namespace fgpl {
template <class K, class H = std::hash<K>>
class HashSet : public internal::hash::HashSet<K, H> {
 public:
  void set(const K& key) { internal::hash::HashSet<K, H>::set(key, hasher(key)); }

  void unset(const K& key) { internal::hash::HashSet<K, H>::unset(key, hasher(key)); }

  bool has(const K& key) const { return internal::hash::HashSet<K, H>::has(key, hasher(key)); }

 private:
  H hasher;
};
}  // namespace fgpl
