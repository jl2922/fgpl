#pragma once

#include "internal/hash/concurrent_hash_set.h"

namespace fgpl {

template <class K, class H = std::hash<K>>
class ConcurrentHashSet : public internal::hash::ConcurrentHashSet<K, H> {
 public:
  void set(const K& key) { internal::hash::ConcurrentHashSet<K, H>::set(key, hasher(key)); }

  void async_set(const K& key) {
    internal::hash::ConcurrentHashSet<K, H>::async_set(key, hasher(key));
  }

  void unset(const K& key) { internal::hash::ConcurrentHashSet<K, H>::unset(key, hasher(key)); }

  bool has(const K& key) { return internal::hash::ConcurrentHashSet<K, H>::has(key, hasher(key)); }

 private:
  H hasher;
};

}  // namespace fgpl
