#pragma once

#include "internal/hash/concurrent_hash_map.h"
#include "reducer.h"

namespace fgpl {

template <class K, class V, class H = std::hash<K>>
class ConcurrentHashMap : public internal::hash::ConcurrentHashMap<K, V, H> {
 public:
  void set(
      const K& key,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite) {
    internal::hash::ConcurrentHashMap<K, V, H>::set(key, hasher(key), value, reducer);
  }

  void async_set(
      const K& key,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite) {
    internal::hash::ConcurrentHashMap<K, V, H>::async_set(key, hasher(key), value, reducer);
  }

  void unset(const K& key) { internal::hash::ConcurrentHashMap<K, V, H>::unset(key, hasher(key)); }

  bool has(const K& key) {
    return internal::hash::ConcurrentHashMap<K, V, H>::has(key, hasher(key));
  }

 private:
  H hasher;

  using internal::hash::ConcurrentHashMap<K, V, H>::set;

  using internal::hash::ConcurrentHashMap<K, V, H>::async_set;

  using internal::hash::ConcurrentHashMap<K, V, H>::unset;

  using internal::hash::ConcurrentHashMap<K, V, H>::has;
};

}  // namespace fgpl
