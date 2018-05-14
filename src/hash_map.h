#pragma once

#include "internal/hash/hash_map.h"
#include "reducer.h"

namespace fgpl {
template <class K, class V, class H = std::hash<K>>
class HashMap : public internal::hash::HashMap<K, V, H> {
 public:
  void set(
      const K& key,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite) {
    internal::hash::HashMap<K, V, H>::set(key, hasher(key), value, reducer);
  }

  V get(const K& key, const V& default_value = V()) const {
    return internal::hash::HashMap<K, V, H>::get(key, hasher(key), default_value);
  }

  void unset(const K& key) { internal::hash::HashMap<K, V, H>::unset(key, hasher(key)); }

  bool has(const K& key) { return internal::hash::HashMap<K, V, H>::has(key, hasher(key)); }

 private:
  H hasher;

  using internal::hash::HashMap<K, V, H>::set;

  using internal::hash::HashMap<K, V, H>::get;

  using internal::hash::HashMap<K, V, H>::unset;

  using internal::hash::HashMap<K, V, H>::has;
};
}  // namespace fgpl
