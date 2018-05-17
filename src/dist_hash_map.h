#pragma once

#include "internal/hash/dist_hash_map.h"
#include "reducer.h"

namespace fgpl {
template <class K, class V, class H = std::hash<K>>
class DistHashMap : public internal::hash::DistHashMap<K, V, H> {
 public:
  void async_set(
      const K& key,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite) {
    internal::hash::DistHashMap<K, V, H>::async_set(key, hasher(key), value, reducer);
  }

 private:
  H hasher;

  using internal::hash::DistHashMap<K, V, H>::async_set;
};
}  // namespace fgpl
