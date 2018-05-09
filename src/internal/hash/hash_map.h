#pragma once

#include <functional>
#include <vector>
#include "../reducer.h"
#include "hash_base.h"

namespace fgpl {
namespace internal {
namespace hash {

// A linear probing hash map that requires providing hash values when use.
template <class K, class V, class H = std::hash<K>>
class HashMap : public HashBase<K, V, H> {
 public:
  void set(
      const K& key,
      const size_t hash_value,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = hpmr::Reducer<V>::overwrite);

  V get(const K& key, const size_t hash_value, const V& default_value = V()) const;

  void for_each(const std::function<void(const K& key, const size_t hash_value, const V& value)>&
                    handler) const;

  using HashBase<K, V, H>::max_load_factor;

  using HashBase<K, V, H>::reserve_n_buckets;

 protected:
  using HashBase<K, V, H>::n_keys;

  using HashBase<K, V, H>::n_buckets;

  using HashBase<K, V, H>::buckets;

  using HashBase<K, V, H>::check_balance;
};

template <class K, class V, class H>
void HashMap<K, V, H>::set(
    const K& key,
    const size_t hash_value,
    const V& value,
    const std::function<void(V&, const V&)>& reducer) {
  size_t bucket_id = hash_value % n_buckets;
  size_t n_probes = 0;
  while (n_probes < n_buckets) {
    if (!buckets.at(bucket_id).filled) {
      buckets.at(bucket_id).fill(key, hash_value, value);
      n_keys++;
      if (n_buckets * max_load_factor <= n_keys) reserve_n_buckets(n_buckets * 2);
      break;
    } else if (buckets.at(bucket_id).key_equals(key, hash_value)) {
      reducer(buckets.at(bucket_id).value, value);
      break;
    } else {
      n_probes++;
      bucket_id = (bucket_id + 1) % n_buckets;
    }
  }
  check_balance(n_probes);
}

template <class K, class V, class H>
V HashMap<K, V, H>::get(const K& key, const size_t hash_value, const V& default_value) const {
  size_t bucket_id = hash_value % n_buckets;
  size_t n_probes = 0;
  while (n_probes < n_buckets) {
    if (!buckets.at(bucket_id).filled) {
      return default_value;
    } else if (buckets.at(bucket_id).key_equals(key, hash_value)) {
      return buckets.at(bucket_id).value;
    } else {
      n_probes++;
      bucket_id = (bucket_id + 1) % n_buckets;
    }
  }
  return default_value;
}

template <class K, class V, class H>
void HashMap<K, V, H>::for_each(
    const std::function<void(const K& key, const size_t hash_value, const V& value)>& handler)
    const {
  if (n_keys == 0) return;
  for (size_t i = 0; i < n_buckets; i++) {
    if (buckets.at(i).filled) {
      handler(buckets.at(i).key, buckets.at(i).hash_value, buckets.at(i).value);
    }
  }
}
}  // namespace hash
}  // namespace internal
}  // namespace fgpl
