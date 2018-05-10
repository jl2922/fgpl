#pragma once

#include <functional>
#include <vector>
#include "hash_base.h"

namespace fgpl {
namespace internal {
namespace hash {

// A linear probing hash map that requires providing hash values when use.
template <class K, class H = std::hash<K>>
class HashSet : public HashBase<K, void, H> {
 public:
  void set(const K& key, const size_t hash_value);

  void for_each(const std::function<void(const K& key, const size_t hash_value)>& handler) const;

  using HashBase<K, void, H>::max_load_factor;

  using HashBase<K, void, H>::reserve_n_buckets;

 protected:
  using HashBase<K, void, H>::n_keys;

  using HashBase<K, void, H>::n_buckets;

  using HashBase<K, void, H>::buckets;

  using HashBase<K, void, H>::check_balance;
};

template <class K, class H>
void HashSet<K, H>::set(const K& key, const size_t hash_value) {
  size_t bucket_id = hash_value % n_buckets;
  size_t n_probes = 0;
  while (n_probes < n_buckets) {
    if (!buckets.at(bucket_id).filled) {
      buckets.at(bucket_id).fill(key, hash_value);
      n_keys++;
      if (n_buckets * max_load_factor <= n_keys) reserve_n_buckets(n_buckets * 2);
      break;
    } else if (buckets.at(bucket_id).key_equals(key, hash_value)) {
      break;
    } else {
      n_probes++;
      bucket_id = (bucket_id + 1) % n_buckets;
    }
  }
  check_balance(n_probes);
}

template <class K, class H>
void HashSet<K, H>::for_each(
    const std::function<void(const K& key, const size_t hash_value)>& handler) const {
  if (n_keys == 0) return;
  for (size_t i = 0; i < n_buckets; i++) {
    if (buckets.at(i).filled) {
      handler(buckets.at(i).key, buckets.at(i).hash_value);
    }
  }
}
}  // namespace hash
}  // namespace internal
}  // namespace fgpl
