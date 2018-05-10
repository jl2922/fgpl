#pragma once

#include <vector>
#include "hash_entry.h"

namespace fgpl {
namespace internal {
namespace hash {

// A linear probing hash container base.
template <class K, class V, class H = std::hash<K>>
class HashBase {
 public:
  constexpr static float DEFAULT_MAX_LOAD_FACTOR = 0.7;

  constexpr static size_t N_INITIAL_BUCKETS = 11;

  constexpr static size_t MAX_N_PROBES = 64;

  float max_load_factor;

  HashBase();

  size_t get_n_keys() const { return n_keys; }

  size_t get_n_buckets() const { return n_buckets; }

  void reserve(const size_t n_keys_min);

  void reserve_n_buckets(const size_t n_buckets_min);

  void unset(const K& key, const size_t hash_value);

  bool has(const K& key, const size_t hash_value) const;

  void clear();

  void clear_and_shrink();

 protected:
  size_t n_keys;

  size_t n_buckets;

  std::vector<HashEntry<K, V>> buckets;

  void check_balance(const size_t n_probes);

 private:
  bool unbalanced_warned;

  size_t get_n_rehash_buckets(const size_t n_buckets_min);

  void rehash(const size_t n_rehash_buckets);
};

template <class K, class V, class H>
HashBase<K, V, H>::HashBase() {
  n_keys = 0;
  n_buckets = N_INITIAL_BUCKETS;
  buckets.resize(N_INITIAL_BUCKETS);
  max_load_factor = DEFAULT_MAX_LOAD_FACTOR;
  unbalanced_warned = false;
}

template <class K, class V, class H>
void HashBase<K, V, H>::reserve(const size_t n_keys_min) {
  reserve_n_buckets(n_keys_min / max_load_factor);
}

template <class K, class V, class H>
void HashBase<K, V, H>::reserve_n_buckets(const size_t n_buckets_min) {
  if (n_buckets_min <= n_buckets) return;
  const size_t n_rehash_buckets = get_n_rehash_buckets(n_buckets_min);
  rehash(n_rehash_buckets);
}

template <class K, class V, class H>
size_t HashBase<K, V, H>::get_n_rehash_buckets(const size_t n_buckets_min) {
  constexpr size_t PRIMES[] = {
      11, 17, 29, 47, 79, 127, 211, 337, 547, 887, 1433, 2311, 3739, 6053, 9791, 15859};
  constexpr size_t N_PRIMES = sizeof(PRIMES) / sizeof(size_t);
  constexpr size_t LAST_PRIME = PRIMES[N_PRIMES - 1];
  constexpr size_t BIG_PRIME = PRIMES[N_PRIMES - 5];
  size_t remaining_factor = n_buckets_min + n_buckets_min / 4;
  size_t n_rehash_buckets = 1;
  while (remaining_factor > LAST_PRIME) {
    remaining_factor /= BIG_PRIME;
    n_rehash_buckets *= BIG_PRIME;
  }

  // Find a prime larger than or equal to the remaining factor with binary search.
  size_t left = 0, right = N_PRIMES - 1;
  while (left < right) {
    size_t mid = (left + right) / 2;
    if (PRIMES[mid] < remaining_factor) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  n_rehash_buckets *= PRIMES[left];
  return n_rehash_buckets;
}

template <class K, class V, class H>
void HashBase<K, V, H>::rehash(const size_t n_rehash_buckets) {
  std::vector<HashEntry<K, V>> rehash_buckets(n_rehash_buckets);
  for (size_t i = 0; i < n_buckets; i++) {
    if (!buckets.at(i).filled) continue;
    const size_t hash_value = buckets.at(i).hash_value;
    size_t rehash_bucket_id = hash_value % n_rehash_buckets;
    size_t n_probes = 0;
    while (n_probes < n_rehash_buckets) {
      if (!rehash_buckets.at(rehash_bucket_id).filled) {
        rehash_buckets.at(rehash_bucket_id) = buckets.at(i);
        break;
      } else {
        n_probes++;
        rehash_bucket_id = (rehash_bucket_id + 1) % n_rehash_buckets;
      }
    }
  }
  buckets = std::move(rehash_buckets);
  n_buckets = n_rehash_buckets;
}

template <class K, class V, class H>
void HashBase<K, V, H>::check_balance(const size_t n_probes) {
  if (n_probes > MAX_N_PROBES) {
    if (n_keys < n_buckets / 4 && !unbalanced_warned) {
      fprintf(stderr, "Warning: Hash container is unbalanced!\n");
      unbalanced_warned = true;
    }
    if (n_keys < n_buckets / 16) {
      throw std::runtime_error("Hash container is severely unbalanced.");
    }
    reserve_n_buckets(n_buckets * 2);
  }
}

template <class K, class V, class H>
void HashBase<K, V, H>::unset(const K& key, const size_t hash_value) {
  size_t bucket_id = hash_value % n_buckets;
  size_t n_probes = 0;
  while (n_probes < n_buckets) {
    if (!buckets.at(bucket_id).filled) {
      return;
    } else if (buckets.at(bucket_id).key_equals(key, hash_value)) {
      buckets.at(bucket_id).filled = false;
      n_keys--;
      // Find a valid entry to fill the spot if exists.
      size_t swap_bucket_id = (bucket_id + 1) % n_buckets;
      while (buckets.at(swap_bucket_id).filled) {
        const size_t swap_origin_id = buckets.at(swap_bucket_id).hash_value % n_buckets;
        if ((swap_bucket_id < swap_origin_id && swap_origin_id <= bucket_id) ||
            (swap_origin_id <= bucket_id && bucket_id < swap_bucket_id) ||
            (bucket_id < swap_bucket_id && swap_bucket_id < swap_origin_id)) {
          buckets.at(bucket_id) = buckets.at(swap_bucket_id);
          buckets.at(swap_bucket_id).filled = false;
          bucket_id = swap_bucket_id;
        }
        swap_bucket_id = (swap_bucket_id + 1) % n_buckets;
      }
      return;
    } else {
      n_probes++;
      bucket_id = (bucket_id + 1) % n_buckets;
    }
  }
}

template <class K, class V, class H>
bool HashBase<K, V, H>::has(const K& key, const size_t hash_value) const {
  size_t bucket_id = hash_value % n_buckets;
  size_t n_probes = 0;
  while (n_probes < n_buckets) {
    if (!buckets.at(bucket_id).filled) {
      return false;
    } else if (buckets.at(bucket_id).key_equals(key, hash_value)) {
      return true;
    } else {
      n_probes++;
      bucket_id = (bucket_id + 1) % n_buckets;
    }
  }
  return false;
}

template <class K, class V, class H>
void HashBase<K, V, H>::clear() {
  if (n_keys == 0) return;
  for (size_t i = 0; i < n_buckets; i++) {
    buckets.at(i).filled = false;
  }
  n_keys = 0;
}

template <class K, class V, class H>
void HashBase<K, V, H>::clear_and_shrink() {
  buckets.resize(N_INITIAL_BUCKETS);
  n_buckets = N_INITIAL_BUCKETS;
  clear();
}
}  // namespace hash
}  // namespace internal
}  // namespace fgpl
