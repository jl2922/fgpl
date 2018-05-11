#pragma once

#include <functional>
#include "concurrent_hash_base.h"
#include "hash_set.h"

namespace fgpl {
namespace internal {
namespace hash {

template <class K, class H = std::hash<K>>
class ConcurrentHashSet : public ConcurrentHashBase<K, void, HashSet<K, H>, H> {
 public:
  void set(const K& key, const size_t hash_value);

  void async_set(const K& key, const size_t hash_value);

  void sync();

  void for_each_serial(const std::function<void(const K& key, const size_t hash_value)>& handler);

 protected:
  using ConcurrentHashBase<K, void, HashSet<K, H>, H>::n_segments;

  using ConcurrentHashBase<K, void, HashSet<K, H>, H>::segments;

  using ConcurrentHashBase<K, void, HashSet<K, H>, H>::segment_locks;

  using ConcurrentHashBase<K, void, HashSet<K, H>, H>::thread_caches;
};

template <class K, class H>
void ConcurrentHashSet<K, H>::set(const K& key, const size_t hash_value) {
  const size_t segment_id = hash_value % n_segments;
  auto& lock = segment_locks[segment_id];
  omp_set_lock(&lock);
  segments.at(segment_id).set(key, hash_value);
  omp_unset_lock(&lock);
}

template <class K, class H>
void ConcurrentHashSet<K, H>::async_set(const K& key, const size_t hash_value) {
  const size_t segment_id = hash_value % n_segments;
  auto& lock = segment_locks[segment_id];
  if (omp_test_lock(&lock)) {
    segments.at(segment_id).set(key, hash_value);
    omp_unset_lock(&lock);
  } else {
    const int thread_id = omp_get_thread_num();
    thread_caches.at(thread_id).set(key, hash_value);
  }
}

template <class K, class H>
void ConcurrentHashSet<K, H>::sync() {
#pragma omp parallel
  {
    const int thread_id = omp_get_thread_num();
    const auto& handler = [&](const K& key, const size_t hash_value) {
      const size_t segment_id = hash_value % n_segments;
      auto& lock = segment_locks[segment_id];
      omp_set_lock(&lock);
      segments.at(segment_id).set(key, hash_value);
      omp_unset_lock(&lock);
    };
    thread_caches.at(thread_id).for_each(handler);
    thread_caches.at(thread_id).clear();
  }
}

template <class K, class H>
void ConcurrentHashSet<K, H>::for_each_serial(
    const std::function<void(const K& key, const size_t hash_value)>& handler) {
  for (size_t segment_id = 0; segment_id < n_segments; segment_id++) {
    segments[segment_id].for_each(handler);
  }
}

}  // namespace hash
}  // namespace internal
}  // namespace fgpl
