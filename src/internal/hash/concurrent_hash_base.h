#pragma once

#include <omp.h>
#include <functional>
#include <numeric>
#include <vector>

namespace fgpl {
namespace internal {
namespace hash {

// A concurrent map that requires providing hash values when use.
template <class K, class V, class S, class H = std::hash<K>>
class ConcurrentHashBase {
 public:
  ConcurrentHashBase();

  ConcurrentHashBase(const ConcurrentHashBase& m);

  ~ConcurrentHashBase();

  void reserve(const size_t n_keys_min);

  void set_max_load_factor(const float max_load_factor);

  float get_max_load_factor() const { return max_load_factor; };

  size_t get_n_keys() const;

  size_t get_n_buckets() const;

  float get_load_factor();

  void unset(const K& key, const size_t hash_value);

  bool has(const K& key, const size_t hash_value);

  void clear();

  void clear_and_shrink();

  std::string to_string();

  void from_string(const std::string& str);

 protected:
  size_t n_segments;

  std::vector<S> segments;

  size_t n_threads;

  std::vector<S> thread_caches;

  std::vector<omp_lock_t> segment_locks;

 private:
  float max_load_factor;

  bool has_big_prime_factors(const int num);
};

template <class K, class V, class S, class H>
ConcurrentHashBase<K, V, S, H>::ConcurrentHashBase() {
  max_load_factor = S::DEFAULT_MAX_LOAD_FACTOR;
  n_threads = omp_get_max_threads();
  thread_caches.resize(n_threads);
  n_segments = 4;
  while (n_segments < n_threads) n_segments <<= 1;
  n_segments <<= 2;
  segments.resize(n_segments);
  segment_locks.resize(n_segments);
  for (auto& lock : segment_locks) omp_init_lock(&lock);
}

template <class K, class V, class S, class H>
ConcurrentHashBase<K, V, S, H>::ConcurrentHashBase(const ConcurrentHashBase& m) {
  max_load_factor = m.max_load_factor;
  n_threads = omp_get_max_threads();
  thread_caches.resize(n_threads);
  n_segments = m.n_segments;
  segments = m.segments;
  segment_locks.resize(n_segments);
  for (auto& lock : segment_locks) omp_init_lock(&lock);
}

template <class K, class V, class S, class H>
ConcurrentHashBase<K, V, S, H>::~ConcurrentHashBase() {
  for (auto& lock : segment_locks) omp_destroy_lock(&lock);
}

template <class K, class V, class S, class H>
void ConcurrentHashBase<K, V, S, H>::reserve(const size_t n_keys_min) {
  const size_t n_segment_keys_min = n_keys_min / n_segments;
  for (size_t i = 0; i < n_segments; i++) segments.at(i).reserve(n_segment_keys_min);
  const size_t n_thread_keys_est = n_keys_min / 1000;
  for (size_t i = 0; i < n_threads; i++) thread_caches.at(i).reserve(n_thread_keys_est);
};

template <class K, class V, class S, class H>
void ConcurrentHashBase<K, V, S, H>::set_max_load_factor(const float max_load_factor) {
  this->max_load_factor = max_load_factor;
  for (size_t i = 0; i < n_segments; i++) segments.at(i).max_load_factor = max_load_factor;
  for (size_t i = 0; i < n_threads; i++) thread_caches.at(i).max_load_factor = max_load_factor;
}

template <class K, class V, class S, class H>
size_t ConcurrentHashBase<K, V, S, H>::get_n_keys() const {
  size_t n_keys = 0;
  for (size_t i = 0; i < n_segments; i++) n_keys += segments.at(i).get_n_keys();
  return n_keys;
}

template <class K, class V, class S, class H>
size_t ConcurrentHashBase<K, V, S, H>::get_n_buckets() const {
  size_t n_buckets = 0;
  for (size_t i = 0; i < n_segments; i++) n_buckets += segments.at(i).get_n_buckets();
  return n_buckets;
}

template <class K, class V, class S, class H>
void ConcurrentHashBase<K, V, S, H>::unset(const K& key, const size_t hash_value) {
  const size_t segment_id = hash_value % n_segments;
  auto& lock = segment_locks[segment_id];
  omp_set_lock(&lock);
  segments.at(segment_id).unset(key, hash_value);
  omp_unset_lock(&lock);
}

template <class K, class V, class S, class H>
bool ConcurrentHashBase<K, V, S, H>::has(const K& key, const size_t hash_value) {
  const size_t segment_id = hash_value % n_segments;
  auto& lock = segment_locks[segment_id];
  omp_set_lock(&lock);
  bool res = segments.at(segment_id).has(key, hash_value);
  omp_unset_lock(&lock);
  return res;
}

template <class K, class V, class S, class H>
void ConcurrentHashBase<K, V, S, H>::clear() {
  for (size_t i = 0; i < n_segments; i++) segments.at(i).clear();
  for (size_t i = 0; i < n_threads; i++) thread_caches.at(i).clear();
}

template <class K, class V, class S, class H>
void ConcurrentHashBase<K, V, S, H>::clear_and_shrink() {
  for (size_t i = 0; i < n_segments; i++) segments.at(i).clear_and_shrink();
  for (size_t i = 0; i < n_threads; i++) thread_caches.at(i).clear_and_shrink();
}

// template <class K, class V, class S, class H>
// std::string ConcurrentHashBase<K, V, S, H>::to_string() {
//   std::vector<std::string> ostrs(n_segments);
//   size_t total_size = 0;
// #pragma omp parallel for
//   for (size_t i = 0; i < n_segments; i++) {
//     auto& lock = segment_locks[i];
//     omp_set_lock(&lock);
//     hps::serialize_to_string(segments.at(i), ostrs.at(i));
//     omp_unset_lock(&lock);
// #pragma omp atomic
//     total_size += ostrs[i].size();
//   }
//   std::string str;
//   str.reserve(total_size + n_segments * 8);
//   hps::OutputBuffer<std::string> ob_str(str);
//   hps::Serializer<float, std::string>::serialize(max_load_factor, ob_str);
//   for (size_t i = 0; i < n_segments; i++) {
//     hps::Serializer<std::string, std::string>::serialize(ostrs[i], ob_str);
//   }
//   ob_str.flush();
//   return str;
// }

// template <class K, class V, class S, class H>
// void ConcurrentHashBase<K, V, S, H>::from_string(const std::string& str) {
//   std::vector<std::string> istrs(n_segments);
//   hps::InputBuffer<std::string> ib_str(str);
//   hps::Serializer<float, std::string>::parse(max_load_factor, ib_str);
//   for (size_t i = 0; i < n_segments; i++) {
//     hps::Serializer<std::string, std::string>::parse(istrs[i], ib_str);
//   }
// #pragma omp parallel for
//   for (size_t i = 0; i < n_segments; i++) {
//     auto& lock = segment_locks[i];
//     omp_set_lock(&lock);
//     hps::parse_from_string(segments.at(i), istrs[i]);
//     omp_unset_lock(&lock);
//   }
// }
}  // namespace hash
}  // namespace internal
}  // namespace fgpl
