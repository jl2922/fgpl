#pragma once

#include <omp.h>
#include <functional>
#include <numeric>
#include <vector>
#include "reducer.h"

namespace fgpl {

template <class T>
class ConcurrentVector {
 public:
  ConcurrentVector();

  ~ConcurrentVector();

  void resize(const size_t n, const T& value = T());

  void set(
      const size_t i,
      const T& value,
      const std::function<void(T&, const T&)>& reducer = Reducer<T>::overwrite);

  void for_each_serial(const std::function<void(const size_t i, const T& value)>& handler) const;

 private:
  size_t n;

  size_t n_segments;

  size_t n_segments_shift;

  std::vector<std::vector<T>> segments;

  std::vector<omp_lock_t> segment_locks;
};

template <class T>
ConcurrentVector<T>::ConcurrentVector() {
  const size_t n_threads = omp_get_max_threads();
  n_segments = 4;
  while (n_segments < n_threads) n_segments <<= 1;
  n_segments <<= 2;
  n_segments_shift = __builtin_ctzll(n_segments);
  segments.resize(n_segments);
  segment_locks.resize(n_segments);
  for (auto& lock : segment_locks) omp_init_lock(&lock);
}

template <class T>
ConcurrentVector<T>::~ConcurrentVector() {
  for (auto& lock : segment_locks) omp_destroy_lock(&lock);
}

template <class T>
void ConcurrentVector<T>::resize(const size_t n, const T& value) {
  this->n = n;
  const size_t segment_size = (n >> n_segments_shift) + 1;
  for (size_t segment_id = 0; segment_id < n_segments; segment_id++) {
    segments[segment_id].resize(segment_size, value);
  }
}

template <class T>
void ConcurrentVector<T>::set(
    const size_t i, const T& value, const std::function<void(T&, const T&)>& reducer) {
  const size_t segment_id = i & (n_segments - 1);
  const size_t elem_id = i >> n_segments_shift;
  auto& lock = segment_locks[segment_id];
  omp_set_lock(&lock);
  reducer(segments[segment_id][elem_id], value);
  omp_unset_lock(&lock);
}

template <class T>
void ConcurrentVector<T>::for_each_serial(
    const std::function<void(const size_t i, const T& value)>& handler) const {
  size_t i = 0;
  size_t segment_id = 0;
  size_t elem_id = 0;
  while (i < n) {
    handler(i, segments[segment_id][elem_id]); 
    i++;
    segment_id++;
    if (segment_id == n_segments) {
      segment_id = 0;
      elem_id++;
    }
  }
}

}  // namespace fgpl
