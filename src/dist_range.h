#pragma once

#include <mpi.h>
#include <omp.h>
#include <fstream>
#include <functional>
#include <iostream>
#include <vector>

#include "dist_hash_map.h"
#include "gather.h"
#include "internal/mpi_util.h"

namespace fgpl {
template <class T>
class DistRange {
 public:
  DistRange(const T start, const T end, const T inc = 1) : start(start), end(end), inc(inc) {}

  void for_each(const std::function<void(const T)>& handler, const bool verbose = false) {
    const int n_procs = internal::MpiUtil::get_n_procs();
    const int proc_id = internal::MpiUtil::get_proc_id();
    double target_progress = 0.1;
#pragma omp parallel for schedule(dynamic, 5)
    for (T t = start + inc * proc_id; t < end; t += inc * n_procs) {
      handler(t);
      const double current_progress = static_cast<double>(t - start) / (end - start);
      const int thread_id = omp_get_thread_num();
      if (thread_id != 0 || !verbose) continue;
      while (target_progress < current_progress) {
        printf("%.0f%% ", target_progress * 100);
        target_progress += 0.1;
      }
    }
    if (verbose) {
      while (target_progress <= 1.0) {
        printf("%.0f%% ", target_progress * 100);
        target_progress += 0.1;
      }
      printf("\n");
    }
  }

  template <class K, class V, class H>
  void mapreduce(
      const std::function<void(const T value, const std::function<void(const K&, const V&)>& emit)>&
          mapper,
      const std::function<void(V&, const V&)>& reducer,
      DistHashMap<K, V, H>& dm) {
    const auto& emit = [&](const K& key, const V& value) {
      dm.async_set(key, value, reducer);
    };
    for_each([&](const T t) { mapper(t, emit); });
    dm.sync(reducer);
  }

  template <class T2>
  T2 mapreduce(
      const std::function<T2(const T value)>& mapper,
      const std::function<void(T2&, const T2&)>& reducer,
      const T2& default_value) {
    const int n_threads = omp_get_max_threads();
    std::vector<T2> res_thread(n_threads, default_value);
    for_each([&](const T t) {
      const int thread_id = omp_get_thread_num();
      reducer(res_thread[thread_id], mapper(t));
    });
    T2 res_local;
    T2 res;
    res_local = res_thread[0];
    for (int i = 1; i < n_threads; i++) res_local += res_thread[i];
    std::vector<T2> res_locals = gather(res_local);
    res = res_locals[0];
    const int n_procs = internal::MpiUtil::get_n_procs();
    for (int i = 1; i < n_procs; i++) reducer(res, res_locals[i]);
    return res;
  }

 private:
  T start;

  T end;

  T inc;
};
}  // namespace fgpl
