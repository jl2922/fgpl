#include <mpi.h>
#include <omp.h>
#include <functional>
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
    if (verbose) printf("\n");
  }

 private:
  T start;

  T end;

  T inc;
};
}  // namespace fgpl
