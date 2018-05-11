#include <mpi.h>

#include <functional>
#include "internal/mpi_util.h"

namespace fgpl {
template <class T>
class DistRange {
 public:
  DistRange(const T start, const T end, const T inc = 1) : start(start), end(end), inc(inc) {}

  void for_each(const std::function<void(const T)>& handler) {
    const int n_procs = internal::MpiUtil::get_n_procs();
    const int proc_id = internal::MpiUtil::get_proc_id();
#pragma omp parallel for schedule(dynamic, 5)
    for (T t = start + proc_id; t < end; t += inc * n_procs) {
      handler(t);
    }
  }

 private:
  T start;

  T end;

  T inc;
};
}  // namespace fgpl
