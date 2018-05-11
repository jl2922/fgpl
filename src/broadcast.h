#include <hps/src/hps.h>
#include "internal/mpi_type.h"
#include "internal/mpi_util.h"

namespace fgpl {

// Broadcast objects of any type and any size.
template <class T>
void broadcast(T& t, const int root = 0) {
  size_t count;
  char* buffer = nullptr;
  std::string serialized;
  const int proc_id = internal::MpiUtil::get_proc_id();
  const bool is_master = (proc_id == root);

  if (is_master) {
    hps::to_string(t, serialized);
    count = serialized.size();
    buffer = const_cast<char*>(serialized.data());
  }

  MPI_Bcast(&count, 1, internal::MpiType<size_t>::value, root, MPI_COMM_WORLD);
  if (!is_master) buffer = new char[count];
  char* buffer_ptr = buffer;
  const int TRUNK_SIZE = 1 << 30;
  while (count > TRUNK_SIZE) {
    MPI_Bcast(buffer_ptr, TRUNK_SIZE, MPI_CHAR, root, MPI_COMM_WORLD);
    buffer_ptr += TRUNK_SIZE;
    count -= TRUNK_SIZE;
  }
  MPI_Bcast(buffer_ptr, count, MPI_CHAR, root, MPI_COMM_WORLD);

  if (!is_master) {
    hps::from_char_array(buffer, t);
    delete[] buffer;
  }
}
}  // namespace fgpl
