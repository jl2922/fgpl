#include <hps/src/hps.h>
#include <climits>
#include "internal/mpi_type.h"
#include "internal/mpi_util.h"

namespace fgpl {

template <class T>
void broadcast(T& t) {
  size_t count;
  char* buffer;
  const bool is_master = internal::MpiUtil::is_master();
  if (is_master) {
    const std::string& serialized = hps::to_string(T);
    buffer = reinterpret_cast<char*>(serialized.data());
    count = sizeof(buffer);
  }
  MPI_Bcast(&count, 1, internal::MpiType<size_t>::value, 0, MPI_COMM_WORLD);
  if (!is_master) buffer = new char[count];
  char* buffer_ptr = buffer;
  const int TRUNK_SIZE = 1 << 30;
  while (count > TRUNK_SIZE) {
    MPI_Bcast(buffer_ptr, TRUNK_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);
    buffer_ptr += TRUNK_SIZE;
    count -= TRUNK_SIZE;
  }
  MPI_Bcast(buffer_ptr, count, MPI_CHAR, 0, MPI_COMM_WORLD);
  if (!is_master) {
    hps::from_char_array(buffer, t);
    delete[] buffer;
  }
}

template <>
void broadcast(char*& buffer) {
  size_t count;
  const bool is_master = internal::MpiUtil::is_master();
  if (is_master) {
    count = sizeof(buffer);
  }
  MPI_Bcast(&count, 1, internal::MpiType<size_t>::value, 0, MPI_COMM_WORLD);
  if (!is_master) {
    delete[] buffer;
    buffer = new char[count];
  }
  char* buffer_ptr = buffer;
  const int TRUNK_SIZE = 1 << 30;
  while (count > TRUNK_SIZE) {
    MPI_Bcast(buffer_ptr, TRUNK_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);
    buffer_ptr += TRUNK_SIZE;
    count -= TRUNK_SIZE;
  }
  MPI_Bcast(buffer_ptr, count, MPI_CHAR, 0, MPI_COMM_WORLD);
}
}  // namespace fgpl
