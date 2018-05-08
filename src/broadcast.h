#include <hps/src/hps.h>
#include <climits>
#include "internal/mpi_type.h"
#include "internal/mpi_util.h"

namespace fgpl {

// Broadcast objects of any type and any size.
template <class T>
void broadcast(T& t) {
  size_t count;
  char* buffer;
  const bool is_master = internal::MpiUtil::is_master();

  if (is_master) {
    const std::string& serialized = hps::to_string(t);
    printf("serialized 0: %u\n", serialized[0]);
    buffer = const_cast<char*>(serialized.data());
    printf("buffer 0: %u\n", buffer[0]);
    count = serialized.size();
  }

  MPI_Bcast(&count, 1, internal::MpiType<size_t>::value, 0, MPI_COMM_WORLD);
  printf("cnt %zu\n", count);
  if (!is_master) buffer = new char[count];
  char* buffer_ptr = buffer;
  const int TRUNK_SIZE = 1 << 30;
  while (count > TRUNK_SIZE) {
    MPI_Bcast(buffer_ptr, TRUNK_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);
    buffer_ptr += TRUNK_SIZE;
    count -= TRUNK_SIZE;
  }
  MPI_Bcast(buffer_ptr, count, MPI_CHAR, 0, MPI_COMM_WORLD);

  printf("buffer 0: %u\n", buffer[0]);
  if (!is_master) {
    hps::from_char_array(buffer, t);
    delete[] buffer;
  }
}
}  // namespace fgpl
