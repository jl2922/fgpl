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
  std::string serialized;
  const bool is_master = internal::MpiUtil::is_master();

  if (is_master) {
    serialized = hps::to_string(t, serialized);
    count = serialized.size();
    printf("serialized 0: %u\n", serialized[0]);
    buffer = const_cast<char*>(serialized.data());
    printf("buffer 0: %u\n", buffer[0]);
  }

  MPI_Bcast(&count, 1, internal::MpiType<size_t>::value, 0, MPI_COMM_WORLD);
  printf("cnt %zu\n", count);
  printf("buffer 0: %u\n", buffer[0]);
  if (!is_master) buffer = new char[count];
  printf("buffer 0: %u\n", buffer[0]);
  char* buffer_ptr = buffer;
  printf("buffer 0: %u\n", buffer[0]);
  const int TRUNK_SIZE = 1 << 30;
  while (count > TRUNK_SIZE) {
    MPI_Bcast(buffer_ptr, TRUNK_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);
    buffer_ptr += TRUNK_SIZE;
    count -= TRUNK_SIZE;
  }
  printf("buffer 0: %u\n", buffer[0]);
  MPI_Bcast(buffer_ptr, count, MPI_CHAR, 0, MPI_COMM_WORLD);

  printf("buffer 0: %u\n", buffer[0]);
  if (!is_master) {
    hps::from_char_array(buffer, t);
    delete[] buffer;
  }
}
}  // namespace fgpl
