#include <cstdlib>
#include <string>

#include "node/node.h"

int main(int argc, char** argv) {
  if (argc < 5) {
    fprintf(stderr, "usage: dfs_node <coord_host> <coord_port> <data_port> <data_dir> [fsync]\n");
    return 1;
  }
  std::string coord_host = argv[1];
  uint32_t coord_port = (uint32_t)std::atoi(argv[2]);
  uint32_t data_port = (uint32_t)std::atoi(argv[3]);
  std::string data_dir = argv[4];
  bool fsync_on_write = (argc > 5) && (std::atoi(argv[5]) == 1);

  dfs::StorageNode node(coord_host, coord_port, data_port, data_dir, fsync_on_write);
  node.run();
  return 0;
}
