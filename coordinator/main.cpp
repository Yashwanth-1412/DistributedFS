#include <cstdlib>
#include <iostream>
#include <string>

#include "coordinator/coordinator.h"

int main(int argc, char** argv) {
  uint32_t port = 9000;
  int64_t  timeout_ms = 5000;
  uint32_t rf = 2;
  std::string journal = "/tmp/dfs_coord.journal";
  if (argc > 1) port = (uint32_t)std::atoi(argv[1]);
  if (argc > 2) timeout_ms = std::atoll(argv[2]);
  if (argc > 3) rf = (uint32_t)std::atoi(argv[3]);
  if (argc > 4) journal = argv[4];

  dfs::Coordinator c(port, timeout_ms, rf, journal);
  c.run();
  return 0;
}
