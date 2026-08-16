#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "client/client.h"

static void usage(const char* p) {
  fprintf(stderr,
          "usage:\n"
          "  %s <host> <port> put  <local_file> <dfs_name> [chunk_size] [threads]\n"
          "  %s <host> <port> get  <dfs_name> <out_file> [threads]\n"
          "  %s <host> <port> bench <size_mb> <chunk_size> <threads> <clients>\n",
          p, p, p);
}

int main(int argc, char** argv) {
  if (argc < 4) { usage(argv[0]); return 1; }
  std::string host = argv[1];
  uint32_t port = (uint32_t)std::atoi(argv[2]);
  std::string cmd = argv[3];

  if (cmd == "put") {
    if (argc < 6) { usage(argv[0]); return 1; }
    std::string local = argv[4];
    std::string name = argv[5];
    uint32_t chunk_size = (argc > 6) ? (uint32_t)std::atoi(argv[6]) : (4u << 20);
    uint32_t threads = (argc > 7) ? (uint32_t)std::atoi(argv[7]) : 8;
    dfs::Client c(host, port);
    c.put(local, name, chunk_size, threads);
  } else if (cmd == "get") {
    if (argc < 6) { usage(argv[0]); return 1; }
    std::string name = argv[4];
    std::string out = argv[5];
    uint32_t threads = (argc > 6) ? (uint32_t)std::atoi(argv[6]) : 8;
    dfs::Client c(host, port);
    c.get(name, out, threads);
  } else if (cmd == "bench") {
    if (argc < 8) { usage(argv[0]); return 1; }
    uint64_t size_mb = (uint64_t)std::atoi(argv[4]);
    uint32_t chunk_size = (uint32_t)std::atoi(argv[5]);
    uint32_t threads = (uint32_t)std::atoi(argv[6]);
    uint32_t clients = (uint32_t)std::atoi(argv[7]);

    std::string tmp = "/tmp/dfs_bench.dat";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { perror("bench temp"); return 1; }
    // sparse file of requested size
    ::ftruncate(fd, (off_t)(size_mb * 1024 * 1024));
    ::close(fd);

    std::vector<std::thread> ts;
    double put_mbps = 0.0, get_mbps = 0.0;
    for (uint32_t k = 0; k < clients; k++) {
      ts.emplace_back([&, k] {
        dfs::Client c(host, port);
        std::string name = "bench_client" + std::to_string(k);
        put_mbps += c.put(tmp, name, chunk_size, threads / clients);
        get_mbps += c.get(name, "/tmp/dfs_bench_out" + std::to_string(k), threads / clients);
      });
    }
    for (auto& t : ts) t.join();

    std::cout << "== BENCH aggregate ==\n";
    std::cout << "  PUT: " << put_mbps << " MB/s across " << clients << " clients\n";
    std::cout << "  GET: " << get_mbps << " MB/s across " << clients << " clients\n";
  } else {
    usage(argv[0]);
    return 1;
  }
  return 0;
}
