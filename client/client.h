#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dfs {

struct NodeAddr {
  std::string addr;
  uint32_t    port = 0;
};

class Client {
 public:
  Client(std::string coord_host, uint32_t coord_port);

  // Uploads a local file into the DFS. Returns throughput in MB/s.
  double put(const std::string& local_path, const std::string& dfs_name,
             uint32_t chunk_size, uint32_t threads);

  // Downloads a DFS file to out_path. Returns throughput in MB/s.
  double get(const std::string& dfs_name, const std::string& out_path,
             uint32_t threads);

 private:
  std::string coord_rpc(uint32_t type, const std::string& payload);
  std::vector<NodeAddr> allocate(const std::string& chunk_id);
  std::vector<NodeAddr> lookup(const std::string& chunk_id);

  bool put_chunk(const NodeAddr& n, const std::string& chunk_id,
                 const char* data, uint64_t len, uint64_t* checksum);
  bool get_chunk(const NodeAddr& n, const std::string& chunk_id,
                 std::string* data, uint64_t* checksum);

  std::string coord_host_;
  uint32_t    coord_port_;
};

}  // namespace dfs
