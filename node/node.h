#pragma once

#include <cstdint>
#include <string>

namespace dfs {

class StorageNode {
 public:
  StorageNode(std::string coord_host, uint32_t coord_port,
              uint32_t data_port, std::string data_dir,
              bool fsync_on_write = false);

  void run();

 private:
  void register_with_coordinator();
  void heartbeat_loop();
  void server_loop();
  void handle_conn(int fd);

  // Stream `len` bytes from `from_fd` into this node's chunk file, computing
  // the xxhash as we go. Used by both client PUT and node-to-node replication.
  bool store_stream(int from_fd, const std::string& chunk_id, uint64_t len,
                    uint64_t* checksum);

  // Serve a stored chunk: send GET_HDR then sendfile() the bytes (zero-copy).
  bool send_chunk(int fd, const std::string& chunk_id);

  std::string chunk_path(const std::string& chunk_id) const;

  std::string coord_host_;
  uint32_t    coord_port_;
  uint32_t    data_port_;
  std::string data_dir_;
  bool        fsync_on_write_ = false;
  uint32_t    node_id_ = 0;
};

}  // namespace dfs
