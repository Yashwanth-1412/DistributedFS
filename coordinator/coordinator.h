#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

namespace dfs {

struct NodeInfo {
  std::string addr;
  uint32_t    port = 0;
  int64_t     last_beat_ms = 0;
  bool        dead = false;
  int         num_chunks = 0;
};

class Coordinator {
 public:
  Coordinator(uint32_t port, int64_t heartbeat_timeout_ms,
              uint32_t replication_factor, std::string journal_path);

  void run();

 private:
  struct RepairJob {
    std::string chunk_id;
    uint32_t    dead_node;
    uint32_t    target_node;
    uint32_t    source_node;
  };

  void handle_client(int fd);
  void monitor_loop();
  void repair_loop();
  void trigger_repair(uint32_t dead_node);

  std::vector<uint32_t> pick_nodes(uint32_t n, uint32_t exclude = UINT32_MAX);

  // ---- append-only journal (WAL) ----
  void journal_init();
  void journal_append(const std::string& record);
  void replay();
  void rebuild_chunk_counts();
  void journal_chunk(const std::string& chunk_id,
                     const std::vector<uint32_t>& replicas);

  uint32_t port_;
  int64_t  timeout_ms_;
  uint32_t rf_;
  std::string journal_path_;
  int         journal_fd_ = -1;

  std::mutex mu_;
  std::map<uint32_t, NodeInfo> nodes_;
  std::map<std::string, std::vector<uint32_t>> chunks_;
  uint32_t next_node_id_ = 1;

  std::queue<RepairJob> repair_q_;
  std::mutex repair_mu_;
};

}  // namespace dfs
