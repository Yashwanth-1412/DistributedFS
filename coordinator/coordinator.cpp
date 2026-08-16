#include "coordinator/coordinator.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

#include "common/net.h"
#include "common/protocol.h"

namespace dfs {

using Clock = std::chrono::steady_clock;

static int64_t now_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             Clock::now().time_since_epoch())
      .count();
}

Coordinator::Coordinator(uint32_t port, int64_t heartbeat_timeout_ms,
                         uint32_t replication_factor, std::string journal_path)
    : port_(port),
      timeout_ms_(heartbeat_timeout_ms),
      rf_(replication_factor),
      journal_path_(std::move(journal_path)) {}

// ---------------------------------------------------------------------------
// Append-only journal (WAL). Every metadata mutation is written as one line:
//   REG   <node_id> <addr> <port>          -> node came online
//   CHUNK <chunk_id> <nid> <nid> ...        -> full replica list for a chunk
// On startup we replay the file to rebuild nodes_ / chunks_ / next_node_id_.
// ---------------------------------------------------------------------------

void Coordinator::journal_init() {
  journal_fd_ = ::open(journal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (journal_fd_ < 0)
    std::cerr << "[coordinator] warning: cannot open journal " << journal_path_
              << "\n";
}

void Coordinator::journal_append(const std::string& record) {
  if (journal_fd_ < 0) return;
  std::string line = record + "\n";
  const char* p = line.data();
  size_t n = line.size();
  while (n > 0) {
    ssize_t w = ::write(journal_fd_, p, n);  // regular file: use write, not send
    if (w <= 0) return;
    p += w;
    n -= (size_t)w;
  }
  ::fsync(journal_fd_);  // fsync so a crash right after won't lose the record
}

void Coordinator::journal_chunk(const std::string& chunk_id,
                                const std::vector<uint32_t>& replicas) {
  std::ostringstream oss;
  oss << "CHUNK " << chunk_id;
  for (uint32_t nid : replicas) oss << ' ' << nid;
  journal_append(oss.str());
}

void Coordinator::replay() {
  std::ifstream in(journal_path_);
  if (!in) return;

  std::string line;
  int recovered_nodes = 0, recovered_chunks = 0;
  while (std::getline(in, line)) {
    std::istringstream ss(line);
    std::string op;
    ss >> op;
    if (op == "REG") {
      uint32_t id;
      std::string addr;
      uint32_t port;
      if (ss >> id >> addr >> port) {
        NodeInfo info;
        info.addr = addr;
        info.port = port;
        info.last_beat_ms = now_ms();
        nodes_[id] = info;
        next_node_id_ = std::max(next_node_id_, id + 1);
        recovered_nodes++;
      }
    } else if (op == "CHUNK") {
      std::string cid;
      ss >> cid;
      std::vector<uint32_t> reps;
      uint32_t nid;
      while (ss >> nid) reps.push_back(nid);
      chunks_[cid] = reps;
      recovered_chunks++;
    }
  }
  in.close();

  std::cout << "[coordinator] journal replay: recovered " << recovered_nodes
            << " nodes, " << recovered_chunks << " chunks from "
            << journal_path_ << "\n";
}

void Coordinator::rebuild_chunk_counts() {
  for (auto& [id, info] : nodes_) info.num_chunks = 0;
  for (auto& [cid, reps] : chunks_)
    for (uint32_t nid : reps)
      if (nodes_.count(nid)) nodes_[nid].num_chunks++;
}

std::vector<uint32_t> Coordinator::pick_nodes(uint32_t n, uint32_t exclude) {
  // least-loaded live nodes first
  std::vector<uint32_t> ids;
  for (auto& [id, info] : nodes_)
    if (!info.dead && id != exclude) ids.push_back(id);

  std::sort(ids.begin(), ids.end(), [&](uint32_t a, uint32_t b) {
    return nodes_[a].num_chunks < nodes_[b].num_chunks;
  });

  if (ids.size() > n) ids.resize(n);
  return ids;
}

void Coordinator::handle_client(int fd) {
  uint32_t type;
  std::string payload;
  while (recv_frame(fd, type, payload)) {
    switch (type) {
      case MSG_REGISTER: {
        Reader r(payload);
        uint32_t data_port = r.get_u32();
        std::string addr = peer_addr(fd);
        uint32_t id;
        {
          std::lock_guard<std::mutex> lk(mu_);
          id = next_node_id_++;
          NodeInfo info;
          info.addr = addr;
          info.port = data_port;
          info.last_beat_ms = now_ms();
          nodes_[id] = info;
        }
        std::ostringstream oss;
        oss << "REG " << id << ' ' << addr << ' ' << data_port;
        journal_append(oss.str());
        std::string resp;
        put_u32(resp, id);
        send_frame(fd, MSG_REGISTER_ACK, resp);
        std::cout << "[coordinator] node " << id << " registered at " << addr
                  << ":" << data_port << "\n";
        break;
      }

      case MSG_HEARTBEAT: {
        Reader r(payload);
        uint32_t id = r.get_u32();
        std::lock_guard<std::mutex> lk(mu_);
        auto it = nodes_.find(id);
        if (it != nodes_.end()) {
          it->second.last_beat_ms = now_ms();
          if (it->second.dead) {
            it->second.dead = false;
            std::cout << "[coordinator] node " << id << " revived\n";
          }
        }
        break;
      }

      case MSG_ALLOCATE: {
        Reader r(payload);
        std::string chunk_id(r.get_str());
        std::vector<uint32_t> chosen;
        {
          std::lock_guard<std::mutex> lk(mu_);
          auto it = chunks_.find(chunk_id);
          if (it != chunks_.end()) {
            chosen = it->second;
          } else {
            chosen = pick_nodes(rf_);
            for (uint32_t nid : chosen) nodes_[nid].num_chunks++;
            chunks_[chunk_id] = chosen;
            journal_chunk(chunk_id, chosen);
          }
        }
        std::string resp;
        put_u32(resp, (uint32_t)chosen.size());
        for (uint32_t nid : chosen) {
          NodeInfo info;
          { std::lock_guard<std::mutex> lk(mu_); info = nodes_[nid]; }
          put_str(resp, info.addr);
          put_u32(resp, info.port);
        }
        send_frame(fd, MSG_ALLOCATE_RESP, resp);
        break;
      }

      case MSG_LOOKUP: {
        Reader r(payload);
        std::string chunk_id(r.get_str());
        std::vector<uint32_t> chosen;
        {
          std::lock_guard<std::mutex> lk(mu_);
          auto it = chunks_.find(chunk_id);
          if (it != chunks_.end()) chosen = it->second;
        }
        std::string resp;
        put_u32(resp, (uint32_t)chosen.size());
        for (uint32_t nid : chosen) {
          NodeInfo info;
          { std::lock_guard<std::mutex> lk(mu_); info = nodes_[nid]; }
          put_str(resp, info.addr);
          put_u32(resp, info.port);
        }
        send_frame(fd, MSG_LOOKUP_RESP, resp);
        break;
      }

      default:
        break;
    }
  }
  ::close(fd);
}

void Coordinator::trigger_repair(uint32_t dead_node) {
  std::lock_guard<std::mutex> lk(mu_);
  for (auto& [chunk_id, replicas] : chunks_) {
    bool affected = false;
    for (uint32_t nid : replicas)
      if (nid == dead_node) { affected = true; break; }
    if (!affected) continue;

    // surviving live replicas
    std::vector<uint32_t> live;
    for (uint32_t nid : replicas)
      if (nid != dead_node && !nodes_[nid].dead) live.push_back(nid);
    if (live.empty()) {
      std::cout << "[coordinator] chunk " << chunk_id
                << " lost all replicas!\n";
      continue;
    }
    uint32_t source = live[0];

    // pick a replacement node not already holding this chunk
    uint32_t target = UINT32_MAX;
    for (auto& [nid, info] : nodes_) {
      if (info.dead) continue;
      bool holds = false;
      for (uint32_t r : replicas)
        if (r == nid) { holds = true; break; }
      if (!holds) {
        if (target == UINT32_MAX || nodes_[nid].num_chunks < nodes_[target].num_chunks)
          target = nid;
      }
    }
    if (target == UINT32_MAX) continue;

    // Do NOT mutate metadata here: enqueue the job and only commit (metadata +
    // journal) once the repair thread confirms the copy succeeded via ack.
    RepairJob job{chunk_id, dead_node, target, source};
    {
      std::lock_guard<std::mutex> lkq(repair_mu_);
      repair_q_.push(job);
    }
    std::cout << "[coordinator] scheduling repair: chunk " << chunk_id
              << " node " << dead_node << " -> " << target << "\n";
  }
}

void Coordinator::repair_loop() {
  for (;;) {
    RepairJob job;
    {
      std::lock_guard<std::mutex> lk(repair_mu_);
      if (repair_q_.empty()) {
        // no condition variable: simple poll
      } else {
        job = repair_q_.front();
        repair_q_.pop();
      }
    }
    // re-check outside lock to avoid holding it while sleeping
    bool have = !job.chunk_id.empty();
    if (!have) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      continue;
    }

    NodeInfo target, source;
    {
      std::lock_guard<std::mutex> lk(mu_);
      auto t = nodes_.find(job.target_node);
      auto s = nodes_.find(job.source_node);
      if (t == nodes_.end() || s == nodes_.end()) continue;
      target = t->second;
      source = s->second;
    }

    int fd = connect_socket(target.addr, target.port);
    if (fd < 0) {
      std::cout << "[coordinator] repair: cannot reach node " << job.target_node << "\n";
      continue;
    }
    std::string req;
    put_str(req, job.chunk_id);
    put_str(req, source.addr);
    put_u32(req, source.port);
    send_frame(fd, MSG_REPLICATE, req);

    // wait for the target node to confirm it copied the chunk before committing
    uint32_t rtype;
    std::string rpayload;
    bool ack_ok = false;
    if (recv_frame(fd, rtype, rpayload) && rtype == MSG_REPLICATE_ACK) {
      Reader rr(rpayload);
      std::string ack_chunk(rr.get_str());
      uint8_t ok = rr.get_u8();
      ack_ok = (ack_chunk == job.chunk_id) && (ok == 1);
    }
    ::close(fd);

    if (ack_ok) {
      std::lock_guard<std::mutex> lk(mu_);
      auto it = chunks_.find(job.chunk_id);
      if (it != chunks_.end()) {
        for (uint32_t& r : it->second)
          if (r == job.dead_node) r = job.target_node;
        nodes_[job.target_node].num_chunks++;
        journal_chunk(job.chunk_id, it->second);
      }
      std::cout << "[coordinator] repair committed: chunk " << job.chunk_id
                << " now on node " << job.target_node << "\n";
    } else {
      std::cout << "[coordinator] repair FAILED for chunk " << job.chunk_id
                << " (source node " << job.source_node << ")\n";
    }
  }
}

void Coordinator::monitor_loop() {
  for (;;) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    std::vector<uint32_t> dead;
    {
      std::lock_guard<std::mutex> lk(mu_);
      int64_t t = now_ms();
      for (auto& [id, info] : nodes_) {
        if (!info.dead && (t - info.last_beat_ms) > timeout_ms_) {
          info.dead = true;
          dead.push_back(id);
        }
      }
    }
    for (uint32_t id : dead) {
      std::cout << "[coordinator] node " << id << " marked DEAD\n";
      trigger_repair(id);
    }
  }
}

void Coordinator::run() {
  std::cout << std::unitbuf;  // flush every log line (we get killed in demos)
  journal_init();
  replay();
  rebuild_chunk_counts();

  int fd = listen_socket(port_);
  if (fd < 0) {
    std::cerr << "coordinator: cannot listen on port " << port_ << "\n";
    return;
  }
  std::cout << "[coordinator] listening on :" << port_
            << " (rf=" << rf_ << ", timeout=" << timeout_ms_ << "ms)\n";

  std::thread monitor(&Coordinator::monitor_loop, this);
  std::thread repair(&Coordinator::repair_loop, this);
  monitor.detach();
  repair.detach();

  for (;;) {
    int cfd = ::accept(fd, nullptr, nullptr);
    if (cfd < 0) continue;
    std::thread(&Coordinator::handle_client, this, cfd).detach();
  }
}

}  // namespace dfs
