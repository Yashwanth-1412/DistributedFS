#include "client/client.h"

#include <xxhash.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <vector>

#include "client/thread_pool.h"
#include "common/net.h"
#include "common/protocol.h"

namespace dfs {

using Clock = std::chrono::steady_clock;

Client::Client(std::string coord_host, uint32_t coord_port)
    : coord_host_(std::move(coord_host)), coord_port_(coord_port) {}

std::string Client::coord_rpc(uint32_t type, const std::string& payload) {
  int fd = connect_socket(coord_host_, coord_port_);
  if (fd < 0) return {};
  if (!send_frame(fd, type, payload)) { ::close(fd); return {}; }
  uint32_t rtype;
  std::string resp;
  if (!recv_frame(fd, rtype, resp)) { ::close(fd); return {}; }
  ::close(fd);
  return resp;
}

std::vector<NodeAddr> Client::allocate(const std::string& chunk_id) {
  std::string req;
  put_str(req, chunk_id);
  std::string resp = coord_rpc(MSG_ALLOCATE, req);
  Reader r(resp);
  std::vector<NodeAddr> out;
  uint32_t n = r.get_u32();
  for (uint32_t i = 0; i < n; i++) {
    NodeAddr a;
    a.addr = std::string(r.get_str());
    a.port = r.get_u32();
    out.push_back(a);
  }
  return out;
}

std::vector<NodeAddr> Client::lookup(const std::string& chunk_id) {
  std::string req;
  put_str(req, chunk_id);
  std::string resp = coord_rpc(MSG_LOOKUP, req);
  Reader r(resp);
  std::vector<NodeAddr> out;
  uint32_t n = r.get_u32();
  for (uint32_t i = 0; i < n; i++) {
    NodeAddr a;
    a.addr = std::string(r.get_str());
    a.port = r.get_u32();
    out.push_back(a);
  }
  return out;
}

bool Client::put_chunk(const NodeAddr& n, const std::string& chunk_id,
                       const char* data, uint64_t len, uint64_t* checksum) {
  int fd = connect_socket(n.addr, n.port);
  if (fd < 0) return false;

  std::string hdr;
  put_str(hdr, chunk_id);
  put_u64(hdr, len);
  if (!send_frame(fd, MSG_PUT_HDR, hdr)) { ::close(fd); return false; }

  // stream straight from the mmap (no intermediate buffer)
  if (!write_full(fd, data, len)) { ::close(fd); return false; }

  uint32_t type;
  std::string resp;
  bool ok = false;
  if (recv_frame(fd, type, resp) && type == MSG_PUT_ACK) {
    Reader r(resp);
    ok = r.get_u8() == 1;
    *checksum = r.get_u64();
  }
  ::close(fd);
  return ok;
}

bool Client::get_chunk(const NodeAddr& n, const std::string& chunk_id,
                       std::string* data, uint64_t* checksum) {
  int fd = connect_socket(n.addr, n.port);
  if (fd < 0) return false;

  std::string req;
  put_str(req, chunk_id);
  if (!send_frame(fd, MSG_GET, req)) { ::close(fd); return false; }

  uint32_t type;
  std::string resp;
  if (!recv_frame(fd, type, resp) || type != MSG_GET_HDR) { ::close(fd); return false; }
  Reader r(resp);
  bool ok = r.get_u8() == 1;
  *checksum = r.get_u64();
  uint64_t len = r.get_u64();
  if (!ok) { ::close(fd); return false; }

  data->resize(len);
  if (!read_full(fd, &(*data)[0], len)) { ::close(fd); return false; }
  ::close(fd);
  return true;
}

double Client::put(const std::string& local_path, const std::string& dfs_name,
                   uint32_t chunk_size, uint32_t threads) {
  int fd = ::open(local_path.c_str(), O_RDONLY);
  if (fd < 0) {
    std::cerr << "cannot open " << local_path << "\n";
    return 0.0;
  }
  struct stat st;
  fstat(fd, &st);
  size_t file_size = (size_t)st.st_size;
  if (file_size == 0) { ::close(fd); return 0.0; }

  const char* map = (const char*)::mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
  ::close(fd);
  if (map == MAP_FAILED) {
    std::cerr << "mmap failed\n";
    return 0.0;
  }

  uint32_t num_chunks = (uint32_t)((file_size + chunk_size - 1) / chunk_size);
  auto t0 = Clock::now();

  // phase 1: allocate all chunks (metadata) in parallel
  std::vector<std::vector<NodeAddr>> replicas(num_chunks);
  {
    ThreadPool pool(threads);
    for (uint32_t i = 0; i < num_chunks; i++)
      pool.submit([&, i] { replicas[i] = allocate(dfs_name + ":" + std::to_string(i)); });
  }

  // phase 2: stream every chunk to every replica, one pool task per (chunk,replica)
  {
    ThreadPool pool(threads);
    for (uint32_t i = 0; i < num_chunks; i++) {
      for (const NodeAddr& n : replicas[i]) {
        pool.submit([&, i, n] {
          std::string chunk_id = dfs_name + ":" + std::to_string(i);
          size_t off = (size_t)i * chunk_size;
          uint64_t len = std::min<uint64_t>(chunk_size, file_size - off);
          uint64_t chk = 0;
          put_chunk(n, chunk_id, map + off, len, &chk);
        });
      }
    }
  }

  double secs = std::chrono::duration<double>(Clock::now() - t0).count();
  double mb = (double)file_size / (1024.0 * 1024.0);

  ::munmap((void*)map, file_size);
  std::cout << "[client] PUT " << local_path << " -> " << dfs_name
            << " (" << num_chunks << " chunks, " << mb << " MB) in "
            << secs << "s = " << (mb / secs) << " MB/s\n";
  return mb / secs;
}

double Client::get(const std::string& dfs_name, const std::string& out_path,
                   uint32_t threads) {
  // discover chunk count
  uint32_t num_chunks = 0;
  for (;;) {
    std::string chunk_id = dfs_name + ":" + std::to_string(num_chunks);
    std::vector<NodeAddr> replicas = lookup(chunk_id);
    if (replicas.empty()) break;
    num_chunks++;
  }
  if (num_chunks == 0) {
    std::cerr << "file not found: " << dfs_name << "\n";
    return 0.0;
  }

  int ofd = ::open(out_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (ofd < 0) {
    std::cerr << "cannot open " << out_path << " for writing\n";
    return 0.0;
  }

  // fetch one chunk, verifying its checksum against the stored value
  auto fetch = [&](uint32_t idx, std::string* data) {
    std::string chunk_id = dfs_name + ":" + std::to_string(idx);
    for (const NodeAddr& n : lookup(chunk_id)) {
      uint64_t chk = 0;
      if (get_chunk(n, chunk_id, data, &chk)) {
        if (XXH64(data->data(), data->size(), 0) == chk) return true;
        std::cerr << "[client] checksum mismatch on " << chunk_id
                  << ", trying replica\n";
      }
    }
    return false;
  };

  std::atomic<uint64_t> written{0};
  std::atomic<uint32_t> failed{0};
  uint64_t chunk_size = 0;  // set from chunk 0's size below
  auto write_chunk = [&](uint32_t idx, const std::string& data) {
    uint64_t off = (uint64_t)idx * chunk_size;
    ssize_t w = ::pwrite(ofd, data.data(), data.size(), (off_t)off);
    if (w != (ssize_t)data.size()) failed++;
    else written += data.size();
  };

  auto t0 = Clock::now();

  // learn chunk_size from chunk 0 (uniform for every chunk but the last)
  std::string d0;
  if (!fetch(0, &d0)) {
    std::cerr << "chunk 0 unavailable (no healthy replica)\n";
    ::close(ofd);
    return 0.0;
  }
  chunk_size = d0.size();
  write_chunk(0, d0);

  // stream remaining chunks straight to disk (pwrite) — no full-file buffer
  {
    ThreadPool pool(threads);
    for (uint32_t i = 1; i < num_chunks; i++) {
      pool.submit([&, i] {
        std::string data;
        if (fetch(i, &data)) write_chunk(i, data);
        else {
          std::cerr << "chunk " << i << " unavailable (no healthy replica)\n";
          failed++;
        }
      });
    }
  }

  double secs = std::chrono::duration<double>(Clock::now() - t0).count();
  uint64_t total = written.load();
  ::close(ofd);

  double mb = (double)total / (1024.0 * 1024.0);
  if (failed) std::cerr << "[client] " << failed.load() << " chunk(s) failed\n";
  std::cout << "[client] GET " << dfs_name << " -> " << out_path
            << " (" << num_chunks << " chunks, " << mb << " MB) in "
            << secs << "s = " << (mb / secs) << " MB/s\n";
  return mb / secs;
}

}  // namespace dfs
