#include "node/node.h"

#include <xxhash.h>

#include <fcntl.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <algorithm>
#include <iostream>
#include <thread>
#include <vector>

#include "common/net.h"
#include "common/protocol.h"

namespace dfs {

namespace {
constexpr size_t kIoBuf = 1u << 20;  // 1MB streaming buffer
}

StorageNode::StorageNode(std::string coord_host, uint32_t coord_port,
                         uint32_t data_port, std::string data_dir,
                         bool fsync_on_write)
    : coord_host_(std::move(coord_host)),
      coord_port_(coord_port),
      data_port_(data_port),
      data_dir_(std::move(data_dir)),
      fsync_on_write_(fsync_on_write) {}

std::string StorageNode::chunk_path(const std::string& chunk_id) const {
  uint64_t h = XXH64(chunk_id.data(), chunk_id.size(), 0);
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%016llx", (unsigned long long)h);
  return data_dir_ + "/" + buf + ".chunk";
}

bool StorageNode::store_stream(int from_fd, const std::string& chunk_id,
                               uint64_t len, uint64_t* checksum) {
  std::string path = chunk_path(chunk_id);
  int ffd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (ffd < 0) return false;

  XXH64_state_t* st = XXH64_createState();
  XXH64_reset(st, 0);

  std::vector<char> buf(kIoBuf);
  uint64_t remaining = len;
  bool ok = true;
  while (remaining > 0) {
    size_t want = (size_t)std::min<uint64_t>(remaining, kIoBuf);
    if (!read_full(from_fd, buf.data(), want)) { ok = false; break; }
    XXH64_update(st, buf.data(), want);
    if (!write_file(ffd, buf.data(), want)) { ok = false; break; }
    remaining -= want;
  }
  *checksum = XXH64_digest(st);
  XXH64_freeState(st);
  if (fsync_on_write_ && ok) ::fsync(ffd);  // durable write: flush to disk
  ::close(ffd);
  if (!ok) return false;

  // checksum sidecar (durable across restart)
  int sfd = ::open((path + ".sum").c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (sfd < 0) return false;
  bool wrote = write_file(sfd, reinterpret_cast<const char*>(checksum), 8);
  if (fsync_on_write_ && wrote) ::fsync(sfd);
  ::close(sfd);
  return wrote;
}

bool StorageNode::send_chunk(int fd, const std::string& chunk_id) {
  std::string path = chunk_path(chunk_id);

  // read stored checksum
  uint64_t checksum = 0;
  {
    int sfd = ::open((path + ".sum").c_str(), O_RDONLY);
    if (sfd < 0) { send_frame(fd, MSG_GET_HDR, std::string(1, 0)); return false; }
    char c[8];
    bool ok = read_file(sfd, c, 8);
    ::close(sfd);
    if (!ok) { send_frame(fd, MSG_GET_HDR, std::string(1, 0)); return false; }
    std::memcpy(&checksum, c, 8);
  }

  int ffd = ::open(path.c_str(), O_RDONLY);
  if (ffd < 0) {
    std::string hdr;
    put_u8(hdr, 0);
    send_frame(fd, MSG_GET_HDR, hdr);
    return false;
  }

  struct stat st;
  fstat(ffd, &st);
  uint64_t len = (uint64_t)st.st_size;

  std::string hdr;
  put_u8(hdr, 1);
  put_u64(hdr, checksum);
  put_u64(hdr, len);
  if (!send_frame(fd, MSG_GET_HDR, hdr)) { ::close(ffd); return false; }

  // zero-copy: kernel moves file -> socket
  off_t off = 0;
  uint64_t remaining = len;
  while (remaining > 0) {
    ssize_t sent = ::sendfile(fd, ffd, &off, (size_t)std::min<uint64_t>(remaining, (uint64_t)1 << 30));
    if (sent <= 0) { ::close(ffd); return false; }
    remaining -= (uint64_t)sent;
  }
  ::close(ffd);
  return true;
}

void StorageNode::register_with_coordinator() {
  for (;;) {
    int fd = connect_socket(coord_host_, coord_port_);
    if (fd < 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }
    std::string req;
    put_u32(req, data_port_);
    send_frame(fd, MSG_REGISTER, req);

    uint32_t type;
    std::string resp;
    if (recv_frame(fd, type, resp) && type == MSG_REGISTER_ACK) {
      Reader r(resp);
      node_id_ = r.get_u32();
      std::cout << "[node] registered as node " << node_id_ << " on :" << data_port_ << "\n";
      ::close(fd);
      return;
    }
    ::close(fd);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
}

void StorageNode::heartbeat_loop() {
  for (;;) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    int fd = connect_socket(coord_host_, coord_port_);
    if (fd < 0) continue;
    std::string req;
    put_u32(req, node_id_);
    send_frame(fd, MSG_HEARTBEAT, req);
    ::close(fd);
  }
}

void StorageNode::handle_conn(int fd) {
  set_tcp_nodelay(fd);

  uint32_t type;
  std::string payload;
  if (!recv_frame(fd, type, payload)) {
    ::close(fd);
    return;
  }

  switch (type) {
    case MSG_PUT_HDR: {
      Reader r(payload);
      std::string chunk_id(r.get_str());
      uint64_t len = r.get_u64();

      uint64_t checksum = 0;
      bool ok = store_stream(fd, chunk_id, len, &checksum);

      std::string resp;
      put_u8(resp, ok ? 1 : 0);
      put_u64(resp, checksum);
      send_frame(fd, MSG_PUT_ACK, resp);
      break;
    }

    case MSG_GET: {
      Reader r(payload);
      std::string chunk_id(r.get_str());
      send_chunk(fd, chunk_id);
      break;
    }

    case MSG_REPLICATE: {
      Reader r(payload);
      std::string chunk_id(r.get_str());
      std::string src_addr(r.get_str());
      uint32_t src_port = r.get_u32();

      bool ok = false;

      // pull from source node, streaming straight to our own disk
      int sfd = connect_socket(src_addr, src_port);
      if (sfd >= 0) {
        std::string req;
        put_str(req, chunk_id);
        if (send_frame(sfd, MSG_GET, req)) {
          uint32_t stype;
          std::string sresp;
          if (recv_frame(sfd, stype, sresp) && stype == MSG_GET_HDR) {
            Reader sr(sresp);
            uint8_t sok = sr.get_u8();
            uint64_t schk = sr.get_u64();
            uint64_t slen = sr.get_u64();
            if (sok && slen > 0) {
              uint64_t out_chk = 0;
              ok = store_stream(sfd, chunk_id, slen, &out_chk);
              // verify we didn't copy corrupt bytes from the source
              if (ok && out_chk != schk) {
                std::cerr << "[node " << node_id_ << "] replication checksum "
                          << "mismatch for " << chunk_id << " (source corrupt)\n";
                ok = false;
              }
            }
          }
        }
        ::close(sfd);
      }

      std::string resp;
      put_str(resp, chunk_id);
      put_u8(resp, ok ? 1 : 0);
      send_frame(fd, MSG_REPLICATE_ACK, resp);
      if (ok)
        std::cout << "[node " << node_id_ << "] replicated chunk " << chunk_id << "\n";
      break;
    }

    default:
      break;
  }
  ::close(fd);
}

void StorageNode::server_loop() {
  int fd = listen_socket(data_port_);
  if (fd < 0) {
    std::cerr << "node: cannot listen on :" << data_port_ << "\n";
    return;
  }
  std::cout << "[node " << node_id_ << "] serving data on :" << data_port_ << "\n";
  for (;;) {
    int cfd = ::accept(fd, nullptr, nullptr);
    if (cfd < 0) continue;
    std::thread(&StorageNode::handle_conn, this, cfd).detach();
  }
}

void StorageNode::run() {
  ::mkdir(data_dir_.c_str(), 0755);
  register_with_coordinator();
  std::thread hb(&StorageNode::heartbeat_loop, this);
  hb.detach();
  server_loop();
}

}  // namespace dfs
