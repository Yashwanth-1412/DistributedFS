#include "common/net.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

namespace dfs {

static int set_common_opts(int fd) {
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  return fd;
}

void set_tcp_nodelay(int fd) {
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

int listen_socket(uint32_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons((uint16_t)port);

  set_common_opts(fd);
  if (::bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) { ::close(fd); return -1; }
  if (::listen(fd, 128) < 0) { ::close(fd); return -1; }
  return fd;
}

int connect_socket(const std::string& host, uint32_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)port);
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) { ::close(fd); return -1; }

  set_common_opts(fd);
  if (::connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) { ::close(fd); return -1; }
  return fd;
}

std::string peer_addr(int fd) {
  sockaddr_in addr{};
  socklen_t len = sizeof(addr);
  if (getpeername(fd, (sockaddr*)&addr, &len) != 0) return "127.0.0.1";
  char buf[INET_ADDRSTRLEN];
  inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));
  return std::string(buf);
}

bool read_full(int fd, char* buf, size_t n) {
  size_t got = 0;
  while (got < n) {
    ssize_t r = ::recv(fd, buf + got, n - got, 0);
    if (r <= 0) return false;
    got += (size_t)r;
  }
  return true;
}

bool write_full(int fd, const char* buf, size_t n) {
  size_t sent = 0;
  while (sent < n) {
    ssize_t w = ::send(fd, buf + sent, n - sent, MSG_NOSIGNAL);
    if (w <= 0) return false;
    sent += (size_t)w;
  }
  return true;
}

bool read_file(int fd, char* buf, size_t n) {
  size_t got = 0;
  while (got < n) {
    ssize_t r = ::read(fd, buf + got, n - got);
    if (r <= 0) return false;
    got += (size_t)r;
  }
  return true;
}

bool write_file(int fd, const char* buf, size_t n) {
  size_t sent = 0;
  while (sent < n) {
    ssize_t w = ::write(fd, buf + sent, n - sent);
    if (w <= 0) return false;
    sent += (size_t)w;
  }
  return true;
}

bool send_frame(int fd, uint32_t type, const std::string& payload) {
  char hdr[8];
  uint32_t len = (uint32_t)payload.size();
  std::memcpy(hdr, &len, 4);
  std::memcpy(hdr + 4, &type, 4);
  if (!write_full(fd, hdr, 8)) return false;
  if (len && !write_full(fd, payload.data(), len)) return false;
  return true;
}

bool recv_frame(int fd, uint32_t& type, std::string& payload) {
  char hdr[8];
  if (!read_full(fd, hdr, 8)) return false;
  uint32_t len, t;
  std::memcpy(&len, hdr, 4);
  std::memcpy(&t, hdr + 4, 4);
  type = t;
  payload.resize(len);
  if (len && !read_full(fd, &payload[0], len)) return false;
  return true;
}

}  // namespace dfs
