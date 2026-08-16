#pragma once

#include <sys/socket.h>

#include <cstdint>
#include <string>

namespace dfs {

// Create a listening TCP socket bound to localhost:port. Returns fd or -1.
int listen_socket(uint32_t port);

// Connect to host:port. Returns fd or -1.
int connect_socket(const std::string& host, uint32_t port);

// Dotted-decimal IP of the remote peer of an accepted/connected socket.
std::string peer_addr(int fd);

// Disable Nagle's algorithm (matters for small header/ack frames and for
// accepted sockets, which do not inherit TCP_NODELAY from the listener).
void set_tcp_nodelay(int fd);

// Read exactly n bytes (blocking). Returns false on EOF/error.
bool read_full(int fd, char* buf, size_t n);

// Write exactly n bytes. Returns false on error.
bool write_full(int fd, const char* buf, size_t n);

// Regular-file I/O (read_full/write_full use recv/send, which fail on files).
bool read_file(int fd, char* buf, size_t n);
bool write_file(int fd, const char* buf, size_t n);

// Send a length-prefixed frame: [u32 payload_len][u32 type][payload].
bool send_frame(int fd, uint32_t type, const std::string& payload);

// Receive a frame. Returns false on error. On success sets type + payload.
bool recv_frame(int fd, uint32_t& type, std::string& payload);

}  // namespace dfs
