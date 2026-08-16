#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace dfs {

enum MsgType : uint32_t {
  MSG_REGISTER      = 1,   // node -> coord:  port(u32)
  MSG_REGISTER_ACK  = 2,   // coord -> node:  node_id(u32)
  MSG_HEARTBEAT     = 3,   // node -> coord:  node_id(u32)
  MSG_ALLOCATE      = 4,   // client -> coord: chunk_id(str)
  MSG_ALLOCATE_RESP = 5,   // coord -> client: count(u32), [addr(str),port(u32)]*
  MSG_LOOKUP        = 6,   // client -> coord: chunk_id(str)
  MSG_LOOKUP_RESP   = 7,   // coord -> client: count(u32), [addr(str),port(u32)]*
  MSG_REPLICATE     = 8,   // coord -> node:  chunk_id(str), src_addr(str), src_port(u32)
  MSG_REPLICATE_ACK = 9,   // node -> coord:  chunk_id(str), ok(byte)

  // Streaming chunk transfer. Data travels raw after the header frame, so the
  // chunk bytes are never copied into a framed message buffer.
  MSG_PUT_HDR       = 20,  // client -> node: chunk_id(str), len(u64)   [then raw bytes]
  MSG_PUT_ACK       = 21,  // node -> client: ok(byte), checksum(u64)
  MSG_GET           = 22,  // client -> node: chunk_id(str)
  MSG_GET_HDR       = 23,  // node -> client: ok(byte), checksum(u64), len(u64) [then raw bytes]
};

// ---- little-endian append helpers ----
inline void put_u8(std::string& s, uint8_t v)   { s.push_back(char(v)); }
inline void put_u32(std::string& s, uint32_t v) { char b[4]; std::memcpy(b, &v, 4); s.append(b, 4); }
inline void put_u64(std::string& s, uint64_t v) { char b[8]; std::memcpy(b, &v, 8); s.append(b, 8); }
inline void put_str(std::string& s, std::string_view v) { put_u32(s, (uint32_t)v.size()); s += v; }
inline void put_bytes(std::string& s, const char* p, uint32_t n) { put_u32(s, n); s.append(p, n); }

// ---- cursor-based reader ----
struct Reader {
  const char* p;
  uint32_t    n;
  Reader(std::string_view s) : p(s.data()), n((uint32_t)s.size()) {}

  uint8_t  get_u8()  { if (n < 1) return 0; uint8_t v = (uint8_t)p[0]; p += 1; n -= 1; return v; }
  uint32_t get_u32() { if (n < 4) return 0; uint32_t v; std::memcpy(&v, p, 4); p += 4; n -= 4; return v; }
  uint64_t get_u64() { if (n < 8) return 0; uint64_t v; std::memcpy(&v, p, 8); p += 8; n -= 8; return v; }

  // Non-owning view into the source buffer (no copy).
  std::string_view get_str() {
    uint32_t len = get_u32();
    if (len > n) return {};
    std::string_view v(p, len);
    p += len; n -= len;
    return v;
  }
  std::string_view get_bytes() { return get_str(); }

  bool empty() const { return n == 0; }
};

}  // namespace dfs
