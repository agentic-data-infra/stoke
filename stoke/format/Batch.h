// The batch format: how producers lay out training batches in Kiln chunks
// and the dataloader finds them again. Standard C++ only, so that any
// producer (e.g. Ember's stoke_pack function) can use it.
//
// A chunk is a sequence of frames, each a little-endian u64 length and that
// many bytes of one batch:
//
//   chunk  = frame*
//   frame  = u64 length, batch
//   batch  = header (64 bytes), payload, padding to a multiple of 8 bytes
//   header = u32 magic "STKB", u8 version (1), u8 dtype, u8 number of dims,
//            u8 0, u64 shape[6] (unused dims 0), u64 payload length
//
// The payload is the tensor's elements, row-major, little-endian. Padding
// keeps every frame, and so every payload, 8-byte aligned in a chunk read
// into aligned memory, so that tensors can point straight into it. Batches
// in a chunk may differ in dtype and shape.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace stoke::format {

enum class DType : uint8_t {
  kBool = 1,
  kUInt8 = 2,
  kInt8 = 3,
  kInt16 = 4,
  kUInt16 = 5,
  kInt32 = 6,
  kUInt32 = 7,
  kInt64 = 8,
  kFloat16 = 9,
  kBFloat16 = 10,
  kFloat32 = 11,
  kFloat64 = 12,
};

// Bytes per element. Throws std::invalid_argument for an unknown dtype.
size_t itemSize(DType dtype);

// "int32", "bfloat16", ...; parseDType takes the same names.
std::string_view toString(DType dtype);
DType parseDType(std::string_view name);

constexpr size_t kHeaderSize = 64;
constexpr size_t kMaxDims = 6;
constexpr size_t kFrameLengthSize = 8;
constexpr size_t kAlignment = 8;

struct BatchHeader {
  DType dtype{DType::kUInt8};
  std::vector<uint64_t> shape;

  uint64_t elements() const;

  uint64_t payloadBytes() const {
    return elements() * itemSize(dtype);
  }

  bool operator==(const BatchHeader&) const = default;
};

// A batch inside a chunk, pointing into its bytes.
struct BatchView {
  BatchHeader header;
  std::span<const uint8_t> payload;
};

// The batch's bytes: header, 'payload' and padding. Throws
// std::invalid_argument if the shape has more than kMaxDims dims or does
// not match the payload's length.
std::string encodeBatch(
    const BatchHeader& header,
    std::span<const uint8_t> payload);

// Appends a frame of 'batch' to 'chunk'.
void appendFrame(std::string& chunk, std::string_view batch);

// Parses one batch (without its frame length). Throws std::invalid_argument
// if it is malformed.
BatchView parseBatch(std::span<const uint8_t> batch);

// The batches of a chunk, in order. Throws std::invalid_argument if a frame
// or batch is malformed or the chunk ends inside a frame.
std::vector<BatchView> parseChunk(std::span<const uint8_t> chunk);

} // namespace stoke::format
