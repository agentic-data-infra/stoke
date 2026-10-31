#include "stoke/format/Batch.h"

#include <bit>
#include <cstring>
#include <stdexcept>

namespace stoke::format {

namespace {

static_assert(
    std::endian::native == std::endian::little,
    "the batch format is little-endian");

constexpr char kMagic[4] = {'S', 'T', 'K', 'B'};
constexpr uint8_t kVersion = 1;
// Offsets in the header.
constexpr size_t kDTypeOffset = 5;
constexpr size_t kDimsOffset = 6;
constexpr size_t kShapeOffset = 8;
constexpr size_t kPayloadLengthOffset = kShapeOffset + 8 * kMaxDims;
static_assert(kPayloadLengthOffset + 8 == kHeaderSize);

struct DTypeInfo {
  DType dtype;
  std::string_view name;
  size_t size;
};

constexpr DTypeInfo kDTypes[] = {
    {DType::kBool, "bool", 1},
    {DType::kUInt8, "uint8", 1},
    {DType::kInt8, "int8", 1},
    {DType::kInt16, "int16", 2},
    {DType::kUInt16, "uint16", 2},
    {DType::kInt32, "int32", 4},
    {DType::kUInt32, "uint32", 4},
    {DType::kInt64, "int64", 8},
    {DType::kFloat16, "float16", 2},
    {DType::kBFloat16, "bfloat16", 2},
    {DType::kFloat32, "float32", 4},
    {DType::kFloat64, "float64", 8},
};

const DTypeInfo& info(DType dtype) {
  for (const auto& entry : kDTypes) {
    if (entry.dtype == dtype) {
      return entry;
    }
  }
  throw std::invalid_argument(
      "unknown dtype " + std::to_string(static_cast<int>(dtype)));
}

uint64_t load64(const uint8_t* at) {
  uint64_t value;
  std::memcpy(&value, at, sizeof(value));
  return value;
}

void store64(uint8_t* at, uint64_t value) {
  std::memcpy(at, &value, sizeof(value));
}

size_t padded(size_t size) {
  return (size + kAlignment - 1) / kAlignment * kAlignment;
}

} // namespace

size_t itemSize(DType dtype) {
  return info(dtype).size;
}

std::string_view toString(DType dtype) {
  return info(dtype).name;
}

DType parseDType(std::string_view name) {
  for (const auto& entry : kDTypes) {
    if (entry.name == name) {
      return entry.dtype;
    }
  }
  throw std::invalid_argument("unknown dtype " + std::string(name));
}

uint64_t BatchHeader::elements() const {
  uint64_t count = 1;
  for (auto dim : shape) {
    if (dim != 0 && count > UINT64_MAX / dim) {
      throw std::invalid_argument("batch shape overflows");
    }
    count *= dim;
  }
  return count;
}

std::string encodeBatch(
    const BatchHeader& header,
    std::span<const uint8_t> payload) {
  if (header.shape.size() > kMaxDims) {
    throw std::invalid_argument(
        "a batch has at most " + std::to_string(kMaxDims) + " dims");
  }
  if (header.payloadBytes() != payload.size()) {
    throw std::invalid_argument(
        "payload of " + std::to_string(payload.size()) +
        " bytes does not match the shape's " +
        std::to_string(header.payloadBytes()));
  }
  std::string batch(padded(kHeaderSize + payload.size()), '\0');
  auto* bytes = reinterpret_cast<uint8_t*>(batch.data());
  std::memcpy(bytes, kMagic, sizeof(kMagic));
  bytes[4] = kVersion;
  bytes[kDTypeOffset] = static_cast<uint8_t>(header.dtype);
  bytes[kDimsOffset] = static_cast<uint8_t>(header.shape.size());
  for (size_t i = 0; i < header.shape.size(); ++i) {
    store64(bytes + kShapeOffset + 8 * i, header.shape[i]);
  }
  store64(bytes + kPayloadLengthOffset, payload.size());
  if (!payload.empty()) {
    std::memcpy(bytes + kHeaderSize, payload.data(), payload.size());
  }
  return batch;
}

void appendFrame(std::string& chunk, std::string_view batch) {
  uint8_t length[kFrameLengthSize];
  store64(length, batch.size());
  chunk.append(reinterpret_cast<const char*>(length), sizeof(length));
  chunk.append(batch);
}

BatchView parseBatch(std::span<const uint8_t> batch) {
  if (batch.size() < kHeaderSize) {
    throw std::invalid_argument("batch shorter than its header");
  }
  const auto* bytes = batch.data();
  if (std::memcmp(bytes, kMagic, sizeof(kMagic)) != 0) {
    throw std::invalid_argument("not a batch (bad magic)");
  }
  if (bytes[4] != kVersion) {
    throw std::invalid_argument(
        "unsupported batch version " + std::to_string(bytes[4]));
  }
  BatchView view;
  view.header.dtype = static_cast<DType>(bytes[kDTypeOffset]);
  itemSize(view.header.dtype);
  const size_t dims = bytes[kDimsOffset];
  if (dims > kMaxDims) {
    throw std::invalid_argument("batch with " + std::to_string(dims) + " dims");
  }
  for (size_t i = 0; i < dims; ++i) {
    view.header.shape.push_back(load64(bytes + kShapeOffset + 8 * i));
  }
  const auto length = load64(bytes + kPayloadLengthOffset);
  if (length != view.header.payloadBytes() ||
      length > batch.size() - kHeaderSize) {
    throw std::invalid_argument("batch payload length does not match");
  }
  view.payload = batch.subspan(kHeaderSize, length);
  return view;
}

std::vector<BatchView> parseChunk(std::span<const uint8_t> chunk) {
  std::vector<BatchView> batches;
  size_t offset = 0;
  while (offset < chunk.size()) {
    if (chunk.size() - offset < kFrameLengthSize) {
      throw std::invalid_argument("chunk ends inside a frame length");
    }
    const auto length = load64(chunk.data() + offset);
    offset += kFrameLengthSize;
    if (length > chunk.size() - offset) {
      throw std::invalid_argument("chunk ends inside a frame");
    }
    batches.push_back(parseBatch(chunk.subspan(offset, length)));
    offset += length;
  }
  return batches;
}

} // namespace stoke::format
