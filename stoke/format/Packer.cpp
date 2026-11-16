#include "stoke/format/Packer.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>

#include "stoke/format/Batch.h"

namespace stoke::format {

size_t Packer::bufferSize(uint64_t batch, uint64_t sequence) {
  return encodedSize({.dtype = DType::kInt32, .shape = {batch, sequence + 1}});
}

Packer::Packer(
    uint64_t batch,
    uint64_t sequence,
    std::span<uint8_t> buffer,
    Emit emit)
    : batch_{batch},
      window_{sequence + 1},
      capacity_{batch * (sequence + 1)},
      buffer_{buffer},
      payload_{reinterpret_cast<int32_t*>(buffer.data() + kHeaderSize)},
      emit_{std::move(emit)} {
  if (batch == 0 || sequence == 0) {
    throw std::invalid_argument(
        "packing needs a positive batch size and sequence length");
  }
  if (buffer.size() < bufferSize(batch, sequence)) {
    throw std::invalid_argument("the packer's buffer is too small");
  }
}

void Packer::add(std::span<const int32_t> tokens) {
  while (!tokens.empty()) {
    const auto n = std::min<uint64_t>(tokens.size(), capacity_ - filled_);
    std::memcpy(payload_ + filled_, tokens.data(), n * sizeof(int32_t));
    filled_ += n;
    tokens = tokens.subspan(n);
    if (filled_ == capacity_) {
      emit(batch_);
    }
  }
}

void Packer::addText(std::string_view text) {
  for (const char c : text) {
    push(static_cast<unsigned char>(c));
  }
  push(kEndOfText);
}

void Packer::finish() {
  if (filled_ >= window_) {
    emit(filled_ / window_);
  }
  filled_ = 0;
}

void Packer::emit(uint64_t rows) {
  const BatchHeader header{.dtype = DType::kInt32, .shape = {rows, window_}};
  const auto size = encodedSize(header);
  // The header goes in front of the payload, which is already in place.
  encodeHeader(header, buffer_.first(size));
  emit_(buffer_.first(size));
  filled_ = 0;
}

} // namespace stoke::format
