// Packer: cuts a stream of int32 tokens into training batches in the batch
// format (Batch.h). Standard C++ only, like Batch.h, so that producers such
// as Ember's stoke connector pack the same way.
//
// A batch is an int32 tensor of [rows, sequence + 1]: windows of
// sequence + 1 tokens in order, the inputs and the shifted targets of
// sequence tokens each. Tokens fill windows across the texts or token runs
// added, so documents need no padding; a full batch has 'batch' rows, and
// finish() emits the whole windows left as a last, smaller batch. Tokens
// short of a whole window at the end are dropped.
//
// Text is tokenized by bytes: each UTF-8 byte (0-255), then kEndOfText (a
// vocabulary of 257).

#pragma once

#include <cstdint>
#include <functional>
#include <span>
#include <string_view>

namespace stoke::format {

constexpr int32_t kEndOfText = 256;

class Packer {
 public:
  // Called with each batch's bytes (header, payload and padding), which stay
  // valid only during the call.
  using Emit = std::function<void(std::span<const uint8_t> batch)>;

  // The bytes the buffer of a Packer of 'batch' x 'sequence' needs.
  static size_t bufferSize(uint64_t batch, uint64_t sequence);

  // Packs into 'buffer', of bufferSize(batch, sequence) bytes, aligned
  // for int32. Throws std::invalid_argument if 'batch' or 'sequence' is 0 or
  // 'buffer' is too small.
  Packer(
      uint64_t batch,
      uint64_t sequence,
      std::span<uint8_t> buffer,
      Emit emit);

  // Adds tokens, emitting each batch they fill.
  void add(std::span<const int32_t> tokens);

  // Adds the byte tokens of 'text' and kEndOfText.
  void addText(std::string_view text);

  // Emits the whole windows left, if any, as a last batch.
  void finish();

 private:
  void push(int32_t token) {
    payload_[filled_++] = token;
    if (filled_ == capacity_) {
      emit(batch_);
    }
  }

  // Emits the first 'rows' windows and starts a new batch.
  void emit(uint64_t rows);

  const uint64_t batch_;
  const uint64_t window_;
  const uint64_t capacity_;
  const std::span<uint8_t> buffer_;
  int32_t* const payload_;
  const Emit emit_;
  uint64_t filled_{0};
};

} // namespace stoke::format
