#include "stoke/format/Packer.h"

#include <numeric>
#include <stdexcept>
#include <vector>

#include <gtest/gtest.h>

#include "stoke/format/Batch.h"

namespace stoke::format {
namespace {

// Packs with a Packer of 'batch' x 'sequence' and keeps what it emits.
class Packed {
 public:
  Packed(uint64_t batch, uint64_t sequence)
      : buffer_((Packer::bufferSize(batch, sequence) + 7) / 8),
        packer_{
            batch,
            sequence,
            {reinterpret_cast<uint8_t*>(buffer_.data()),
             Packer::bufferSize(batch, sequence)},
            [this](std::span<const uint8_t> batch) {
              const auto view = parseBatch(batch);
              shapes.push_back(view.header.shape);
              EXPECT_EQ(view.header.dtype, DType::kInt32);
              const auto* tokens =
                  reinterpret_cast<const int32_t*>(view.payload.data());
              batches.emplace_back(
                  tokens, tokens + view.payload.size() / sizeof(int32_t));
            }} {}

  Packer& packer() {
    return packer_;
  }

  std::vector<std::vector<uint64_t>> shapes;
  std::vector<std::vector<int32_t>> batches;

 private:
  std::vector<uint64_t> buffer_;
  Packer packer_;
};

std::vector<int32_t> iota(int32_t first, size_t count) {
  std::vector<int32_t> values(count);
  std::iota(values.begin(), values.end(), first);
  return values;
}

TEST(PackerTest, fillsWindowsAcrossAdds) {
  Packed packed(2, 3); // batches of 2 windows of 4 tokens
  packed.packer().add(iota(0, 3));
  EXPECT_TRUE(packed.batches.empty());
  packed.packer().add(iota(3, 7)); // 10 tokens: one batch of 8, 2 left
  ASSERT_EQ(packed.batches.size(), 1);
  EXPECT_EQ(packed.batches[0], iota(0, 8));
  EXPECT_EQ(packed.shapes[0], (std::vector<uint64_t>{2, 4}));
  packed.packer().add(iota(10, 6));
  ASSERT_EQ(packed.batches.size(), 2);
  EXPECT_EQ(packed.batches[1], iota(8, 8));
}

TEST(PackerTest, finishEmitsTheWholeWindowsLeft) {
  Packed packed(4, 3);
  packed.packer().add(iota(0, 4 * 4 + 6)); // one full batch, then 6 tokens
  packed.packer().finish();
  ASSERT_EQ(packed.batches.size(), 2);
  EXPECT_EQ(packed.shapes[1], (std::vector<uint64_t>{1, 4}));
  EXPECT_EQ(packed.batches[1], iota(16, 4)); // the last 2 tokens are dropped
  packed.packer().finish(); // nothing left
  EXPECT_EQ(packed.batches.size(), 2);
}

TEST(PackerTest, tokenizesTextByBytes) {
  Packed packed(1, 4);
  packed.packer().addText("ab");
  packed.packer().addText("\xc3\xa9"); // é: two bytes
  ASSERT_EQ(packed.batches.size(), 1);
  EXPECT_EQ(
      packed.batches[0],
      (std::vector<int32_t>{'a', 'b', kEndOfText, 0xc3, 0xa9}));
  packed.packer().addText("");
  packed.packer().finish();
  EXPECT_EQ(packed.batches.size(), 1); // one token is no window
}

TEST(PackerTest, padsOddBatches) {
  Packed packed(1, 2); // 3 tokens: 12 bytes of payload, padded to 16
  EXPECT_EQ(Packer::bufferSize(1, 2), kHeaderSize + 16);
  packed.packer().add(iota(0, 3));
  ASSERT_EQ(packed.batches.size(), 1);
  EXPECT_EQ(packed.batches[0], iota(0, 3));
}

TEST(PackerTest, rejectsBadSizes) {
  std::vector<uint8_t> buffer(Packer::bufferSize(2, 3));
  auto emit = [](std::span<const uint8_t>) {};
  EXPECT_THROW(Packer(0, 3, buffer, emit), std::invalid_argument);
  EXPECT_THROW(Packer(2, 0, buffer, emit), std::invalid_argument);
  EXPECT_THROW(
      Packer(2, 3, std::span(buffer).first(buffer.size() - 1), emit),
      std::invalid_argument);
}

} // namespace
} // namespace stoke::format
