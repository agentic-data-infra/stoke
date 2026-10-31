#include "stoke/format/Batch.h"

#include <cstring>
#include <numeric>
#include <stdexcept>

#include <gtest/gtest.h>

namespace stoke::format {
namespace {

std::span<const uint8_t> bytesOf(std::string_view data) {
  return {reinterpret_cast<const uint8_t*>(data.data()), data.size()};
}

std::string tokens(int32_t first, size_t count) {
  std::vector<int32_t> values(count);
  std::iota(values.begin(), values.end(), first);
  return std::string(
      reinterpret_cast<const char*>(values.data()), count * sizeof(int32_t));
}

TEST(BatchTest, encodesAndParsesABatch) {
  auto payload = tokens(100, 2 * 3);
  BatchHeader header{.dtype = DType::kInt32, .shape = {2, 3}};
  auto batch = encodeBatch(header, bytesOf(payload));
  EXPECT_EQ(batch.size(), kHeaderSize + 24);
  EXPECT_EQ(batch.substr(0, 4), "STKB");

  auto view = parseBatch(bytesOf(batch));
  EXPECT_EQ(view.header, header);
  ASSERT_EQ(view.payload.size(), payload.size());
  EXPECT_EQ(
      std::memcmp(view.payload.data(), payload.data(), payload.size()), 0);
}

TEST(BatchTest, padsBatchesToEightBytes) {
  std::string payload(3, 'x');
  auto batch =
      encodeBatch({.dtype = DType::kUInt8, .shape = {3}}, bytesOf(payload));
  EXPECT_EQ(batch.size(), kHeaderSize + 8);
  EXPECT_EQ(parseBatch(bytesOf(batch)).payload.size(), 3);
}

TEST(BatchTest, parsesFramedChunks) {
  std::string chunk;
  auto first = tokens(0, 4 * 8);
  auto second = tokens(1000, 2 * 8);
  appendFrame(
      chunk,
      encodeBatch({.dtype = DType::kInt32, .shape = {4, 8}}, bytesOf(first)));
  std::string scalar(1, '\x01');
  appendFrame(
      chunk,
      encodeBatch({.dtype = DType::kBool, .shape = {}}, bytesOf(scalar)));
  appendFrame(
      chunk,
      encodeBatch({.dtype = DType::kInt32, .shape = {2, 8}}, bytesOf(second)));

  // Read into aligned memory, as the dataloader does.
  std::vector<uint64_t> aligned((chunk.size() + 7) / 8);
  std::memcpy(aligned.data(), chunk.data(), chunk.size());
  std::span<const uint8_t> bytes(
      reinterpret_cast<const uint8_t*>(aligned.data()), chunk.size());

  auto batches = parseChunk(bytes);
  ASSERT_EQ(batches.size(), 3);
  EXPECT_EQ(batches[0].header.shape, (std::vector<uint64_t>{4, 8}));
  EXPECT_EQ(batches[1].header.shape, std::vector<uint64_t>{});
  EXPECT_EQ(batches[1].header.elements(), 1);
  EXPECT_EQ(batches[2].header.dtype, DType::kInt32);
  for (const auto& batch : batches) {
    auto address = reinterpret_cast<uintptr_t>(batch.payload.data());
    EXPECT_EQ(address % kAlignment, 0);
  }
  const auto* values =
      reinterpret_cast<const int32_t*>(batches[2].payload.data());
  EXPECT_EQ(values[0], 1000);
  EXPECT_EQ(values[15], 1015);
  EXPECT_TRUE(parseChunk({}).empty());
}

TEST(BatchTest, rejectsMalformedInput) {
  auto payload = tokens(0, 4);
  EXPECT_THROW(
      encodeBatch({.dtype = DType::kInt32, .shape = {5}}, bytesOf(payload)),
      std::invalid_argument);
  EXPECT_THROW(
      encodeBatch(
          {.dtype = DType::kInt32, .shape = {1, 1, 1, 1, 1, 1, 4}},
          bytesOf(payload)),
      std::invalid_argument);

  auto batch =
      encodeBatch({.dtype = DType::kInt32, .shape = {4}}, bytesOf(payload));
  EXPECT_THROW(parseBatch(bytesOf(batch).first(40)), std::invalid_argument);
  EXPECT_THROW(
      parseBatch(bytesOf(batch).first(kHeaderSize + 8)), std::invalid_argument);
  auto badMagic = batch;
  badMagic[0] = 'X';
  EXPECT_THROW(parseBatch(bytesOf(badMagic)), std::invalid_argument);
  auto badDType = batch;
  badDType[5] = 99;
  EXPECT_THROW(parseBatch(bytesOf(badDType)), std::invalid_argument);

  std::string chunk;
  appendFrame(chunk, batch);
  EXPECT_THROW(
      parseChunk(bytesOf(chunk).first(chunk.size() - 1)),
      std::invalid_argument);
  EXPECT_THROW(parseChunk(bytesOf(chunk).first(5)), std::invalid_argument);
}

TEST(BatchTest, namesDTypes) {
  EXPECT_EQ(toString(DType::kBFloat16), "bfloat16");
  EXPECT_EQ(parseDType("int64"), DType::kInt64);
  EXPECT_EQ(itemSize(DType::kFloat16), 2);
  EXPECT_THROW(parseDType("complex64"), std::invalid_argument);
}

} // namespace
} // namespace stoke::format
