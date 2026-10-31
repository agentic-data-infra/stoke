// The DLPack C ABI (https://github.com/dmlc/dlpack, Apache 2.0), version
// 0.8: the structs a "dltensor" capsule holds, by which torch.from_dlpack
// and NumPy take tensors without copying.

#pragma once

#include <cstdint>

extern "C" {

enum DLDeviceType : int32_t {
  kDLCPU = 1,
};

struct DLDevice {
  DLDeviceType device_type;
  int32_t device_id;
};

enum DLDataTypeCode : uint8_t {
  kDLInt = 0,
  kDLUInt = 1,
  kDLFloat = 2,
  kDLBfloat = 4,
  kDLBool = 6,
};

struct DLDataType {
  uint8_t code;
  uint8_t bits;
  uint16_t lanes;
};

struct DLTensor {
  void* data;
  DLDevice device;
  int32_t ndim;
  DLDataType dtype;
  int64_t* shape;
  // Null for a compact row-major tensor.
  int64_t* strides;
  uint64_t byte_offset;
};

struct DLManagedTensor {
  DLTensor dl_tensor;
  void* manager_ctx;
  void (*deleter)(DLManagedTensor* self);
};

} // extern "C"
