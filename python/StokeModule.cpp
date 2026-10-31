// The Python module stoke._stoke: the dataloader (stoke/loader) and its
// batches as DLPack tensors, for torch.from_dlpack; a producer of chunks
// of NumPy arrays; and creating, sealing and removing streams.

#include <Python.h>
#include <folly/coro/BlockingWait.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "kiln/client/Client.h"
#include "python/DLPack.h"
#include "stoke/client/Producer.h"
#include "stoke/loader/Loader.h"

namespace py = pybind11;

namespace stoke::python {
namespace {

constexpr const char* kCapsuleName = "dltensor";
constexpr const char* kUsedCapsuleName = "used_dltensor";

DLDataType dlType(format::DType dtype) {
  const auto bits = static_cast<uint8_t>(8 * format::itemSize(dtype));
  switch (dtype) {
    case format::DType::kBool:
      return {kDLBool, bits, 1};
    case format::DType::kUInt8:
    case format::DType::kUInt16:
    case format::DType::kUInt32:
      return {kDLUInt, bits, 1};
    case format::DType::kInt8:
    case format::DType::kInt16:
    case format::DType::kInt32:
    case format::DType::kInt64:
      return {kDLInt, bits, 1};
    case format::DType::kBFloat16:
      return {kDLBfloat, bits, 1};
    case format::DType::kFloat16:
    case format::DType::kFloat32:
    case format::DType::kFloat64:
      return {kDLFloat, bits, 1};
  }
  throw std::invalid_argument("unknown dtype");
}

// What a DLPack capsule owns: the tensor, its shape and the batch whose
// chunk holds its elements.
struct Managed {
  DLManagedTensor tensor{};
  std::vector<int64_t> shape;
  Batch batch;
};

void deleteManaged(DLManagedTensor* tensor) {
  delete static_cast<Managed*>(tensor->manager_ctx);
}

// Frees a capsule no consumer took (a consumer renames it).
void deleteCapsule(PyObject* capsule) {
  if (PyCapsule_IsValid(capsule, kCapsuleName)) {
    auto* tensor = static_cast<DLManagedTensor*>(
        PyCapsule_GetPointer(capsule, kCapsuleName));
    tensor->deleter(tensor);
  }
}

// A batch that DLPack consumers (torch.from_dlpack, numpy.from_dlpack) take
// without copying; it stays valid as long as they keep it.
class Tensor {
 public:
  explicit Tensor(Batch batch) : batch_{std::move(batch)} {}

  py::tuple shape() const {
    const auto& shape = batch_.view.header.shape;
    py::tuple result(shape.size());
    for (size_t i = 0; i < shape.size(); ++i) {
      result[i] = shape[i];
    }
    return result;
  }

  std::string dtype() const {
    return std::string(format::toString(batch_.view.header.dtype));
  }

  py::object dlpack() const {
    auto managed = std::make_unique<Managed>();
    managed->batch = batch_;
    const auto& header = batch_.view.header;
    managed->shape.assign(header.shape.begin(), header.shape.end());
    auto& tensor = managed->tensor.dl_tensor;
    // Consumers may write to it; the chunk is this batch's alone.
    tensor.data = const_cast<uint8_t*>(batch_.view.payload.data());
    tensor.device = {kDLCPU, 0};
    tensor.ndim = static_cast<int32_t>(managed->shape.size());
    tensor.dtype = dlType(header.dtype);
    tensor.shape = managed->shape.data();
    tensor.strides = nullptr;
    tensor.byte_offset = 0;
    managed->tensor.manager_ctx = managed.get();
    managed->tensor.deleter = deleteManaged;
    auto* capsule =
        PyCapsule_New(&managed->tensor, kCapsuleName, deleteCapsule);
    if (capsule == nullptr) {
      throw py::error_already_set();
    }
    managed.release();
    return py::reinterpret_steal<py::object>(capsule);
  }

 private:
  Batch batch_;
};

py::dict statsDict(const LoaderStats& stats) {
  py::dict result;
  result["batches"] = stats.batches;
  result["chunks"] = stats.chunks;
  result["bytes"] = stats.bytes;
  result["takes"] = stats.takes;
  result["empty_takes"] = stats.emptyTakes;
  result["take_seconds"] =
      std::chrono::duration<double>(stats.takeTime).count();
  result["errors"] = stats.errors;
  result["lost_chunks"] = stats.lostChunks;
  result["wait_seconds"] =
      std::chrono::duration<double>(stats.waitTime).count();
  return result;
}

std::shared_ptr<kiln::client::Client> connectKiln(const std::string& metadata) {
  return std::make_shared<kiln::client::Client>(kiln::client::ClientOptions{
      .metadata = kiln::MetadataAddress::parse(metadata)});
}

format::DType dtypeOf(const py::dtype& dtype) {
  const auto size = dtype.itemsize();
  switch (dtype.kind()) {
    case 'b':
      return format::DType::kBool;
    case 'u':
      if (size == 1) {
        return format::DType::kUInt8;
      }
      if (size == 2) {
        return format::DType::kUInt16;
      }
      if (size == 4) {
        return format::DType::kUInt32;
      }
      break;
    case 'i':
      if (size == 1) {
        return format::DType::kInt8;
      }
      if (size == 2) {
        return format::DType::kInt16;
      }
      if (size == 4) {
        return format::DType::kInt32;
      }
      if (size == 8) {
        return format::DType::kInt64;
      }
      break;
    case 'f':
      if (size == 2) {
        return format::DType::kFloat16;
      }
      if (size == 4) {
        return format::DType::kFloat32;
      }
      if (size == 8) {
        return format::DType::kFloat64;
      }
      break;
  }
  throw std::invalid_argument(
      "no batch dtype for NumPy dtype " + py::str(dtype).cast<std::string>());
}

// Puts chunks of NumPy arrays into a stream.
class PyProducer {
 public:
  PyProducer(const std::string& metadata, std::string stream) {
    auto kiln = connectKiln(metadata);
    auto registry = std::make_shared<Registry>(kiln);
    std::shared_ptr<StreamClient> client = folly::coro::blockingWait(
        StreamClient::co_open(std::move(registry), std::move(stream)));
    producer_ = std::make_unique<Producer>(std::move(client), kiln);
  }

  // Writes the arrays, one batch each, as one chunk and puts it; returns
  // the chunk's id.
  std::string put(const std::vector<py::array>& arrays) {
    std::string chunk;
    for (const auto& array : arrays) {
      auto contiguous =
          py::array::ensure(array, py::array::c_style | py::array::forcecast);
      format::BatchHeader header{.dtype = dtypeOf(contiguous.dtype())};
      for (py::ssize_t i = 0; i < contiguous.ndim(); ++i) {
        header.shape.push_back(static_cast<uint64_t>(contiguous.shape(i)));
      }
      std::span<const uint8_t> payload(
          static_cast<const uint8_t*>(contiguous.data()),
          static_cast<size_t>(contiguous.nbytes()));
      format::appendFrame(chunk, format::encodeBatch(header, payload));
    }
    py::gil_scoped_release release;
    return folly::coro::blockingWait(producer_->co_put(chunk)).toString();
  }

 private:
  std::unique_ptr<Producer> producer_;
};

std::optional<Tensor> nextTensor(Loader& loader) {
  std::optional<Batch> batch;
  {
    py::gil_scoped_release release;
    batch = loader.next();
  }
  if (!batch) {
    return std::nullopt;
  }
  return Tensor(std::move(*batch));
}

} // namespace
} // namespace stoke::python

PYBIND11_MODULE(_stoke, m) {
  using namespace stoke;
  using namespace stoke::python;
  m.doc() = "stoke's dataloader: batches of a stoke stream as DLPack tensors";

  py::class_<Tensor>(m, "Tensor")
      .def_property_readonly("shape", &Tensor::shape)
      .def_property_readonly("dtype", &Tensor::dtype)
      .def(
          "__dlpack__",
          [](const Tensor& tensor, py::kwargs) { return tensor.dlpack(); })
      .def("__dlpack_device__", [](const Tensor&) {
        return py::make_tuple(static_cast<int>(kDLCPU), 0);
      });

  py::class_<PyProducer>(m, "Producer")
      .def(
          py::init<const std::string&, std::string>(),
          py::arg("metadata"),
          py::arg("stream"))
      .def(
          "put",
          &PyProducer::put,
          py::arg("arrays"),
          "Puts the arrays, a batch each, as one chunk; returns its id.");

  m.def(
      "create_stream",
      [](const std::string& metadata,
         std::string stream,
         std::vector<std::string> shards,
         double stopFraction) {
        py::gil_scoped_release release;
        Registry registry(connectKiln(metadata));
        folly::coro::blockingWait(registry.co_create(
            std::move(stream), std::move(shards), stopFraction));
      },
      py::arg("metadata"),
      py::arg("stream"),
      py::arg("shards"),
      py::arg("stop_fraction") = 0.95);
  m.def(
      "seal_stream",
      [](const std::string& metadata, std::string stream) {
        py::gil_scoped_release release;
        Registry registry(connectKiln(metadata));
        folly::coro::blockingWait(registry.co_seal(std::move(stream)));
      },
      py::arg("metadata"),
      py::arg("stream"));
  m.def(
      "remove_stream",
      [](const std::string& metadata, std::string stream) {
        py::gil_scoped_release release;
        Registry registry(connectKiln(metadata));
        return folly::coro::blockingWait(registry.co_remove(std::move(stream)));
      },
      py::arg("metadata"),
      py::arg("stream"));

  py::class_<Loader>(m, "Loader")
      .def(
          py::init([](const std::string& metadata,
                      std::string stream,
                      std::string job,
                      uint64_t incarnation,
                      uint32_t fetchers,
                      size_t bufferedBatches,
                      uint32_t chunksPerTake,
                      int64_t waitMs,
                      bool removeChunks,
                      bool releasePrevious,
                      uint64_t seed) {
            py::gil_scoped_release release;
            return Loader::open(
                metadata,
                std::move(stream),
                LoaderOptions{
                    .epoch = Epoch{std::move(job), incarnation},
                    .releasePrevious = releasePrevious,
                    .fetchers = fetchers,
                    .bufferedBatches = bufferedBatches,
                    .chunksPerTake = chunksPerTake,
                    .wait = std::chrono::milliseconds(waitMs),
                    .removeChunks = removeChunks,
                    .seed = seed});
          }),
          py::arg("metadata"),
          py::arg("stream"),
          py::arg("job"),
          py::arg("incarnation") = 0,
          py::arg("fetchers") = 2,
          py::arg("buffered_batches") = 64,
          py::arg("chunks_per_take") = 1,
          py::arg("wait_ms") = 100,
          py::arg("remove_chunks") = true,
          py::arg("release_previous") = true,
          py::arg("seed") = 0)
      .def("next", &nextTensor, "The next batch, or None at the end.")
      .def("__iter__", [](Loader& loader) -> Loader& { return loader; })
      .def(
          "__next__",
          [](Loader& loader) {
            auto tensor = nextTensor(loader);
            if (!tensor) {
              throw py::stop_iteration();
            }
            return std::move(*tensor);
          })
      .def("stats", [](const Loader& loader) {
        return statsDict(loader.stats());
      });
}
