/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <pybind11/buffer_info.h>

#include <warabi/Client.hpp>
#include <warabi/TargetHandle.hpp>
#include <warabi/AsyncRequest.hpp>
#include <warabi/Exception.hpp>
#include <warabi/RegionID.hpp>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include <sstream>
#include <string>

// Make RegionID opaque so pybind11 doesn't auto-convert it to a Python list
PYBIND11_MAKE_OPAQUE(warabi::RegionID);

namespace py = pybind11;
using namespace pybind11::literals;

// Wrapper class for async create operations
class AsyncCreateRequest {
    std::shared_ptr<warabi::RegionID> m_region;
    std::shared_ptr<warabi::AsyncRequest> m_request;

public:
    AsyncCreateRequest(std::shared_ptr<warabi::RegionID> region,
                       std::shared_ptr<warabi::AsyncRequest> request)
        : m_region(std::move(region)), m_request(std::move(request)) {}

    warabi::RegionID wait() {
        m_request->wait();
        return *m_region;
    }

    bool completed() const {
        return m_request->completed();
    }
};

// Helper function to convert RegionID to Python bytes
static py::bytes region_id_to_bytes(const warabi::RegionID& region_id) {
    return py::bytes(reinterpret_cast<const char*>(region_id.data()), region_id.size());
}

// Helper function to convert Python bytes to RegionID
static warabi::RegionID bytes_to_region_id(const py::bytes& b) {
    std::string s = b;
    if (s.size() != 16) {
        throw warabi::Exception("RegionID must be exactly 16 bytes");
    }
    warabi::RegionID region_id;
    std::memcpy(region_id.data(), s.data(), 16);
    return region_id;
}

PYBIND11_MODULE(_pywarabi_client, m) {
    m.doc() = "Python binding for the Warabi client library";

    // Register the Warabi exception
    py::register_exception<warabi::Exception>(m, "Exception");

    // Bind RegionID as a simple bytes wrapper
    py::class_<warabi::RegionID>(m, "RegionID")
        .def(py::init<>(),
            R"(
            Default RegionID constructor (creates zeroed ID).
            )")
        .def(py::init([](const py::bytes& b) {
                return bytes_to_region_id(b);
            }),
            R"(
            RegionID constructor from bytes.

            Parameters
            ----------
            data (bytes): 16-byte region identifier.
            )",
            "data"_a)
        .def("to_bytes", &region_id_to_bytes,
            R"(
            Convert RegionID to bytes.

            Returns
            -------
            bytes: 16-byte region identifier.
            )")
        .def("__repr__", [](const warabi::RegionID& rid) {
            std::stringstream ss;
            ss << "RegionID(";
            for (size_t i = 0; i < 16; ++i) {
                ss << std::hex << std::setfill('0') << std::setw(2)
                   << static_cast<int>(rid[i]);
            }
            ss << ")";
            return ss.str();
        })
        .def("__eq__", [](const warabi::RegionID& a, const warabi::RegionID& b) {
            return a == b;
        })
        .def("__hash__", [](const warabi::RegionID& rid) {
            return py::hash(py::bytes(reinterpret_cast<const char*>(rid.data()), rid.size()));
        });

    // Bind AsyncRequest with shared_ptr holder to avoid copying
    py::class_<warabi::AsyncRequest, std::shared_ptr<warabi::AsyncRequest>>(m, "AsyncRequest")
        .def(py::init<>(),
            R"(
            AsyncRequest constructor.
            )")
        .def("wait", &warabi::AsyncRequest::wait,
            R"(
            Wait for the asynchronous request to complete.
            )")
        .def("completed", &warabi::AsyncRequest::completed,
            R"(
            Test if the asynchronous request has completed.

            Returns
            -------
            bool: True if completed, False otherwise.
            )");

    // Bind AsyncCreateRequest
    py::class_<AsyncCreateRequest>(m, "AsyncCreateRequest")
        .def("wait", &AsyncCreateRequest::wait,
            R"(
            Wait for the asynchronous create operation to complete.

            Returns
            -------
            RegionID: The ID of the created region.
            )")
        .def("completed", &AsyncCreateRequest::completed,
            R"(
            Test if the asynchronous create operation has completed.

            Returns
            -------
            bool: True if completed, False otherwise.
            )");

    // Bind Client
    py::class_<warabi::Client>(m, "Client")
        .def(py::init([](const py::object& pyMargoEngine) {
            // Extract margo_instance_id from PyMargo Engine
            py::capsule mid = pyMargoEngine.attr("get_internal_mid")();
            return warabi::Client{mid};
        }), py::keep_alive<1, 2>(),
            R"(
            Client constructor.

            Parameters
            ----------
            engine (pymargo.Engine): PyMargo Engine to use.
            )",
            "engine"_a)
        .def("make_target_handle",
            &warabi::Client::makeTargetHandle,
            R"(
            Create a TargetHandle to a remote Warabi target.

            Parameters
            ----------
            address (str): Address of the provider holding the target.
            provider_id (int): Provider ID.

            Returns
            -------
            TargetHandle: Handle to the remote target.
            )",
            "address"_a, "provider_id"_a)
        .def("get_config", &warabi::Client::getConfig,
            R"(
            Get the client configuration as a JSON string.

            Returns
            -------
            str: JSON configuration string.
            )");

    // Bind TargetHandle
    py::class_<warabi::TargetHandle>(m, "TargetHandle")
        .def(py::init<>(),
            R"(
            Default TargetHandle constructor (creates invalid handle).
            )")
        // Create operation
        .def("create",
            [](const warabi::TargetHandle& handle, size_t size) {
                warabi::RegionID region;
                handle.create(&region, size);
                return region;
            },
            R"(
            Create a new region with the specified size.

            Parameters
            ----------
            size (int): Size of the region to create.

            Returns
            -------
            RegionID: ID of the created region.
            )",
            "size"_a)
        .def("create_async",
            [](const warabi::TargetHandle& handle, size_t size) {
                // Allocate RegionID on heap to keep it alive during async operation
                auto region = std::make_shared<warabi::RegionID>();
                auto req = std::make_shared<warabi::AsyncRequest>();
                handle.create(region.get(), size, req.get());
                return AsyncCreateRequest(region, req);
            },
            R"(
            Create a new region asynchronously.

            Parameters
            ----------
            size (int): Size of the region to create.

            Returns
            -------
            AsyncCreateRequest: Async request that can be waited on to get the RegionID.
            )",
            "size"_a)
        // Write operations (buffer-based)
        .def("write",
            [](const warabi::TargetHandle& handle,
               const warabi::RegionID& region,
               size_t offset,
               const py::buffer& data,
               bool persist) {
                py::buffer_info info = data.request();
                if (info.ndim != 1) {
                    throw warabi::Exception("Buffer must be 1-dimensional");
                }
                handle.write(region, offset,
                            static_cast<const char*>(info.ptr),
                            info.size * info.itemsize,
                            persist);
            },
            R"(
            Write data to a region.

            Parameters
            ----------
            region (RegionID): Region to write to.
            offset (int): Offset in the region.
            data (buffer): Data to write (bytes, bytearray, memoryview, numpy array, etc.).
            persist (bool): Whether to persist the data (default: False).
            )",
            "region"_a, "offset"_a, "data"_a, "persist"_a=false)
        .def("write_async",
            [](const warabi::TargetHandle& handle,
               const warabi::RegionID& region,
               size_t offset,
               const py::buffer& data,
               bool persist) {
                py::buffer_info info = data.request();
                if (info.ndim != 1) {
                    throw warabi::Exception("Buffer must be 1-dimensional");
                }
                auto req = std::make_shared<warabi::AsyncRequest>();
                handle.write(region, offset,
                            static_cast<const char*>(info.ptr),
                            info.size * info.itemsize,
                            persist, req.get());
                return req;
            },
            R"(
            Write data to a region asynchronously.

            Parameters
            ----------
            region (RegionID): Region to write to.
            offset (int): Offset in the region.
            data (buffer): Data to write.
            persist (bool): Whether to persist the data (default: False).

            Returns
            -------
            AsyncRequest: Asynchronous request handle.
            )",
            "region"_a, "offset"_a, "data"_a, "persist"_a=false)
        // Read operations
        .def("read",
            [](const warabi::TargetHandle& handle,
               const warabi::RegionID& region,
               size_t offset,
               size_t size) {
                std::vector<char> buffer(size);
                handle.read(region, offset, buffer.data(), size);
                return py::bytes(buffer.data(), size);
            },
            R"(
            Read data from a region.

            Parameters
            ----------
            region (RegionID): Region to read from.
            offset (int): Offset in the region.
            size (int): Number of bytes to read.

            Returns
            -------
            bytes: Data read from the region.
            )",
            "region"_a, "offset"_a, "size"_a)
        .def("read_into",
            [](const warabi::TargetHandle& handle,
               const warabi::RegionID& region,
               size_t offset,
               py::buffer& buffer) {
                py::buffer_info info = buffer.request();
                if (info.ndim != 1) {
                    throw warabi::Exception("Buffer must be 1-dimensional");
                }
                if (info.readonly) {
                    throw warabi::Exception("Buffer must be writable");
                }
                handle.read(region, offset,
                           static_cast<char*>(info.ptr),
                           info.size * info.itemsize);
            },
            R"(
            Read data from a region into a pre-allocated buffer.

            Parameters
            ----------
            region (RegionID): Region to read from.
            offset (int): Offset in the region.
            buffer (writable buffer): Buffer to read into (must be writable).
            )",
            "region"_a, "offset"_a, "buffer"_a)
        .def("read_async",
            [](const warabi::TargetHandle& handle,
               const warabi::RegionID& region,
               size_t offset,
               py::buffer& buffer) {
                py::buffer_info info = buffer.request();
                if (info.ndim != 1) {
                    throw warabi::Exception("Buffer must be 1-dimensional");
                }
                if (info.readonly) {
                    throw warabi::Exception("Buffer must be writable");
                }
                auto req = std::make_shared<warabi::AsyncRequest>();
                handle.read(region, offset,
                           static_cast<char*>(info.ptr),
                           info.size * info.itemsize,
                           req.get());
                return req;
            },
            R"(
            Read data from a region asynchronously into a buffer.

            Parameters
            ----------
            region (RegionID): Region to read from.
            offset (int): Offset in the region.
            buffer (writable buffer): Buffer to read into.

            Returns
            -------
            AsyncRequest: Asynchronous request handle.
            )",
            "region"_a, "offset"_a, "buffer"_a)
        // Persist operations
        .def("persist",
            [](const warabi::TargetHandle& handle,
               const warabi::RegionID& region,
               size_t offset,
               size_t size) {
                handle.persist(region, offset, size);
            },
            R"(
            Persist a segment of a region.

            Parameters
            ----------
            region (RegionID): Region to persist.
            offset (int): Offset in the region.
            size (int): Number of bytes to persist.
            )",
            "region"_a, "offset"_a, "size"_a)
        .def("persist_async",
            [](const warabi::TargetHandle& handle,
               const warabi::RegionID& region,
               size_t offset,
               size_t size) {
                auto req = std::make_shared<warabi::AsyncRequest>();
                handle.persist(region, offset, size, req.get());
                return req;
            },
            R"(
            Persist a segment of a region asynchronously.

            Parameters
            ----------
            region (RegionID): Region to persist.
            offset (int): Offset in the region.
            size (int): Number of bytes to persist.

            Returns
            -------
            AsyncRequest: Asynchronous request handle.
            )",
            "region"_a, "offset"_a, "size"_a)
        // Erase operations
        .def("erase",
            [](const warabi::TargetHandle& handle,
               const warabi::RegionID& region) {
                handle.erase(region);
            },
            R"(
            Erase a region.

            Parameters
            ----------
            region (RegionID): Region to erase.
            )",
            "region"_a)
        .def("erase_async",
            [](const warabi::TargetHandle& handle,
               const warabi::RegionID& region) {
                auto req = std::make_shared<warabi::AsyncRequest>();
                handle.erase(region, req.get());
                return req;
            },
            R"(
            Erase a region asynchronously.

            Parameters
            ----------
            region (RegionID): Region to erase.

            Returns
            -------
            AsyncRequest: Asynchronous request handle.
            )",
            "region"_a)
        // Create and write combined
        .def("create_and_write",
            [](const warabi::TargetHandle& handle,
               const py::buffer& data,
               bool persist) {
                py::buffer_info info = data.request();
                if (info.ndim != 1) {
                    throw warabi::Exception("Buffer must be 1-dimensional");
                }
                warabi::RegionID region;
                handle.createAndWrite(&region,
                                     static_cast<const char*>(info.ptr),
                                     info.size * info.itemsize,
                                     persist);
                return region;
            },
            R"(
            Create a new region and write data to it in one operation.

            Parameters
            ----------
            data (buffer): Data to write.
            persist (bool): Whether to persist the data (default: False).

            Returns
            -------
            RegionID: ID of the created region.
            )",
            "data"_a, "persist"_a=false)
        // Threshold setters
        .def("set_eager_write_threshold",
            &warabi::TargetHandle::setEagerWriteThreshold,
            R"(
            Set the threshold for eager writes (default: 2048 bytes).

            Parameters
            ----------
            size (int): Threshold size in bytes.
            )",
            "size"_a)
        .def("set_eager_read_threshold",
            &warabi::TargetHandle::setEagerReadThreshold,
            R"(
            Set the threshold for eager reads (default: 2048 bytes).

            Parameters
            ----------
            size (int): Threshold size in bytes.
            )",
            "size"_a)
        .def("__bool__", [](const warabi::TargetHandle& handle) {
            return static_cast<bool>(handle);
        });
}
