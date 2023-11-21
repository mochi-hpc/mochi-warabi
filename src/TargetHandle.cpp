/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/TargetHandle.hpp"
#include "warabi/Result.hpp"
#include "warabi/Exception.hpp"

#include "AsyncRequestImpl.hpp"
#include "ClientImpl.hpp"
#include "TargetHandleImpl.hpp"
#include "BufferWrapper.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace warabi {

TargetHandle::TargetHandle() = default;

TargetHandle::TargetHandle(const std::shared_ptr<TargetHandleImpl>& impl)
: self(impl) {}

TargetHandle::TargetHandle(const TargetHandle&) = default;

TargetHandle::TargetHandle(TargetHandle&&) = default;

TargetHandle& TargetHandle::operator=(const TargetHandle&) = default;

TargetHandle& TargetHandle::operator=(TargetHandle&&) = default;

TargetHandle::~TargetHandle() = default;

TargetHandle::operator bool() const {
    return static_cast<bool>(self);
}

Client TargetHandle::client() const {
    return Client(self->m_client);
}

void TargetHandle::setEagerWriteThreshold(size_t size) {
    if(not self) throw Exception("Invalid warabi::TargetHandle object");
    self->m_eager_write_threshold = size;
}

void TargetHandle::setEagerReadThreshold(size_t size) {
    if(not self) throw Exception("Invalid warabi::TargetHandle object");
    self->m_eager_read_threshold = size;
}

void TargetHandle::create(RegionID* region, size_t size,
                          AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid warabi::TargetHandle object");
    auto& rpc = self->m_client->m_create;
    auto& ph  = self->m_ph;
    auto async_response = rpc.on(ph).async(size);
    if(req == nullptr) { // synchronous call
        Result<RegionID> response = async_response.wait();
        if(region) *region = std::move(response).valueOrThrow();
    } else { // asynchronous call
        auto async_request_impl =
            std::make_shared<AsyncRequestImpl>(std::move(async_response));
        async_request_impl->m_wait_callback =
            [region](AsyncRequestImpl& async_request_impl) {
                Result<RegionID> response = async_request_impl.m_async_response.wait();
                if(region) *region = std::move(response).value();
            };
        *req = AsyncRequest(std::move(async_request_impl));
    }
}

void TargetHandle::write(const RegionID& region,
                         size_t regionOffset,
                         const char* data, size_t size,
                         bool persist,
                         AsyncRequest* req) const
{
    std::vector<std::pair<size_t, size_t>> regionOffsetSizes{{regionOffset, size}};
    write(region, regionOffsetSizes, data,  persist, req);
}

void TargetHandle::write(const RegionID& region,
                         const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
                         const char* data,
                         bool persist,
                         AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid warabi::TargetHandle object");
    size_t size = std::accumulate(regionOffsetSizes.begin(),
                                  regionOffsetSizes.end(), (size_t)0,
        [](size_t s, const std::pair<size_t, size_t>& segment) {
            return s + segment.second;
        });
    if(size >= self->m_eager_write_threshold) {
        auto bulk = self->m_client->m_engine.expose(
                {{const_cast<char*>(data), size}}, tl::bulk_mode::read_only);
        write(region, regionOffsetSizes, std::move(bulk), "", 0, persist, req);
        return;
    }
    // eager path
    auto& rpc = self->m_client->m_write_eager;
    auto& ph  = self->m_ph;
    auto buffer = BufferWrapper::Ref(data, size);
    auto async_response = rpc.on(ph).async(region, regionOffsetSizes, buffer, persist);
    if(req == nullptr) { // synchronous call
        Result<bool> response = async_response.wait();
        response.check();
    } else { // asynchronous call
        auto async_request_impl =
            std::make_shared<AsyncRequestImpl>(std::move(async_response));
        async_request_impl->m_wait_callback =
            [](AsyncRequestImpl& async_request_impl) {
                Result<bool> response = async_request_impl.m_async_response.wait();
                response.check();
            };
        *req = AsyncRequest(std::move(async_request_impl));
    }
}

void TargetHandle::write(const RegionID& region,
                         size_t regionOffset,
                         thallium::bulk data,
                         const std::string& address,
                         size_t bulkOffset, size_t size,
                         bool persist,
                         AsyncRequest* req) const
{
    std::vector<std::pair<size_t, size_t>> regionOffsetSizes{{regionOffset, size}};
    write(region, regionOffsetSizes, data, address, bulkOffset, persist, req);
}

void TargetHandle::write(const RegionID& region,
                         const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
                         thallium::bulk data,
                         const std::string& address,
                         size_t bulkOffset,
                         bool persist,
                         AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid warabi::TargetHandle object");
    auto& rpc = self->m_client->m_write;
    auto& ph  = self->m_ph;
    auto async_response = rpc.on(ph).async(region, regionOffsetSizes, data, address, bulkOffset, persist);
    if(req == nullptr) { // synchronous call
        Result<bool> response = async_response.wait();
        response.check();
    } else { // asynchronous call
        auto async_request_impl =
            std::make_shared<AsyncRequestImpl>(std::move(async_response));
        async_request_impl->m_wait_callback =
            [](AsyncRequestImpl& async_request_impl) {
                Result<bool> response = async_request_impl.m_async_response.wait();
                response.check();
            };
        *req = AsyncRequest(std::move(async_request_impl));
    }
}

void TargetHandle::persist(const RegionID& region,
                           size_t offset, size_t size,
                           AsyncRequest* req) const
{
    persist(region, {{offset, size}}, req);
}

void TargetHandle::persist(const RegionID& region,
                           const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
                           AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid warabi::TargetHandle object");
    auto& rpc = self->m_client->m_persist;
    auto& ph  = self->m_ph;
    auto async_response = rpc.on(ph).async(region, regionOffsetSizes);
    if(req == nullptr) { // synchronous call
        Result<bool> response = async_response.wait();
        response.check();
    } else { // asynchronous call
        auto async_request_impl =
            std::make_shared<AsyncRequestImpl>(std::move(async_response));
        async_request_impl->m_wait_callback =
            [](AsyncRequestImpl& async_request_impl) {
                Result<bool> response = async_request_impl.m_async_response.wait();
                response.check();
            };
        *req = AsyncRequest(std::move(async_request_impl));
    }
}

void TargetHandle::createAndWrite(RegionID* region,
                                  const char* data, size_t size,
                                  bool persist,
                                  AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid warabi::TargetHandle object");
    if(size >= self->m_eager_write_threshold) {
        auto bulk = self->m_client->m_engine.expose(
                {{const_cast<char*>(data), size}}, tl::bulk_mode::read_only);
        createAndWrite(region, std::move(bulk), "", 0, size, persist, req);
        return;
    }
    // eager path
    auto& rpc = self->m_client->m_create_write_eager;
    auto& ph  = self->m_ph;
    auto async_response = rpc.on(ph).async(
        BufferWrapper::Ref(data, size), persist);
    if(req == nullptr) { // synchronous call
        Result<RegionID> response = async_response.wait();
        if(region) *region = std::move(response).valueOrThrow();
        else response.check();
    } else { // asynchronous call
        auto async_request_impl =
            std::make_shared<AsyncRequestImpl>(std::move(async_response));
        async_request_impl->m_wait_callback =
            [region](AsyncRequestImpl& async_request_impl) {
                Result<RegionID> response = async_request_impl.m_async_response.wait();
                if(region) *region = std::move(response).valueOrThrow();
                else response.check();
            };
        *req = AsyncRequest(std::move(async_request_impl));
    }
}

void TargetHandle::createAndWrite(
        RegionID* region,
        thallium::bulk data,
        const std::string& address,
        size_t bulkOffset, size_t size,
        bool persist,
        AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid warabi::TargetHandle object");
    auto& rpc = self->m_client->m_create_write;
    auto& ph  = self->m_ph;
    auto async_response = rpc.on(ph).async(data, address, bulkOffset, size, persist);
    if(req == nullptr) { // synchronous call
        Result<RegionID> response = async_response.wait();
        if(region) *region = std::move(response).valueOrThrow();
        else response.check();
    } else { // asynchronous call
        auto async_request_impl =
            std::make_shared<AsyncRequestImpl>(std::move(async_response));
        async_request_impl->m_wait_callback =
            [region](AsyncRequestImpl& async_request_impl) {
                Result<RegionID> response = async_request_impl.m_async_response.wait();
                if(region) *region = std::move(response).valueOrThrow();
                else response.check();
            };
        *req = AsyncRequest(std::move(async_request_impl));
    }
}

void TargetHandle::read(
        const RegionID& region,
        size_t regionOffset,
        char* data, size_t size,
        AsyncRequest* req) const
{
    read(region, {{regionOffset, size}}, data, req);
}

void TargetHandle::read(
        const RegionID& region,
        const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
        char* data,
        AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid warabi::TargetHandle object");
    size_t size = std::accumulate(regionOffsetSizes.begin(),
                                  regionOffsetSizes.end(), (size_t)0,
        [](size_t s, const std::pair<size_t, size_t>& segment) {
            return s + segment.second;
        });
    if(size >= self->m_eager_read_threshold) {
        auto bulk = self->m_client->m_engine.expose({{data, size}}, tl::bulk_mode::write_only);
        read(region, regionOffsetSizes, std::move(bulk), "", 0, req);
        return;
    }
    // eager path
    auto& rpc = self->m_client->m_read_eager;
    auto& ph  = self->m_ph;
    auto async_response = rpc.on(ph).async(region, regionOffsetSizes);
    if(req == nullptr) { // synchronous call
        Result<BufferWrapper> response = async_response.wait();
        response.check();
        // TODO we are forced to do a copy here, ideally thallium's packed_data
        // should give us a way to deserialize directly into an existing BufferWrapper
        std::memcpy(data, response.value().data(), size);
    } else { // asynchronous call
        auto async_request_impl =
            std::make_shared<AsyncRequestImpl>(std::move(async_response));
        async_request_impl->m_wait_callback =
            [data, size](AsyncRequestImpl& async_request_impl) {
                Result<BufferWrapper> response = async_request_impl.m_async_response.wait();
                response.check();
                // TODO same as above
                std::memcpy(data, response.value().data(), size);
            };
        *req = AsyncRequest(std::move(async_request_impl));
    }
}

void TargetHandle::read(
        const RegionID& region,
        size_t regionOffset,
        thallium::bulk data,
        const std::string& address,
        size_t bulkOffset, size_t size,
        AsyncRequest* req) const
{
    read(region, {{regionOffset, size}}, data, address, bulkOffset, req);
}

void TargetHandle::read(
        const RegionID& region,
        const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
        thallium::bulk data,
        const std::string& address,
        size_t bulkOffset,
        AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid warabi::TargetHandle object");
    auto& rpc = self->m_client->m_read;
    auto& ph  = self->m_ph;
    auto async_response = rpc.on(ph).async(region, regionOffsetSizes, data, address, bulkOffset);
    if(req == nullptr) { // synchronous call
        Result<bool> response = async_response.wait();
        response.check();
    } else { // asynchronous call
        auto async_request_impl =
            std::make_shared<AsyncRequestImpl>(std::move(async_response));
        async_request_impl->m_wait_callback =
            [](AsyncRequestImpl& async_request_impl) {
                Result<bool> response = async_request_impl.m_async_response.wait();
                response.check();
            };
        *req = AsyncRequest(std::move(async_request_impl));
    }
}

void TargetHandle::erase(const RegionID& region,
                         AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid warabi::TargetHandle object");
    auto& rpc = self->m_client->m_erase;
    auto& ph  = self->m_ph;
    auto async_response = rpc.on(ph).async(region);
    if(req == nullptr) { // synchronous call
        Result<bool> response = async_response.wait();
        response.check();
    } else { // asynchronous call
        auto async_request_impl =
            std::make_shared<AsyncRequestImpl>(std::move(async_response));
        async_request_impl->m_wait_callback =
            [](AsyncRequestImpl& async_request_impl) {
                Result<bool> response = async_request_impl.m_async_response.wait();
                response.check();
            };
        *req = AsyncRequest(std::move(async_request_impl));
    }
}

}
