/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "alpha/ResourceHandle.hpp"
#include "alpha/Result.hpp"
#include "alpha/Exception.hpp"

#include "AsyncRequestImpl.hpp"
#include "ClientImpl.hpp"
#include "ResourceHandleImpl.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace alpha {

ResourceHandle::ResourceHandle() = default;

ResourceHandle::ResourceHandle(const std::shared_ptr<ResourceHandleImpl>& impl)
: self(impl) {}

ResourceHandle::ResourceHandle(const ResourceHandle&) = default;

ResourceHandle::ResourceHandle(ResourceHandle&&) = default;

ResourceHandle& ResourceHandle::operator=(const ResourceHandle&) = default;

ResourceHandle& ResourceHandle::operator=(ResourceHandle&&) = default;

ResourceHandle::~ResourceHandle() = default;

ResourceHandle::operator bool() const {
    return static_cast<bool>(self);
}

Client ResourceHandle::client() const {
    return Client(self->m_client);
}

void ResourceHandle::computeSum(
        int32_t x, int32_t y,
        int32_t* sum,
        AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid alpha::ResourceHandle object");
    auto& rpc = self->m_client->m_compute_sum;
    auto& ph  = self->m_ph;
    auto& resource_id = self->m_resource_id;
    if(req == nullptr) { // synchronous call
        Result<int32_t> response = rpc.on(ph)(resource_id, x, y);
        response.andThen([sum](int32_t s) { if(sum) *sum = s; });
    } else { // asynchronous call
        auto async_response = rpc.on(ph).async(resource_id, x, y);
        auto async_request_impl =
            std::make_shared<AsyncRequestImpl>(std::move(async_response));
        async_request_impl->m_wait_callback =
            [sum](AsyncRequestImpl& async_request_impl) {
                Result<int32_t> response =
                    async_request_impl.m_async_response.wait();
                response.andThen([sum](int32_t s) { if(sum) *sum = s; });
            };
        *req = AsyncRequest(std::move(async_request_impl));
    }
}

}
