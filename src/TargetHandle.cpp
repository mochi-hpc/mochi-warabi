/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/TargetHandle.hpp"
#include "warabi/Result.hpp"
#include "warabi/Exception.hpp"

#include "AsyncRequestImpl.hpp"
#include "ClientImpl.hpp"
#include "TargetHandleImpl.hpp"

#include <thallium/serialization/stl/string.hpp>
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

void TargetHandle::computeSum(
        int32_t x, int32_t y,
        int32_t* sum,
        AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid warabi::TargetHandle object");
    auto& rpc = self->m_client->m_compute_sum;
    auto& ph  = self->m_ph;
    auto& target_id = self->m_target_id;
    if(req == nullptr) { // synchronous call
        Result<int32_t> response = rpc.on(ph)(target_id, x, y);
        response.andThen([sum](int32_t s) { if(sum) *sum = s; });
    } else { // asynchronous call
        auto async_response = rpc.on(ph).async(target_id, x, y);
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
