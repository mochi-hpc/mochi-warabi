/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/Exception.hpp"
#include "warabi/AsyncRequest.hpp"
#include "AsyncRequestImpl.hpp"

namespace warabi {

AsyncRequest::AsyncRequest() = default;

AsyncRequest::AsyncRequest(const std::shared_ptr<AsyncRequestImpl>& impl)
: self(impl) {}

AsyncRequest::AsyncRequest(const AsyncRequest& other) = default;

AsyncRequest::AsyncRequest(AsyncRequest&& other) {
    self = std::move(other.self);
    other.self = nullptr;
}

AsyncRequest::~AsyncRequest() {
    if(self && self.unique()) {
        wait();
    }
}

AsyncRequest& AsyncRequest::operator=(const AsyncRequest& other) {
    if(this == &other || self == other.self) return *this;
    if(self && self.unique()) {
        wait();
    }
    self = other.self;
    return *this;
}

AsyncRequest& AsyncRequest::operator=(AsyncRequest&& other) {
    if(this == &other || self == other.self) return *this;
    if(self && self.unique()) {
        wait();
    }
    self = std::move(other.self);
    other.self = nullptr;
    return *this;
}

void AsyncRequest::wait() const {
    if(not self) throw Exception("Invalid warabi::AsyncRequest object");
    if(self->m_waited) return;
    self->m_waited = true;
    self->m_wait_callback(*self);
}

bool AsyncRequest::completed() const {
    if(not self) throw Exception("Invalid warabi::AsyncRequest object");
    return self->m_async_response.received();
}

}
