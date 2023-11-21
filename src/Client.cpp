/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/Exception.hpp"
#include "warabi/Client.hpp"
#include "warabi/TargetHandle.hpp"
#include "warabi/Result.hpp"

#include "ClientImpl.hpp"
#include "TargetHandleImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace warabi {

Client::Client() = default;

Client::Client(const tl::engine& engine)
: self(std::make_shared<ClientImpl>(engine)) {}

Client::Client(margo_instance_id mid)
: self(std::make_shared<ClientImpl>(mid)) {}

Client::Client(const std::shared_ptr<ClientImpl>& impl)
: self(impl) {}

Client::Client(Client&& other) = default;

Client& Client::operator=(Client&& other) = default;

Client::Client(const Client& other) = default;

Client& Client::operator=(const Client& other) = default;


Client::~Client() = default;

const tl::engine& Client::engine() const {
    return self->m_engine;
}

Client::operator bool() const {
    return static_cast<bool>(self);
}

TargetHandle Client::makeTargetHandle(
        const std::string& address,
        uint16_t provider_id) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    return std::make_shared<TargetHandleImpl>(self, std::move(ph));
}

std::string Client::getConfig() const {
    return "{}";
}

}
