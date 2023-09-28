/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/Admin.hpp"
#include "warabi/Exception.hpp"
#include "warabi/Result.hpp"

#include "AdminImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace warabi {

Admin::Admin() = default;

Admin::Admin(const tl::engine& engine)
: self(std::make_shared<AdminImpl>(engine)) {}

Admin::Admin(margo_instance_id mid)
: self(std::make_shared<AdminImpl>(mid)) {}

Admin::Admin(Admin&& other) = default;

Admin& Admin::operator=(Admin&& other) = default;

Admin::Admin(const Admin& other) = default;

Admin& Admin::operator=(const Admin& other) = default;


Admin::~Admin() = default;

Admin::operator bool() const {
    return static_cast<bool>(self);
}

UUID Admin::addTarget(const std::string& address,
                      uint16_t provider_id,
                      const std::string& target_type,
                      const std::string& target_config,
                      const std::string& token) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    Result<UUID> result = self->m_add_target.on(ph)(token, target_type, target_config);
    return std::move(result).valueOrThrow();
}

void Admin::removeTarget(const std::string& address,
                         uint16_t provider_id,
                         const UUID& target_id,
                         const std::string& token) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    Result<bool> result = self->m_remove_target.on(ph)(token, target_id);
    result.check();
}

void Admin::destroyTarget(const std::string& address,
                          uint16_t provider_id,
                          const UUID& target_id,
                          const std::string& token) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    Result<bool> result = self->m_destroy_target.on(ph)(token, target_id);
    result.check();
}

void Admin::shutdownServer(const std::string& address) const {
    auto ep = self->m_engine.lookup(address);
    self->m_engine.shutdown_remote_engine(ep);
}

}
