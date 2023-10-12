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
                      const std::string& target_config) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    Result<UUID> result = self->m_add_target.on(ph)(target_type, target_config);
    return std::move(result).valueOrThrow();
}

void Admin::migrateTarget(const std::string& address,
                          uint16_t provider_id,
                          const UUID& target_id,
                          const std::string& dest_address,
                          uint16_t dest_provider_id,
                          const MigrationOptions& options) const {
    auto endpoint = self->m_engine.lookup(address);
    auto ph       = tl::provider_handle(endpoint, provider_id);
    Result<bool> result = self->m_migrate_target.on(ph)(target_id, dest_address, dest_provider_id, options);
    result.check();
}

void Admin::addTransferManager(const std::string& address,
                               uint16_t provider_id,
                               const std::string& name,
                               const std::string& type,
                               const std::string& config) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    Result<bool> result = self->m_add_transfer_manager.on(ph)(name, type, config);
    result.check();
}

void Admin::removeTarget(const std::string& address,
                         uint16_t provider_id,
                         const UUID& target_id) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    Result<bool> result = self->m_remove_target.on(ph)(target_id);
    result.check();
}

void Admin::destroyTarget(const std::string& address,
                          uint16_t provider_id,
                          const UUID& target_id) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    Result<bool> result = self->m_destroy_target.on(ph)(target_id);
    result.check();
}

}
