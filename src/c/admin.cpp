/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "error.hpp"
#include "warabi/admin.h"
#include "warabi/Admin.hpp"

struct warabi_admin : public warabi::Admin {

    template<typename ... Args>
    warabi_admin(Args&&... args)
    : warabi::Admin(std::forward<Args>(args)...) {}
};

extern "C" warabi_err_t warabi_admin_create(
        margo_instance_id mid, warabi_admin_t* admin) {
    try {
        *admin = new warabi_admin{mid};
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_admin_free(
        warabi_admin_t admin) {
    delete admin;
    return nullptr;
}

extern "C" warabi_err_t warabi_admin_add_target(
        warabi_admin_t admin,
        const char* address,
        uint16_t provider_id,
        const char* type,
        const char* config,
        warabi_target_id id) {
    try {
        warabi::UUID uuid = admin->addTarget(address, provider_id, type, config);
        std::memcpy(id, uuid.m_data, sizeof(uuid_t));
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_admin_remove_target(
        warabi_admin_t admin,
        const char* address,
        uint16_t provider_id,
        const warabi_target_id target_id) {
    try {
        admin->removeTarget(address, provider_id, target_id);
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_admin_destroy_target(
        warabi_admin_t admin,
        const char* address,
        uint16_t provider_id,
        const warabi_target_id target_id) {
    try {
        admin->destroyTarget(address, provider_id, target_id);
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_admin_add_transfer_manager(
        warabi_admin_t admin,
        const char* address,
        uint16_t provider_id,
        const char* name,
        const char* type,
        const char* config) {
    try {
        admin->addTransferManager(address, provider_id, name, type, config);
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_admin_migrate_target(
        warabi_admin_t admin,
        const char* source_addr,
        uint16_t source_provider_id,
        const warabi_target_id target_id,
        const char* dest_addr,
        uint16_t dest_provider_id,
        const warabi_migration_options* options) {
    try {
        warabi::MigrationOptions extra = {
            options && options->new_root ? options->new_root : "",
            options ? options->transfer_size : 0,
            options && options->extra_config ? options->extra_config : "{}",
            options ? options->remove_source : false
        };
        admin->migrateTarget(source_addr, source_provider_id, target_id, dest_addr, dest_provider_id, extra);
    } HANDLE_WARABI_ERROR;
}
