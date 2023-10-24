/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_ADMIN_H
#define __WARABI_ADMIN_H

#include <margo.h>
#include <warabi/target_id.h>
#include <warabi/error.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct warabi_admin* warabi_admin_t;
#define WARABI_ADMIN_NULL ((warabi_admin_t)0)

/**
 * @brief Create a warabi_admint_t handle.
 *
 * @param[in] mid Margo instance id.
 * @param[out] admin Admin.
 */
warabi_err_t warabi_admin_create(margo_instance_id mid, warabi_admin_t* admin);

/**
 * @brief Free the admin handle.
 *
 * @param admin Admin handle to free.
 */
warabi_err_t warabi_admin_free(warabi_admin_t admin);

/**
 * @brief Adds a target on the target provider.
 * The config string must be a JSON object acceptable
 * by the desired backend's creation function.
 *
 * @param[in] admin Admin to use.
 * @param[in] address Address of the target provider.
 * @param[in] provider_id Provider id.
 * @param[in] type Type of the target to create.
 * @param[in] config JSON configuration for the target.
 * @param[out] id Resulting target ID.
 */
warabi_err_t warabi_admin_add_target(
        warabi_admin_t admin,
        const char* address,
        uint16_t provider_id,
        const char* type,
        const char* config,
        warabi_target_id id);

/**
 * @brief Removes an open target in the target provider.
 *
 * @param[in] admin Admin to use.
 * @param[in] address Address of the target provider.
 * @param[in] provider_id Provider id.
 * @param[in] target_id UUID of the target to close.
 */
warabi_err_t warabi_admin_remove_target(
        warabi_admin_t admin,
        const char* address,
        uint16_t provider_id,
        const warabi_target_id target_id);

/**
 * @brief Destroys an open target in the target provider.
 *
 * @param[in] admin Admin to use.
 * @param[in] address Address of the target provider.
 * @param[in] provider_id Provider id.
 * @param[in] target_id UUID of the target to destroy.
 */
warabi_err_t warabi_admin_destroy_target(
        warabi_admin_t admin,
        const char* address,
        uint16_t provider_id,
        const warabi_target_id target_id);

/**
 * @brief Adds a transfer manager on the target provider.
 * The config string must be a JSON object acceptable
 * by the desired transfer manager type's creation function.
 *
 * @param admin Admin to use.
 * @param address Address of the target provider.
 * @param provider_id Provider id.
 * @param name Name of the transfer manager to create.
 * @param type Type of the transfer manager to create.
 * @param config JSON configuration for the transfer manager.
 */
warabi_err_t warabi_admin_add_transfer_manager(
        warabi_admin_t admin,
        const char* address,
        uint16_t provider_id,
        const char* name,
        const char* type,
        const char* config);

/**
 * @brief Extra migration options.
 */
typedef struct {
    const char* new_root;
    size_t      transfer_size;
    const char* extra_config;
    bool        remove_source;
} warabi_migration_options;

/**
 * @brief Migrate a target from the source to the destination.
 *
 * @param admin Admin.
 * @param source_addr Source address.
 * @param source_provider_id Source provider ID.
 * @param target_id Target ID.
 * @param dest_addr Destination address.
 * @param dest_provider_id Destination provider ID.
 * @param options Options (may be null).
 */
warabi_err_t warabi_admin_migrate_target(
        warabi_admin_t admin,
        const char* source_addr,
        uint16_t source_provider_id,
        const warabi_target_id target_id,
        const char* dest_addr,
        uint16_t dest_provider_id,
        const warabi_migration_options* options);

#ifdef __cplusplus
}
#endif

#endif
