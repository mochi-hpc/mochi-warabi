/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_SERVER_H
#define __WARABI_SERVER_H

#include <margo.h>
#include <warabi/error.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct remi_client* remi_client_t;     // forward-define without including <remi-client.h>
typedef struct remi_provider* remi_provider_t; // forward-define without including <remi-server.h>

typedef struct warabi_provider* warabi_provider_t;
#define WARABI_PROVIDER_IGNORE ((warabi_provider_t)0)

struct warabi_provider_init_args {
    const char*     config;
    ABT_pool        pool;
    remi_client_t   remi_cl;
    remi_provider_t remi_pr;
};

/**
 * @brief Register a Warabi provider.
 *
 * @param[out] provider Provider.
 * @param[in] mid Margo instance id.
 * @param[in] provider_id Provider id.
 * @param[in] args Optional extra arguments.
 *
 * @return warabi_err_t handle.
 */
warabi_err_t warabi_provider_register(
    warabi_provider_t* provider,
    margo_instance_id mid,
    uint16_t provider_id,
    struct warabi_provider_init_args* args);

/**
 * @brief Deregister the provider.
 */
warabi_err_t warabi_provider_deregister(warabi_provider_t provider);

/**
 * @brief Get the provider configuration. The caller is responsible
 * for freeing the returned string.
 */
char* warabi_provider_get_config(warabi_provider_t provider);

#ifdef __cplusplus
}
#endif

#endif
