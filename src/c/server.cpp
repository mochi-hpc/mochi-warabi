/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/server.h"
#include "error.hpp"
#include "warabi/Provider.hpp"

struct warabi_provider : public warabi::Provider {
    template<typename ... Args>
    warabi_provider(Args&&... args)
    : warabi::Provider(std::forward<Args>(args)...) {}
};

extern "C" warabi_err_t warabi_provider_register(
    warabi_provider_t* provider,
    margo_instance_id mid,
    uint16_t provider_id,
    const char* config,
    struct warabi_provider_init_args* args) {
    try {
        auto p = new warabi_provider{
            mid, provider_id,
            config ? config : "{}",
            thallium::pool{args ? args->pool : ABT_POOL_NULL},
            args ? args->remi_cl : nullptr,
            args ? args->remi_pr : nullptr
        };
        if(provider) *provider = p;
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_provider_deregister(
    warabi_provider_t provider) {
    delete provider;
    return nullptr;
}

extern "C" char* warabi_provider_get_config(warabi_provider_t provider) {
    return strdup(provider->getConfig().c_str());
}
