/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_PROVIDER_HPP
#define __WARABI_PROVIDER_HPP

#include <thallium.hpp>
#include <memory>

typedef struct remi_client* remi_client_t;     // forward-define without including <remi-client.h>
typedef struct remi_provider* remi_provider_t; // forward-define without including <remi-server.h>

namespace warabi {

namespace tl = thallium;

class ProviderImpl;

/**
 * @brief A Provider is an object that can receive RPCs
 * and dispatch them to specific targets.
 */
class Provider {

    public:

    /**
     * @brief Constructor.
     *
     * @param engine Thallium engine to use to receive RPCs.
     * @param provider_id Provider id.
     * @param config JSON-formatted configuration.
     * @param pool Argobots pool to use to handle RPCs.
     * @param remi_cl Remi client for migration.
     * @param remi_pr Remi provider for migration.
     */
    Provider(const tl::engine& engine,
             uint16_t provider_id,
             const std::string& config,
             const tl::pool& pool = tl::pool(),
             remi_client_t remi_cl = nullptr,
             remi_provider_t remi_pr = nullptr);

    /**
     * @brief Constructor.
     *
     * @param mid Margo instance id to use to receive RPCs.
     * @param provider_id Provider id.
     * @param config JSON-formatted configuration.
     * @param pool Argobots pool to use to handle RPCs.
     * @param remi_cl Remi client for migration.
     * @param remi_pr Remi provider for migration.
     */
    Provider(margo_instance_id mid,
             uint16_t provider_id,
             const std::string& config,
             const tl::pool& pool = tl::pool(),
             remi_client_t remi_cl = nullptr,
             remi_provider_t remi_pr = nullptr);

    /**
     * @brief Copy-constructor is deleted.
     */
    Provider(const Provider&) = delete;

    /**
     * @brief Move-constructor.
     */
    Provider(Provider&&);

    /**
     * @brief Copy-assignment operator is deleted.
     */
    Provider& operator=(const Provider&) = delete;

    /**
     * @brief Move-assignment operator is deleted.
     */
    Provider& operator=(Provider&&) = delete;

    /**
     * @brief Destructor.
     */
    ~Provider();

    /**
     * @brief Return a JSON-formatted configuration of the provider.
     *
     * @return JSON formatted string.
     */
    std::string getConfig() const;

    /**
     * @brief Checks whether the Provider instance is valid.
     */
    operator bool() const;

    private:

    std::shared_ptr<ProviderImpl> self;
};

}

#endif
