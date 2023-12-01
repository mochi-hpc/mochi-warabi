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

    /**
     * @brief Migrate the internal target to a destination provider.
     * The options argument should be a JSON string with the following
     * optional keys:
     *
     * - "new_root" (string): path where to place the target in the
     *   destination (defaults to the same path as in the source);
     * - "transfer_size" (int): size of individual transfers used by
     *   REMI (defaults to using a single transfer for the full target);
     * - "merge_config" (object): the content of this field will be
     *   merged with the target's configuration (defaults to an empty
     *   object);
     * - "remove_source" (bool): whether to remove the target in the
     *   source provider (defaults to true);
     *
     * @param address
     * @param provider_id
     * @param options
     */
    void migrateTarget(const std::string& address,
                       uint16_t provider_id,
                       const std::string& options);

    private:

    std::shared_ptr<ProviderImpl> self;
};

}

#endif
