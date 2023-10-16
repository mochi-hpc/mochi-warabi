/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __PMEM_BACKEND_HPP
#define __PMEM_BACKEND_HPP

#include <warabi/Backend.hpp>
#include <libpmemobj.h>

namespace warabi {

using json = nlohmann::json;

/**
 * Pmem-based implementation of an warabi Backend.
 */
class PmemTarget : public warabi::Backend {

    thallium::engine               m_engine;
    json                           m_config;
    PMEMobjpool*                   m_pmem_pool;
    std::string                    m_filename;


    public:

    /**
     * @brief Constructor.
     */
    PmemTarget(thallium::engine engine, const json& config, PMEMobjpool* pool);

    /**
     * @brief Move-constructor.
     */
    PmemTarget(PmemTarget&&) = default;

    /**
     * @brief Move-assignment operator.
     */
    PmemTarget& operator=(PmemTarget&&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~PmemTarget();

    /**
     * @brief Get the target's configuration as a JSON-formatted string.
     */
    std::string getConfig() const override;

    /**
     * @brief Create a region of a given size and return
     * an std::unique_ptr to a WritableRegion, or nullptr
     * if the operation failed.
     *
     * @param size Size of the region to create.
     *
     * @return std::unique_ptr<WritableRegion>.
     */
    Result<std::unique_ptr<WritableRegion>> create(size_t size) override;

    /**
     * @brief Request access to a particular region for writing.
     * If the region does not exist, returns a nullptr.
     */
    Result<std::unique_ptr<WritableRegion>> write(const RegionID& region, bool persist) override;

    /**
     * @brief Request access to a particular region for reading.
     * If the region does not exist, returns a nullptr.
     */
    Result<std::unique_ptr<ReadableRegion>> read(const RegionID& region) override;

    /**
     * @see TopicHandle::erase
     */
    Result<bool> erase(const RegionID& region) override;

    /**
     * @brief Destroy the underlying storage.
     */
    Result<bool> destroy() override;

    /**
     * @brief Start a migration.
     */
    Result<std::unique_ptr<MigrationHandle>> startMigration() override;

    /**
     * @brief Static factory function used by the TargetFactory to
     * create a PmemTarget.
     *
     * @param engine Thallium engine
     * @param config JSON configuration for the target
     *
     * @return a unique_ptr to a target
     */
    static Result<std::unique_ptr<warabi::Backend>> create(const thallium::engine& engine, const json& config);

    /**
     * @brief Recovers after migration.
     */
    static Result<std::unique_ptr<warabi::Backend>> recover(
            const thallium::engine& engine, const json& config,
            const std::vector<std::string>& filenames);

    /**
     * @brief Validates that the configuration is correct for this backend.
     */
    static Result<bool> validate(const json& config);
};

}

#endif
