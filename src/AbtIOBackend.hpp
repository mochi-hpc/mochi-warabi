/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __ABTIO_BACKEND_HPP
#define __ABTIO_BACKEND_HPP

#include <warabi/Backend.hpp>
#include <abt-io.h>

namespace warabi {

using json = nlohmann::json;

/**
 * AbtIO-based implementation of an warabi Backend.
 */
class AbtIOTarget : public warabi::Backend {

    public:

    thallium::engine               m_engine;
    json                           m_config;
    abt_io_instance_id             m_abtio;
    int                            m_fd;
    std::atomic<size_t>            m_file_size;
    std::string                    m_filename;
    bool                           m_sync;
    size_t                         m_alignment;

    /**
     * @brief Constructor.
     */
    AbtIOTarget(thallium::engine engine, const json& config,
                abt_io_instance_id abtio, int fd, size_t file_size);

    /**
     * @brief Move-constructor.
     */
    AbtIOTarget(AbtIOTarget&&) = default;

    /**
     * @brief Move-assignment operator.
     */
    AbtIOTarget& operator=(AbtIOTarget&&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~AbtIOTarget();

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
     * create a AbtIOTarget.
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
