/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_BACKEND_HPP
#define __WARABI_BACKEND_HPP

#include <warabi/Result.hpp>
#include <warabi/Migration.hpp>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <nlohmann/json.hpp>
#include <thallium.hpp>

#include <warabi/RegionID.hpp>

/**
 * @brief Helper class to register backend types into the backend factory.
 */
template<typename BackendType>
class __WarabiBackendRegistration;

namespace warabi {

/**
 * @brief Abstract class representing a handle to a region in
 * a given Backend. Each Backend implementation will typically
 * have implementations for WritableRegion and ReadableRegion.
 */
class Region {

    public:

    virtual ~Region() {}

    /**
     * @brief Return the RegionID of the region.
     */
    virtual Result<RegionID> getRegionID() = 0;
};

class WritableRegion : public Region {

    public:

    /**
     * @see TopicHandle::write
     */
    virtual Result<bool> write(
        const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
        thallium::bulk data,
        const thallium::endpoint& address,
        size_t bulkOffset,
        bool persist) = 0;

    /**
     * @see TopicHandle::write
     */
    virtual Result<bool> write(
        const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
        const void* data, bool persist) = 0;

    /**
     * @see TopicHandle::persist
     */
    virtual Result<bool> persist(
        const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes) = 0;

};

class ReadableRegion : public Region {

    public:

    /**
     * @see TopicHandle::read
     */
    virtual Result<bool> read(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            thallium::bulk data,
            const thallium::endpoint& address,
            size_t bulkOffset) = 0;

    /**
     * @see TopicHandle::read
     */
    virtual Result<bool> read(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            void* data) = 0;
};

/**
 * @brief Interface for target backends. To build a new backend,
 * implement a class MyBackend that inherits from Backend, and put
 * WARABI_REGISTER_BACKEND(mybackend, MyBackend); in a cpp file
 * that includes your backend class' header file.
 *
 * Your backend class should also have two static functions to
 * respectively create and open a target:
 *
 * std::unique_ptr<Backend> create(const json& config)
 * std::unique_ptr<Backend> attach(const json& config)
 */
class Backend {

    template<typename BackendType>
    friend class ::__WarabiBackendRegistration;

    std::string m_name;

    public:

    /**
     * @brief Constructor.
     */
    Backend() = default;

    /**
     * @brief Move-constructor.
     */
    Backend(Backend&&) = default;

    /**
     * @brief Copy-constructor.
     */
    Backend(const Backend&) = default;

    /**
     * @brief Move-assignment operator.
     */
    Backend& operator=(Backend&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    Backend& operator=(const Backend&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~Backend() = default;

    /**
     * @brief Return the name of backend.
     */
    const std::string& name() const {
        return m_name;
    }

    /**
     * @brief Returns a JSON-formatted configuration string.
     */
    virtual std::string getConfig() const = 0;

    /**
     * @brief Create a region of a given size and return
     * an std::unique_ptr to a WritableRegion, or nullptr
     * if the operation failed.
     *
     * @param size Size of the region to create.
     *
     * @return std::unique_ptr<WritableRegion>.
     */
    virtual Result<std::unique_ptr<WritableRegion>> create(size_t size) = 0;

    /**
     * @brief Request access to a particular region for writing.
     * If the region does not exist, returns a nullptr.
     */
    virtual Result<std::unique_ptr<WritableRegion>> write(const RegionID& region, bool persist) = 0;

    /**
     * @brief Request access to a particular region for reading.
     * If the region does not exist, returns a nullptr.
     */
    virtual Result<std::unique_ptr<ReadableRegion>> read(const RegionID& region) = 0;

    /**
     * @see TopicHandle::erase
     */
    virtual Result<bool> erase(
            const RegionID& region) = 0;

    /**
     * @brief Destroys the underlying target.
     *
     * @return a Result<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    virtual Result<bool> destroy() = 0;

    /**
     * @brief Create a MigrationHandle to start a migration of the
     * target to another provider.
     *
     * @param removeSource Whether to delete the source target after completion.
     */
    virtual Result<std::unique_ptr<MigrationHandle>> startMigration(bool removeSource) = 0;

};

/**
 * @brief The TargetFactory contains functions to create
 * or open targets.
 */
class TargetFactory {

    template<typename BackendType>
    friend class ::__WarabiBackendRegistration;

    using json = nlohmann::json;

    TargetFactory() = default;

    public:


    /**
     * @brief Creates a target and returns a unique_ptr to the created instance.
     *
     * @param backend_name Name of the backend to use.
     * @param engine Thallium engine.
     * @param config Configuration object to pass to the backend's create function.
     *
     * @return a unique_ptr to the created Target.
     */
    static Result<std::unique_ptr<Backend>>
        createTarget(const std::string& backend_name,
                     const thallium::engine& engine,
                     const json& config);

    /**
     * @brief Recovers a target after migration.
     *
     * @param backend_name Name of the backend to use.
     * @param engine Thallium engine.
     * @param config Configuration object to pass to the backend's create function.
     * @param filenames List of files that were migrated.
     *
     * @return a unique_ptr to the recovered Target.
     */
    static Result<std::unique_ptr<Backend>>
        recoverTarget(const std::string& backend_name,
                     const thallium::engine& engine,
                     const json& config,
                     const std::vector<std::string>& filenames);

    /**
     * @brief Validate that the JSON configuration has the expected schema.
     *
     * @param backend_name Name of the backend to use.
     * @param config Configuration object to validate.
     *
     * @return true if the JSON configuration is valid, false otherwise.
     */
    static Result<bool> validateConfig(const std::string& backend_name, const json& config);

    /**
     * @brief Get a singleton instance.
     */
    static TargetFactory& instance() {
        static TargetFactory f;
        return f;
    }

    private:

    std::unordered_map<std::string,
        std::function<Result<std::unique_ptr<Backend>>(const thallium::engine&, const json&)>> create_fn;

    std::unordered_map<std::string,
        std::function<Result<std::unique_ptr<Backend>>(
            const thallium::engine&, const json&, const std::vector<std::string>&)>> recover_fn;

    std::unordered_map<std::string,
        std::function<Result<bool>(const json&)>> validate_fn;
};

} // namespace warabi


#define WARABI_REGISTER_BACKEND(__backend_name, __backend_type) \
    static __WarabiBackendRegistration<__backend_type> __warabi ## __backend_name ## _backend( #__backend_name )

template<typename BackendType>
class __WarabiBackendRegistration {

    using json = nlohmann::json;

    public:

    __WarabiBackendRegistration(const std::string& backend_name)
    {
        warabi::TargetFactory::instance().create_fn[backend_name] =
            [backend_name](const thallium::engine& engine, const json& config) {
                auto p = BackendType::create(engine, config);
                if(p.success()) p.value()->m_name = backend_name;
                return p;
        };
        warabi::TargetFactory::instance().recover_fn[backend_name] =
            [backend_name](const thallium::engine& engine, const json& config,
                           const std::vector<std::string>& filenames) {
                auto p = BackendType::recover(engine, config, filenames);
                if(p.success()) p.value()->m_name = backend_name;
                return p;
        };
        warabi::TargetFactory::instance().validate_fn[backend_name] =
            [backend_name](const json& config) {
                return BackendType::validate(config);
        };
    }
};

#endif
