/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_TRANSFER_MANAGER_HPP
#define __WARABI_TRANSFER_MANAGER_HPP

#include <warabi/Result.hpp>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <nlohmann/json.hpp>
#include <thallium.hpp>

#include <warabi/Backend.hpp>

/**
 * @brief Helper class to register backend types into the TransferManagerfactory.
 */
template<typename Type>
class __WarabiTransferManagerRegistration;

namespace warabi {

/**
 * @brief Interface for transfer managers. To build a new TransferManager,
 * implement a class MyType that inherits from TransferManager, and put
 * WARABI_REGISTER_TRANSFER_MANAGER(mytype, MyType); in a cpp file
 * that includes your class' header file.
 *
 * Your class should also have a static functions to create
 * a TransferManager:
 *
 * std::unique_ptr<TransferManager> create(const json& config)
 */
class TransferManager {

    template<typename TransferManagerType>
    friend class ::__WarabiTransferManagerRegistration;

    std::string m_name;

    public:

    /**
     * @brief Constructor.
     */
    TransferManager() = default;

    /**
     * @brief Move-constructor.
     */
    TransferManager(TransferManager&&) = default;

    /**
     * @brief Copy-constructor.
     */
    TransferManager(const TransferManager&) = default;

    /**
     * @brief Move-assignment operator.
     */
    TransferManager& operator=(TransferManager&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    TransferManager& operator=(const TransferManager&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~TransferManager() = default;

    /**
     * @brief Return the name of class.
     */
    const std::string& name() const {
        return m_name;
    }

    /**
     * @brief Returns a JSON-formatted configuration string.
     */
    virtual std::string getConfig() const = 0;

    /**
     * @brief Pull data from the bulk handle and into the
     * region wrapped by the WritableRegion object.
     *
     * @param[in] region Region to pull into.
     * @param[in] regionOffsetSizes ranges in the region to pull into.
     * @param[in] data Remote bulk to pull from.
     * @param[in] address Address ot the remote process.
     * @param[in] bulkOffset Offset in the bulk handle.
     * @param[in] persist Whether to persist the data.
     *
     * @return a Result<bool> indicating the result of the operation.
     */
    virtual Result<bool> pull(
            WritableRegion& region,
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            thallium::bulk data,
            thallium::endpoint address,
            size_t bulkOffset,
            bool persist) = 0;

    /**
     * @brief Push data from the ReadableRegion and to the
     * remote bulk handle.
     *
     * @param[in] region Region to push from.
     * @param[in] regionOffsetSizes ranges in the region to pull into.
     * @param[in] data Remote bulk to push to.
     * @param[in] address Address ot the remote process.
     * @param[in] bulkOffset Offset in the bulk handle.
     *
     * @return a Result<bool> indicating the result of the operation.
     */
    virtual Result<bool> push(
            ReadableRegion& region,
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            thallium::bulk data,
            thallium::endpoint address,
            size_t bulkOffset) = 0;
};

/**
 * @brief The TransferManagerFactory contains functions to create
 * TransferManagers.
 */
class TransferManagerFactory {

    template<typename TransferManagerType>
    friend class ::__WarabiTransferManagerRegistration;

    using json = nlohmann::json;

    TransferManagerFactory() = default;

    public:

    static TransferManagerFactory& instance() {
        static TransferManagerFactory tmf;
        return tmf;
    }

    /**
     * @brief Validate that the JSON configuration has the expected schema.
     *
     * @param name Type of transfer manager to use.
     * @param config Configuration object to validate.
     *
     * @return true if the JSON configuration is valid, false otherwise.
     */
    static Result<bool> validateConfig(const std::string& name, const json& config);

    /**
     * @brief Creates a TransferManager and returns a unique_ptr to the created instance.
     *
     * @param name Name of the type to use.
     * @param engine Thallium engine.
     * @param config Configuration object to pass to the create function.
     *
     * @return a unique_ptr to the created TransferManager.
     */
    static Result<std::unique_ptr<TransferManager>> createTransferManager(
        const std::string& name,
        const thallium::engine& engine,
        const json& config);

    private:

    std::unordered_map<
        std::string,
        std::function<Result<std::unique_ptr<TransferManager>>(const thallium::engine&, const json&)>> create_fn;

    std::unordered_map<
        std::string,
        std::function<Result<bool>(const json&)>> validate_fn;
};

} // namespace warabi


#define WARABI_REGISTER_TRANSFER_MANAGER(__name, __type) \
    static __WarabiTransferManagerRegistration<__type> \
    __warabi ## __name ## _transfer_manager( #__name )

template<typename TransferManagerType>
class __WarabiTransferManagerRegistration {

    using json = nlohmann::json;

    public:

    __WarabiTransferManagerRegistration(const std::string& name)
    {
        warabi::TransferManagerFactory::instance().create_fn[name] =
            [name](const thallium::engine& engine, const json& config) {
                auto r = TransferManagerType::create(engine, config);
                if(!r.success()) return r;
                r.value()->m_name = name;
                return r;
            };
        warabi::TransferManagerFactory::instance().validate_fn[name] =
            [name](const json& config) {
                return TransferManagerType::validate(config);
            };
    }
};

#endif
