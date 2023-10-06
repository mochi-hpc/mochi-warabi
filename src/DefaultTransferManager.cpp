/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/TransferManager.hpp"

namespace warabi {

class DefaultTransferManager : public TransferManager {

    public:

    DefaultTransferManager() = default;
    DefaultTransferManager(DefaultTransferManager&&) = default;
    DefaultTransferManager(const DefaultTransferManager&) = default;
    DefaultTransferManager& operator=(DefaultTransferManager&&) = default;
    DefaultTransferManager& operator=(const DefaultTransferManager&) = default;
    virtual ~DefaultTransferManager() = default;

    std::string getConfig() const override {
        return "{}";
    }

    Result<bool> pull(
            WritableRegion& region,
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            thallium::bulk data,
            thallium::endpoint address,
            size_t bulkOffset,
            bool persist) override {
        return region.write(regionOffsetSizes, data, address, bulkOffset, persist);
    }

    Result<bool> push(
            ReadableRegion& region,
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            thallium::bulk data,
            thallium::endpoint address,
            size_t bulkOffset) override {
        return region.read(regionOffsetSizes, data, address, bulkOffset);
    }

    using json = nlohmann::json;

    static Result<std::unique_ptr<TransferManager>> create(
            const thallium::engine& engine, const json& config) {
        (void)engine;
        (void)config;
        Result<std::unique_ptr<TransferManager>> result;
        result.value() = std::make_unique<DefaultTransferManager>();
        return result;
    }

    static Result<bool> validate(const json& config) {
        (void)config;
        return Result<bool>{};
    }
};

WARABI_REGISTER_TRANSFER_MANAGER(__default__, DefaultTransferManager);

}
