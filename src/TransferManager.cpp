/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/TransferManager.hpp"
#include <fmt/format.h>

namespace tl = thallium;

namespace warabi {

using json = nlohmann::json;

Result<std::unique_ptr<TransferManager>> TransferManagerFactory::createTransferManager(
        const std::string& name,
        const tl::engine& engine,
        const json& config) {
    auto it = instance().create_fn.find(name);
    if(it == instance().create_fn.end()) {
        Result<std::unique_ptr<TransferManager>> result;
        result.success() = false;
        result.error() = fmt::format("Unknown transfer manager type \"{}\"", name);
        return result;
    }
    auto& f = it->second;
    return f(engine, config);
}

Result<bool> TransferManagerFactory::validateConfig(
        const std::string& name,
        const json& config) {
    Result<bool> result;
    auto it = instance().validate_fn.find(name);
    if(it == instance().validate_fn.end()) {
        result.success() = false;
        result.error() = fmt::format("Unknown transfer manager type \"{}\"", name);
        return result;
    }
    auto& f = it->second;
    return f(config);
}

}
