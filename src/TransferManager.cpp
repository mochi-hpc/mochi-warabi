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
    return instance().create_fn[name](engine, config);
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
