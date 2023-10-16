/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/Backend.hpp"
#include <fmt/format.h>

namespace tl = thallium;

namespace warabi {

using json = nlohmann::json;

Result<std::unique_ptr<Backend>> TargetFactory::createTarget(
        const std::string& backend_name,
        const tl::engine& engine,
        const json& config) {
    return instance().create_fn[backend_name](engine, config);
}

Result<std::unique_ptr<Backend>> TargetFactory::recoverTarget(
        const std::string& backend_name,
        const thallium::engine& engine,
        const json& config,
        const std::vector<std::string>& filenames) {
    return instance().recover_fn[backend_name](engine, config, filenames);
}

Result<bool> TargetFactory::validateConfig(const std::string& backend_name,
                                           const json& config) {
    Result<bool> result;
    auto it = instance().validate_fn.find(backend_name);
    if(it == instance().validate_fn.end()) {
        result.success() = false;
        result.error() = fmt::format("Unknown target type \"{}\"", backend_name);
        return result;
    }
    auto& f = it->second;
    return f(config);
}

}
