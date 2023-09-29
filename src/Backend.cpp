/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/Backend.hpp"

namespace tl = thallium;

namespace warabi {

using json = nlohmann::json;

std::unique_ptr<Backend> TargetFactory::createTarget(const std::string& backend_name,
                                                     const tl::engine& engine,
                                                     const json& config) {
    auto it = instance().create_fn.find(backend_name);
    if(it == instance().create_fn.end()) return nullptr;
    auto& f = it->second;
    return f(engine, config);
}

Result<bool> TargetFactory::validateConfig(const std::string& backend_name,
                                           const json& config) {
    Result<bool> result;
    auto it = instance().validate_fn.find(backend_name);
    if(it == instance().validate_fn.end()) {
        result.success() = false;
        result.error() = "Could not find backend \"" + backend_name + "\"";
        return result;
    }
    auto& f = it->second;
    return f(config);
}

}
