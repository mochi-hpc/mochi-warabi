/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/Backend.hpp"

namespace tl = thallium;

namespace warabi {

using json = nlohmann::json;

std::unordered_map<std::string,
                std::function<std::unique_ptr<Backend>(const tl::engine&, const json&)>> TargetFactory::create_fn;

std::unordered_map<std::string,
                   std::function<Result<bool>(const json&)>> TargetFactory::validate_fn;

std::unique_ptr<Backend> TargetFactory::createTarget(const std::string& backend_name,
                                                     const tl::engine& engine,
                                                     const json& config) {
    auto it = create_fn.find(backend_name);
    if(it == create_fn.end()) return nullptr;
    auto& f = it->second;
    return f(engine, config);
}

Result<bool> TargetFactory::validateConfig(const std::string& backend_name,
                                           const json& config) {
    Result<bool> result;
    auto it = validate_fn.find(backend_name);
    if(it == validate_fn.end()) {
        result.success() = false;
        result.error() = "Could not find backend \"" + backend_name + "\"";
        return result;
    }
    auto& f = it->second;
    return f(config);
}

}
