/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __ALPHA_PROVIDER_IMPL_H
#define __ALPHA_PROVIDER_IMPL_H

#include "alpha/Backend.hpp"
#include "alpha/UUID.hpp"

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <tuple>

#define FIND_RESOURCE(__var__) \
        std::shared_ptr<Backend> __var__;\
        do {\
            std::lock_guard<tl::mutex> lock(m_backends_mtx);\
            auto it = m_backends.find(resource_id);\
            if(it == m_backends.end()) {\
                result.success() = false;\
                result.error() = "Resource with UUID "s + resource_id.to_string() + " not found";\
                error(result.error());\
                return;\
            }\
            __var__ = it->second;\
        } while(0)

namespace alpha {

using namespace std::string_literals;
namespace tl = thallium;

/**
 * @brief This class automatically deregisters a tl::remote_procedure
 * when the ProviderImpl's destructor is called.
 */
struct AutoDeregistering : public tl::remote_procedure {

    AutoDeregistering(tl::remote_procedure rpc)
    : tl::remote_procedure(std::move(rpc)) {}

    ~AutoDeregistering() {
        deregister();
    }
};

/**
 * @brief This class automatically calls req.respond(resp)
 * when its constructor is called, helping developers not
 * forget to respond in all code branches.
 */
template<typename ResponseType>
struct AutoResponse {

    AutoResponse(const tl::request& req, ResponseType& resp)
    : m_req(req)
    , m_resp(resp) {}

    AutoResponse(const AutoResponse&) = delete;
    AutoResponse(AutoResponse&&) = delete;

    ~AutoResponse() {
        m_req.respond(m_resp);
    }

    const tl::request& m_req;
    ResponseType&      m_resp;
};

class ProviderImpl : public tl::provider<ProviderImpl> {

    auto id() const { return get_provider_id(); } // for convenience

    using json = nlohmann::json;

    #define DEF_LOGGING_FUNCTION(__name__)                         \
    template<typename ... Args>                                    \
    void __name__(Args&&... args) {                                \
        auto msg = fmt::format(std::forward<Args>(args)...);       \
        spdlog::__name__("[alpha:{}] {}", get_provider_id(), msg); \
    }

    DEF_LOGGING_FUNCTION(trace)
    DEF_LOGGING_FUNCTION(debug)
    DEF_LOGGING_FUNCTION(info)
    DEF_LOGGING_FUNCTION(warn)
    DEF_LOGGING_FUNCTION(error)
    DEF_LOGGING_FUNCTION(critical)

    #undef DEF_LOGGING_FUNCTION

    public:

    tl::engine           m_engine;
    std::string          m_token;
    tl::pool             m_pool;
    // Admin RPC
    AutoDeregistering m_create_resource;
    AutoDeregistering m_open_resource;
    AutoDeregistering m_close_resource;
    AutoDeregistering m_destroy_resource;
    // Client RPC
    AutoDeregistering m_check_resource;
    AutoDeregistering m_compute_sum;
    // Backends
    std::unordered_map<UUID, std::shared_ptr<Backend>> m_backends;
    tl::mutex m_backends_mtx;

    ProviderImpl(const tl::engine& engine, uint16_t provider_id, const std::string& config, const tl::pool& pool)
    : tl::provider<ProviderImpl>(engine, provider_id)
    , m_engine(engine)
    , m_pool(pool)
    , m_create_resource(define("alpha_create_resource", &ProviderImpl::createResourceRPC, pool))
    , m_open_resource(define("alpha_open_resource", &ProviderImpl::openResourceRPC, pool))
    , m_close_resource(define("alpha_close_resource", &ProviderImpl::closeResourceRPC, pool))
    , m_destroy_resource(define("alpha_destroy_resource", &ProviderImpl::destroyResourceRPC, pool))
    , m_check_resource(define("alpha_check_resource", &ProviderImpl::checkResourceRPC, pool))
    , m_compute_sum(define("alpha_compute_sum",  &ProviderImpl::computeSumRPC, pool))
    {
        trace("Registered provider with id {}", get_provider_id());
        json json_config;
        try {
            json_config = json::parse(config);
        } catch(json::parse_error& e) {
            error("Could not parse provider configuration: {}", e.what());
            return;
        }
        if(!json_config.is_object()) return;
        if(!json_config.contains("resources")) return;
        auto& resources = json_config["resources"];
        if(!resources.is_array()) return;
        for(auto& resource : resources) {
            if(!(resource.contains("type") && resource["type"].is_string()))
                continue;
            const std::string& resource_type = resource["type"].get_ref<const std::string&>();
            auto resource_config = resource.contains("config") ? resource["config"] : json::object();
            createResource(resource_type, resource_config.dump());
        }
    }

    ~ProviderImpl() {
        trace("Deregistering provider");
    }

    std::string getConfig() const {
        auto config = json::object();
        config["resources"] = json::array();
        for(auto& pair : m_backends) {
            auto resource_config = json::object();
            resource_config["__id__"] = pair.first.to_string();
            resource_config["type"] = pair.second->name();
            resource_config["config"] = json::parse(pair.second->getConfig());
            config["resources"].push_back(resource_config);
        }
        return config.dump();
    }

    Result<UUID> createResource(const std::string& resource_type,
                                       const std::string& resource_config) {

        auto resource_id = UUID::generate();
        Result<UUID> result;

        json json_config;
        try {
            json_config = json::parse(resource_config);
        } catch(json::parse_error& e) {
            result.error() = e.what();
            result.success() = false;
            error("Could not parse resource configuration for resource {}",
                  resource_id.to_string());
            return result;
        }

        std::unique_ptr<Backend> backend;
        try {
            backend = ResourceFactory::createResource(resource_type, get_engine(), json_config);
        } catch(const std::exception& ex) {
            result.success() = false;
            result.error() = ex.what();
            error("Error when creating resource {} of type {}: {}",
                   resource_id.to_string(), resource_type, result.error());
            return result;
        }

        if(not backend) {
            result.success() = false;
            result.error() = "Unknown resource type "s + resource_type;
            error("Unknown resource type {} for resource {}",
                  resource_type, resource_id.to_string());
            return result;
        } else {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);
            m_backends[resource_id] = std::move(backend);
            result.value() = resource_id;
        }

        trace("Successfully created resource {} of type {}",
              resource_id.to_string(), resource_type);
        return result;
    }

    void createResourceRPC(const tl::request& req,
                           const std::string& token,
                           const std::string& resource_type,
                           const std::string& resource_config) {

        trace("Received createResource request", id());
        trace(" => type = {}", resource_type);
        trace(" => config = {}", resource_config);

        Result<UUID> result;
        AutoResponse<decltype(result)> response{req, result};

        if(m_token.size() > 0 && m_token != token) {
            result.success() = false;
            result.error() = "Invalid security token";
            error("Invalid security token {}", token);
            return;
        }

        result = createResource(resource_type, resource_config);
    }

    void openResourceRPC(const tl::request& req,
                         const std::string& token,
                         const std::string& resource_type,
                         const std::string& resource_config) {

        trace("Received openResource request");
        trace(" => type = {}", resource_type);
        trace(" => config = {}", resource_config);

        auto resource_id = UUID::generate();
        Result<UUID> result;
        AutoResponse<decltype(result)> response{req, result};

        if(m_token.size() > 0 && m_token != token) {
            result.success() = false;
            result.error() = "Invalid security token";
            error("Invalid security token {}", token);
            return;
        }

        json json_config;
        try {
            json_config = json::parse(resource_config);
        } catch(json::parse_error& e) {
            result.error() = e.what();
            result.success() = false;
            error("Could not parse resource configuration for resource {}",
                  resource_id.to_string());
            return;
        }

        std::unique_ptr<Backend> backend;
        try {
            backend = ResourceFactory::openResource(resource_type, get_engine(), json_config);
        } catch(const std::exception& ex) {
            result.success() = false;
            result.error() = ex.what();
            error("Error when opening resource {} of type {}: {}",
                  resource_id.to_string(), resource_type, result.error());
            return;
        }

        if(not backend) {
            result.success() = false;
            result.error() = "Unknown resource type "s + resource_type;
            error("Unknown resource type {} for resource {}",
                  resource_type, resource_id.to_string());
            return;
        } else {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);
            m_backends[resource_id] = std::move(backend);
            result.value() = resource_id;
        }

        trace("Successfully created resource {} of type {}",
              resource_id.to_string(), resource_type);
    }

    void closeResourceRPC(const tl::request& req,
                          const std::string& token,
                          const UUID& resource_id) {
        trace("Received closeResource request for resource {}",
              resource_id.to_string());

        Result<bool> result;
        AutoResponse<decltype(result)> response{req, result};

        if(m_token.size() > 0 && m_token != token) {
            result.success() = false;
            result.error() = "Invalid security token";
            error("Invalid security token {}", token);
            return;
        }

        {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);

            if(m_backends.count(resource_id) == 0) {
                result.success() = false;
                result.error() = "Resource "s + resource_id.to_string() + " not found";
                error(result.error());
                return;
            }

            m_backends.erase(resource_id);
        }

        trace("Resource {} successfully closed", resource_id.to_string());
    }

    void destroyResourceRPC(const tl::request& req,
                            const std::string& token,
                            const UUID& resource_id) {
        Result<bool> result;
        AutoResponse<decltype(result)> response{req, result};
        trace("Received destroyResource request for resource {}", resource_id.to_string());

        if(m_token.size() > 0 && m_token != token) {
            result.success() = false;
            result.error() = "Invalid security token";
            error("Invalid security token {}", token);
            return;
        }

        {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);

            if(m_backends.count(resource_id) == 0) {
                result.success() = false;
                result.error() = "Resource "s + resource_id.to_string() + " not found";
                error(result.error());
                return;
            }

            result = m_backends[resource_id]->destroy();
            m_backends.erase(resource_id);
        }

        trace("Resource {} successfully destroyed", resource_id.to_string());
    }

    void checkResourceRPC(const tl::request& req,
                          const UUID& resource_id) {
        trace("Received checkResource request for resource {}", resource_id.to_string());
        Result<bool> result;
        AutoResponse<decltype(result)> response{req, result};
        FIND_RESOURCE(resource);
        trace("Successfully check for presence of resource {}", resource_id.to_string());
    }

    void computeSumRPC(const tl::request& req,
                       const UUID& resource_id,
                       int32_t x, int32_t y) {
        trace("Received computeSum request for resource {}", resource_id.to_string());
        Result<int32_t> result;
        AutoResponse<decltype(result)> response{req, result};
        FIND_RESOURCE(resource);
        result = resource->computeSum(x, y);
        trace("Successfully executed computeSum on resource {}", resource_id.to_string());
    }

};

}

#endif
