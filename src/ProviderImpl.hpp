/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_PROVIDER_IMPL_H
#define __WARABI_PROVIDER_IMPL_H

#include "warabi/Backend.hpp"
#include "warabi/UUID.hpp"

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <tuple>

#define FIND_TARGET(__var__) \
        std::shared_ptr<Backend> __var__;\
        do {\
            std::lock_guard<tl::mutex> lock(m_backends_mtx);\
            auto it = m_backends.find(target_id);\
            if(it == m_backends.end()) {\
                result.success() = false;\
                result.error() = "Target with UUID "s + target_id.to_string() + " not found";\
                error(result.error());\
                return;\
            }\
            __var__ = it->second;\
        } while(0)

namespace warabi {

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
        spdlog::__name__("[warabi:{}] {}", get_provider_id(), msg); \
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
    AutoDeregistering m_create_target;
    AutoDeregistering m_open_target;
    AutoDeregistering m_close_target;
    AutoDeregistering m_destroy_target;
    // Client RPC
    AutoDeregistering m_check_target;
    AutoDeregistering m_compute_sum;
    // Backends
    std::unordered_map<UUID, std::shared_ptr<Backend>> m_backends;
    tl::mutex m_backends_mtx;

    ProviderImpl(const tl::engine& engine, uint16_t provider_id, const std::string& config, const tl::pool& pool)
    : tl::provider<ProviderImpl>(engine, provider_id)
    , m_engine(engine)
    , m_pool(pool)
    , m_create_target(define("warabi_create_target", &ProviderImpl::createTargetRPC, pool))
    , m_open_target(define("warabi_open_target", &ProviderImpl::openTargetRPC, pool))
    , m_close_target(define("warabi_close_target", &ProviderImpl::closeTargetRPC, pool))
    , m_destroy_target(define("warabi_destroy_target", &ProviderImpl::destroyTargetRPC, pool))
    , m_check_target(define("warabi_check_target", &ProviderImpl::checkTargetRPC, pool))
    , m_compute_sum(define("warabi_compute_sum",  &ProviderImpl::computeSumRPC, pool))
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
        if(!json_config.contains("targets")) return;
        auto& targets = json_config["targets"];
        if(!targets.is_array()) return;
        for(auto& target : targets) {
            if(!(target.contains("type") && target["type"].is_string()))
                continue;
            const std::string& target_type = target["type"].get_ref<const std::string&>();
            auto target_config = target.contains("config") ? target["config"] : json::object();
            createTarget(target_type, target_config.dump());
        }
    }

    ~ProviderImpl() {
        trace("Deregistering provider");
    }

    std::string getConfig() const {
        auto config = json::object();
        config["targets"] = json::array();
        for(auto& pair : m_backends) {
            auto target_config = json::object();
            target_config["__id__"] = pair.first.to_string();
            target_config["type"] = pair.second->name();
            target_config["config"] = json::parse(pair.second->getConfig());
            config["targets"].push_back(target_config);
        }
        return config.dump();
    }

    Result<UUID> createTarget(const std::string& target_type,
                                       const std::string& target_config) {

        auto target_id = UUID::generate();
        Result<UUID> result;

        json json_config;
        try {
            json_config = json::parse(target_config);
        } catch(json::parse_error& e) {
            result.error() = e.what();
            result.success() = false;
            error("Could not parse target configuration for target {}",
                  target_id.to_string());
            return result;
        }

        std::unique_ptr<Backend> backend;
        try {
            backend = TargetFactory::createTarget(target_type, get_engine(), json_config);
        } catch(const std::exception& ex) {
            result.success() = false;
            result.error() = ex.what();
            error("Error when creating target {} of type {}: {}",
                   target_id.to_string(), target_type, result.error());
            return result;
        }

        if(not backend) {
            result.success() = false;
            result.error() = "Unknown target type "s + target_type;
            error("Unknown target type {} for target {}",
                  target_type, target_id.to_string());
            return result;
        } else {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);
            m_backends[target_id] = std::move(backend);
            result.value() = target_id;
        }

        trace("Successfully created target {} of type {}",
              target_id.to_string(), target_type);
        return result;
    }

    void createTargetRPC(const tl::request& req,
                           const std::string& token,
                           const std::string& target_type,
                           const std::string& target_config) {

        trace("Received createTarget request", id());
        trace(" => type = {}", target_type);
        trace(" => config = {}", target_config);

        Result<UUID> result;
        AutoResponse<decltype(result)> response{req, result};

        if(m_token.size() > 0 && m_token != token) {
            result.success() = false;
            result.error() = "Invalid security token";
            error("Invalid security token {}", token);
            return;
        }

        result = createTarget(target_type, target_config);
    }

    void openTargetRPC(const tl::request& req,
                         const std::string& token,
                         const std::string& target_type,
                         const std::string& target_config) {

        trace("Received openTarget request");
        trace(" => type = {}", target_type);
        trace(" => config = {}", target_config);

        auto target_id = UUID::generate();
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
            json_config = json::parse(target_config);
        } catch(json::parse_error& e) {
            result.error() = e.what();
            result.success() = false;
            error("Could not parse target configuration for target {}",
                  target_id.to_string());
            return;
        }

        std::unique_ptr<Backend> backend;
        try {
            backend = TargetFactory::openTarget(target_type, get_engine(), json_config);
        } catch(const std::exception& ex) {
            result.success() = false;
            result.error() = ex.what();
            error("Error when opening target {} of type {}: {}",
                  target_id.to_string(), target_type, result.error());
            return;
        }

        if(not backend) {
            result.success() = false;
            result.error() = "Unknown target type "s + target_type;
            error("Unknown target type {} for target {}",
                  target_type, target_id.to_string());
            return;
        } else {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);
            m_backends[target_id] = std::move(backend);
            result.value() = target_id;
        }

        trace("Successfully created target {} of type {}",
              target_id.to_string(), target_type);
    }

    void closeTargetRPC(const tl::request& req,
                          const std::string& token,
                          const UUID& target_id) {
        trace("Received closeTarget request for target {}",
              target_id.to_string());

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

            if(m_backends.count(target_id) == 0) {
                result.success() = false;
                result.error() = "Target "s + target_id.to_string() + " not found";
                error(result.error());
                return;
            }

            m_backends.erase(target_id);
        }

        trace("Target {} successfully closed", target_id.to_string());
    }

    void destroyTargetRPC(const tl::request& req,
                            const std::string& token,
                            const UUID& target_id) {
        Result<bool> result;
        AutoResponse<decltype(result)> response{req, result};
        trace("Received destroyTarget request for target {}", target_id.to_string());

        if(m_token.size() > 0 && m_token != token) {
            result.success() = false;
            result.error() = "Invalid security token";
            error("Invalid security token {}", token);
            return;
        }

        {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);

            if(m_backends.count(target_id) == 0) {
                result.success() = false;
                result.error() = "Target "s + target_id.to_string() + " not found";
                error(result.error());
                return;
            }

            result = m_backends[target_id]->destroy();
            m_backends.erase(target_id);
        }

        trace("Target {} successfully destroyed", target_id.to_string());
    }

    void checkTargetRPC(const tl::request& req,
                          const UUID& target_id) {
        trace("Received checkTarget request for target {}", target_id.to_string());
        Result<bool> result;
        AutoResponse<decltype(result)> response{req, result};
        FIND_TARGET(target);
        trace("Successfully check for presence of target {}", target_id.to_string());
    }

    void computeSumRPC(const tl::request& req,
                       const UUID& target_id,
                       int32_t x, int32_t y) {
        trace("Received computeSum request for target {}", target_id.to_string());
        Result<int32_t> result;
        AutoResponse<decltype(result)> response{req, result};
        FIND_TARGET(target);
        result = target->computeSum(x, y);
        trace("Successfully executed computeSum on target {}", target_id.to_string());
    }

};

}

#endif
