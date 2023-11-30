/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_PROVIDER_IMPL_H
#define __WARABI_PROVIDER_IMPL_H

#include "config.h"
#include <valijson/adapters/nlohmann_json_adapter.hpp>
#include <valijson/schema.hpp>
#include <valijson/schema_parser.hpp>
#include <valijson/validator.hpp>

#include "warabi/Provider.hpp"
#include "warabi/Backend.hpp"
#include "warabi/TransferManager.hpp"
#include "warabi/MigrationOptions.hpp"
#include "BufferWrapper.hpp"
#include "Defer.hpp"

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/array.hpp>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <tuple>

#ifdef WARABI_HAS_REMI
#include <remi/remi-client.h>
#include <remi/remi-server.h>
#endif

namespace warabi {

using namespace std::string_literals;
namespace tl = thallium;

static constexpr const char* configSchema = R"(
{
  "type": "object",
  "properties": {
    "target": {
      "type": "object",
      "properties": {
        "type": {"type": "string"},
        "config": {"type": "object"}
      },
      "required": ["type"]
    },
    "transfer_manager": {
      "properties": {
        "type": {"type": "string"},
        "config": {"type": "object"}
      }
    }
  }
}
)";

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

    tl::engine      m_engine;
    tl::pool        m_pool;
    remi_client_t   m_remi_client;
    remi_provider_t m_remi_provider;

    tl::auto_remote_procedure m_create;
    tl::auto_remote_procedure m_write;
    tl::auto_remote_procedure m_write_eager;
    tl::auto_remote_procedure m_persist;
    tl::auto_remote_procedure m_create_write;
    tl::auto_remote_procedure m_create_write_eager;
    tl::auto_remote_procedure m_read;
    tl::auto_remote_procedure m_read_eager;
    tl::auto_remote_procedure m_erase;

    // Backend
    std::shared_ptr<Backend>         m_target;
    std::shared_ptr<TransferManager> m_transfer_manager;

    ProviderImpl(
            const tl::engine& engine,
            uint16_t provider_id,
            const std::string& config,
            const tl::pool& pool,
            remi_client_t remi_cl,
            remi_provider_t remi_pr)
    : tl::provider<ProviderImpl>(engine, provider_id)
    , m_engine(engine)
    , m_pool(pool)
    , m_remi_client(remi_cl)
    , m_remi_provider(remi_pr)
    , m_create(define("warabi_create",  &ProviderImpl::createRPC, pool))
    , m_write(define("warabi_write",  &ProviderImpl::writeRPC, pool))
    , m_write_eager(define("warabi_write_eager",  &ProviderImpl::writeEagerRPC, pool))
    , m_persist(define("warabi_persist",  &ProviderImpl::persistRPC, pool))
    , m_create_write(define("warabi_create_write",  &ProviderImpl::createWriteRPC, pool))
    , m_create_write_eager(define("warabi_create_write_eager",  &ProviderImpl::createWriteEagerRPC, pool))
    , m_read(define("warabi_read",  &ProviderImpl::readRPC, pool))
    , m_read_eager(define("warabi_read_eager",  &ProviderImpl::readEagerRPC, pool))
    , m_erase(define("warabi_erase",  &ProviderImpl::eraseRPC, pool))
    {
        trace("Registered provider with id {}", get_provider_id());
        json json_config;
        try {
            if(!config.empty())
                json_config = json::parse(config);
            else
                json_config = json::object();
        } catch(json::parse_error& e) {
            auto err = fmt::format("Could not parse warabi provider configuration: {}", e.what());
            error("{}", err);
            throw Exception(err);
        }

#if 0
#ifdef WARABI_HAS_REMI
        if(m_remi_client && !m_remi_provider) {
            warn("Warabi provider initialized with only a REMI client"
                 " will only be able to *send* targets to other providers");
        } else if(!m_remi_client && m_remi_provider) {
            warn("Warabi provider initialized with only a REMI provider"
                 " will only be able to *receive* targets from other providers");
        }
        if(m_remi_provider) {
            std::string remi_class = fmt::format("warabi/{}", provider_id);
            int rret = remi_provider_register_migration_class(
                    m_remi_provider, remi_class.c_str(),
                    beforeMigrationCallback,
                    afterMigrationCallback, nullptr, this);
            if(rret != REMI_SUCCESS) {
                throw Exception(fmt::format(
                    "Failed to register migration class in REMI:"
                    " remi_provider_register_migration_class returned {}",
                    rret));
            }
        }
#else
        if(m_remi_client || m_remi_provider) {
            error("Provided REMI client or provider will be ignored because"
                  " Warabi wasn't built with REMI support");
        }
#endif
#endif
        static json schemaDocument = json::parse(configSchema);

        valijson::Schema schemaValidator;
        valijson::SchemaParser schemaParser;
        valijson::adapters::NlohmannJsonAdapter schemaAdapter(schemaDocument);
        schemaParser.populateSchema(schemaAdapter, schemaValidator);

        valijson::Validator validator;
        valijson::adapters::NlohmannJsonAdapter jsonAdapter(json_config);

        valijson::ValidationResults validationResults;
        validator.validate(schemaValidator, jsonAdapter, &validationResults);

        if(validationResults.numErrors() != 0) {
            error("Error(s) while validating JSON config for warabi provider:");
            for(auto& err : validationResults) {
                error("\t{}", err.description);
            }
            throw Exception("Invalid JSON configuration (see error logs for information)");
        }

        {
            auto transfer_manager = json_config.value("transfer_manager", json::object());
            auto transfer_manager_type = transfer_manager.value("type", "__default__");
            auto transfer_manager_config = transfer_manager.value("config", json::object());
            auto transfer_manager_config_is_valid = validateTransferManagerConfig(transfer_manager_type, transfer_manager_config);
            if(!transfer_manager_config_is_valid.success())
                throw Exception(transfer_manager_config_is_valid.error());
            setTransferManager(transfer_manager_type, transfer_manager_config);
        }

        if(json_config.contains("target")) {
            auto& target = json_config["target"];
            auto& target_type = target["type"].get_ref<const std::string&>();
            auto target_config = target.value("config", json::object());
            auto target_config_is_valid = validateTargetConfig(target_type, target_config);
            if(!target_config_is_valid.success())
                throw Exception(target_config_is_valid.error());
            setTarget(target_type, target_config);
        }
    }

    ~ProviderImpl() {
        trace("Deregistering provider");
        m_target->destroy();
    }

    std::string getConfig() const {
        auto config = json::object();
        if(m_target) {
            config["target"] = json::object();
            auto& target = config["target"];
            target["type"] = m_target->name();
            target["config"] = json::parse(m_target->getConfig());
        }
        config["transfer_manager"] = json::object();
        auto& tm = config["transfer_manager"];
        tm["type"] = m_transfer_manager->name();
        tm["config"] = json::parse(m_transfer_manager->getConfig());
        return config.dump();
    }

    Result<bool> validateTargetConfig(
            const std::string& target_type,
            const json& target_config) {
        return TargetFactory::validateConfig(target_type, target_config);
    }

    Result<bool> setTarget(const std::string& type,
                           const json& config) {
        Result<bool> result;
        auto target = TargetFactory::createTarget(type, m_engine, config);
        if(not target.success()) {
            result.success() = false;
            result.error() = target.error();
            return result;
        } else {
            m_target = std::move(target.value());
        }
        return result;
    }

    Result<bool> validateTransferManagerConfig(
            const std::string& type,
            const json& config) {
        return TransferManagerFactory::validateConfig(type, config);
    }

    Result<bool> setTransferManager(const std::string& type,
                                    const json& config) {

        Result<bool> result;
        auto tm = TransferManagerFactory::createTransferManager(type, m_engine, config);
        if(not tm.success()) {
            result.success() = false;
            result.error() = tm.error();
            return result;
        } else {
            m_transfer_manager = std::move(tm.value());
        }
        return result;
    }

    void migrateTargetRPC(const tl::request& req,
                          const std::string& dest_address,
                          uint16_t dest_provider_id,
                          const MigrationOptions& options) {
#if 0
        trace("Received migrateTarget request for target {}", target_id.to_string());
        Result<bool> result;
        tl::auto_respond<decltype(result)> response{req, result};
#ifndef WARABI_HAS_REMI
        result.error() = "Warabi was not compiled with REMI support";
        result.success() = false;
#else

        #define HANDLE_REMI_ERROR(func, rret, ...) do {                          \
            if(rret != REMI_SUCCESS) {                                           \
                result.success() = false;                                        \
                result.error() = fmt::format(__VA_ARGS__);                       \
                result.error() += fmt::format(" ({} returned {})", #func, rret); \
                return;                                                          \
            }                                                                    \
        } while(0)

        // lookup destination address
        tl::endpoint dest_endpoint;
        try {
            dest_endpoint = m_engine.lookup(dest_address);
        } catch(const std::exception& ex) {
            result.success() = false;
            result.error() = fmt::format("Failed to lookup destination address: {}", ex.what());
            return;
        }
        // create remi provider handle
        remi_provider_handle_t remi_ph = NULL;
        int rret = remi_provider_handle_create(
                m_remi_client, dest_endpoint.get_addr(), dest_provider_id, &remi_ph);
        HANDLE_REMI_ERROR(remi_provider_handle_create, rret, "Failed to create REMI provider handle");
        DEFER(remi_provider_handle_release(remi_ph));
        // find target
        FIND_TARGET(target);
        // get a MigrationHandle
        auto& tg = target->m_target;
        auto startMigration = tg->startMigration(options.removeSource);
        if(!startMigration.success()) {
            result.error() = startMigration.error();
            result.success() = false;
            return;
        }
        auto& mh = startMigration.value();
        // create REMI fileset
        remi_fileset_t fileset = REMI_FILESET_NULL;
        std::string remi_class = fmt::format("warabi/{}", dest_provider_id);
        rret = remi_fileset_create(remi_class.c_str(), mh->getRoot().c_str(), &fileset);
        HANDLE_REMI_ERROR(remi_fileset_create, rret, "Failed to create REMI fileset");
        DEFER(remi_fileset_free(fileset));
        // fill REMI fileset
        for(const auto& file : mh->getFiles()) {
            if(!file.empty() && file.back() == '/') {
                rret = remi_fileset_register_directory(fileset, file.c_str());
                HANDLE_REMI_ERROR(remi_fileset_register_directory, rret,
                        "Failed to register directory {} in REMI fileset", file);
            } else {
                rret = remi_fileset_register_file(fileset, file.c_str());
                HANDLE_REMI_ERROR(remi_fileset_register_file, rret,
                        "Failed to register file {} in REMI fileset", file);
            }
        }
        // register REMI metadata
        rret = remi_fileset_register_metadata(fileset, "uuid", target_id.to_string().c_str());
        HANDLE_REMI_ERROR(remi_fileset_register_metadata, rret, "Failed to register metadata in REMI fileset");
        rret = remi_fileset_register_metadata(fileset, "config", tg->getConfig().c_str());
        HANDLE_REMI_ERROR(remi_fileset_register_metadata, rret, "Failed to register metadata in REMI fileset");
        rret = remi_fileset_register_metadata(fileset, "type", tg->name().c_str());
        HANDLE_REMI_ERROR(remi_fileset_register_metadata, rret, "Failed to register metadata in REMI fileset");
        rret = remi_fileset_register_metadata(fileset, "migration_config", options.extraConfig.c_str());
        HANDLE_REMI_ERROR(remi_fileset_register_metadata, rret, "Failed to register metadata in REMI fileset");
        // set block transfer size
        if(options.transferSize) {
            rret = remi_fileset_set_xfer_size(fileset, options.transferSize);
            HANDLE_REMI_ERROR(remi_fileset_set_xfer_size, rret, "Failed to set transfer size for REMI fileset");
        }
        // issue migration
        int remi_status = 0;
        rret = remi_fileset_migrate(remi_ph, fileset, options.newRoot.c_str(),
                REMI_KEEP_SOURCE, REMI_USE_MMAP, &remi_status);
        HANDLE_REMI_ERROR(remi_fileset_migrate, rret, "REMI failed to migrate fileset");
        if(remi_status) {
            result.success() = false;
            result.error() = fmt::format("Migration failed with status {}", remi_status);
            mh->cancel();
            return;
        }
#endif
        trace("Sucessfully executed migrateTarget for target {}", target_id.to_string());
#endif
    }

    void createRPC(const tl::request& req,
                   size_t size) {
        trace("Received create request with size {}", size);
        Result<RegionID> result;
        tl::auto_respond<decltype(result)> response{req, result};
        if(!m_target) {
            result.success() = false;
            result.error() = "No target found in the provider";
            return;
        }
        auto region = m_target->create(size);
        if(!region.success()) {
            result.success() = false;
            result.error() = region.error();
            return;
        }
        result = region.value()->getRegionID();
        trace("Successfully executed create request");
    }

    void writeRPC(const tl::request& req,
                  const RegionID& region_id,
                  const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
                  thallium::bulk data,
                  const std::string& address,
                  size_t bulkOffset,
                  bool persist) {
        trace("Received write request");
        Result<bool> result;
        tl::auto_respond<decltype(result)> response{req, result};
        if(!m_target) {
            result.success() = false;
            result.error() = "No target found in the provider";
            return;
        }
        auto region = m_target->write(region_id, persist);
        if(!region.success()) {
            result.success() = false;
            result.error() = region.error();
            return;
        }
        auto source = address.empty() ? req.get_endpoint() : m_engine.lookup(address);
        result = m_transfer_manager->pull(
                *region.value(), regionOffsetSizes, data, source, bulkOffset, persist);
        trace("Successfully executed write request");
    }

    void writeEagerRPC(const tl::request& req,
                       const RegionID& region_id,
                       const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
                       const BufferWrapper& buffer,
                       bool persist) {
        trace("Received write_eager request");
        Result<bool> result;
        tl::auto_respond<decltype(result)> response{req, result};
        if(!m_target) {
            result.success() = false;
            result.error() = "No target found in the provider";
            return;
        }
        auto region = m_target->write(region_id, persist);
        if(!region.success()) {
            result.success() = false;
            result.error() = region.error();
            return;
        }
        result = region.value()->write(regionOffsetSizes, buffer.data(), persist);
        trace("Successfully executed write_eager request");
    }

    void persistRPC(const tl::request& req,
                    const RegionID& region_id,
                    const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes) {
        trace("Received persist request");
        Result<bool> result;
        tl::auto_respond<decltype(result)> response{req, result};
        if(!m_target) {
            result.success() = false;
            result.error() = "No target found in the provider";
            return;
        }
        auto region = m_target->write(region_id, true);
        if(!region.success()) {
            result.success() = false;
            result.error() = region.error();
            return;
        }
        result = region.value()->persist(regionOffsetSizes);
        trace("Successfully executed persist request");
    }

    void createWriteRPC(const tl::request& req,
                        thallium::bulk data,
                        const std::string& address,
                        size_t bulkOffset, size_t size,
                        bool persist) {
        trace("Received create_write request");
        Result<RegionID> result;
        tl::auto_respond<decltype(result)> response{req, result};
        if(!m_target) {
            result.success() = false;
            result.error() = "No target found in the provider";
            return;
        }
        auto region = m_target->create(size);
        if(!region.success()) {
            result.success() = false;
            result.error() = region.error();
            return;
        }
        result = region.value()->getRegionID();
        auto source = address.empty() ? req.get_endpoint() : m_engine.lookup(address);
        Result<bool> writeResult;
        writeResult = m_transfer_manager->pull(
                *region.value(), {{0, size}}, data, source, bulkOffset, persist);
        if(!writeResult.success()) {
            result.success() = false;
            result.error() = writeResult.error();
        }
        trace("Successfully executed create_write request");
    }

    void createWriteEagerRPC(const tl::request& req,
                             const BufferWrapper& buffer,
                             bool persist) {
        trace("Received create_write_eager request");
        Result<RegionID> result;
        tl::auto_respond<decltype(result)> response{req, result};
        if(!m_target) {
            result.success() = false;
            result.error() = "No target found in the provider";
            return;
        }
        auto region = m_target->create(buffer.size());
        if(!region.success()) {
            result.success() = false;
            result.error() = region.error();
            return;
        }
        result = region.value()->getRegionID();
        auto writeResult = region.value()->write(
                {{0, buffer.size()}}, buffer.data(), persist);
        if(!writeResult.success()) {
            result.success() = false;
            result.error() = writeResult.error();
        }
        trace("Successfully executed create_write_eager request");
    }

    void readRPC(const tl::request& req,
                 const RegionID& region_id,
                 const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
                 thallium::bulk data,
                 const std::string& address,
                 size_t bulkOffset) {
        trace("Received read request");
        Result<bool> result;
        tl::auto_respond<decltype(result)> response{req, result};
        if(!m_target) {
            result.success() = false;
            result.error() = "No target found in the provider";
            return;
        }
        auto region = m_target->read(region_id);
        if(!region.value()) {
            result.success() = false;
            result.error() = region.error();
            return;
        }
        auto source = address.empty() ? req.get_endpoint() : m_engine.lookup(address);
        result = m_transfer_manager->push(
                *region.value(), regionOffsetSizes, data, source, bulkOffset);
        trace("Successfully executed read request");
    }

    void readEagerRPC(const tl::request& req,
                      const RegionID& region_id,
                      const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes) {
        trace("Received read_eager request");
        Result<BufferWrapper> result;
        tl::auto_respond<decltype(result)> response{req, result};
        if(!m_target) {
            result.success() = false;
            result.error() = "No target found in the provider";
            return;
        }
        auto region = m_target->read(region_id);
        if(!region.value()) {
            result.success() = false;
            result.error() = region.error();
            return;
        }
        size_t size = std::accumulate(regionOffsetSizes.begin(), regionOffsetSizes.end(), (size_t)0,
                [](size_t acc, const std::pair<size_t, size_t>& p) { return acc + p.second; });
        result.value().allocate(size);
        auto ret = region.value()->read(regionOffsetSizes, result.value().data());
        if(!ret.success()) {
            result.success() = false;
            result.error() = ret.error();
        }
        trace("Successfully executed read_eager request");
    }

    void eraseRPC(const tl::request& req,
                  const RegionID& region_id) {
        trace("Received erase request");
        Result<bool> result;
        tl::auto_respond<decltype(result)> response{req, result};
        if(!m_target) {
            result.success() = false;
            result.error() = "No target found in the provider";
            return;
        }
        result = m_target->erase(region_id);
        trace("Successfully executed erase request");
    }

#if 0
#ifdef WARABI_HAS_REMI
    static int32_t beforeMigrationCallback(remi_fileset_t fileset, void* uargs) {
        // the goal this callback is just to make sure the required metadata
        // is available and there  isn't any database with the same name yet,
        // so we can do the migration safely.
        ProviderImpl* provider = static_cast<ProviderImpl*>(uargs);

        const char* uuid = nullptr;
        const char* type = nullptr;
        const char* config = nullptr;
        const char* migration_config = nullptr;

        int rret;
        rret = remi_fileset_get_metadata(fileset, "uuid", &uuid);
        if(rret != REMI_SUCCESS) return rret;
        rret = remi_fileset_get_metadata(fileset, "type", &type);
        if(rret != REMI_SUCCESS) return rret;
        rret = remi_fileset_get_metadata(fileset, "config", &config);
        if(rret != REMI_SUCCESS) return rret;
        rret = remi_fileset_get_metadata(fileset, "migration_config", &migration_config);
        if(rret != REMI_SUCCESS) return rret;

        auto target_id = UUID::from_string(uuid);
        json config_json;
        json migration_config_json;
        try {
            config_json = json::parse(config);
            migration_config_json = json::parse(migration_config);
        } catch (...) {
            return 1;
        }
        config_json.update(migration_config_json, true);
        if(config_json.contains("transfer_manager")) {
            auto tm_name = config_json["transfer_manager"].get<std::string>();
            std::unique_lock<tl::mutex> lock{provider->m_transfer_managers_mtx};
            auto it = provider->m_transfer_managers.find(tm_name);
            if(it == provider->m_transfer_managers.end())
                return 2;
        }
        auto it = provider->m_targets.find(target_id);
        if(it != provider->m_targets.end())
            return 3;
        if(!TargetFactory::validateConfig(type, config_json).success())
            return 4;
        return 0;
    }

    static int32_t afterMigrationCallback(remi_fileset_t fileset, void* uargs) {
        ProviderImpl* provider = static_cast<ProviderImpl*>(uargs);

        const char* uuid = nullptr;
        const char* type = nullptr;
        const char* config = nullptr;
        const char* migration_config = nullptr;

        int rret;
        rret = remi_fileset_get_metadata(fileset, "uuid", &uuid);
        if(rret != REMI_SUCCESS) return rret;
        rret = remi_fileset_get_metadata(fileset, "type", &type);
        if(rret != REMI_SUCCESS) return rret;
        rret = remi_fileset_get_metadata(fileset, "config", &config);
        if(rret != REMI_SUCCESS) return rret;
        rret = remi_fileset_get_metadata(fileset, "migration_config", &migration_config);
        if(rret != REMI_SUCCESS) return rret;

        auto target_id = UUID::from_string(uuid);
        json config_json;
        json migration_config_json;
        try {
            config_json = json::parse(config);
            migration_config_json = json::parse(migration_config);
        } catch (...) {
            return 1;
        }
        config_json.update(migration_config_json, true);

        std::shared_ptr<TransferManager> tm;
        std::string tm_name;
        if(config_json.contains("transfer_manager")) {
            tm_name = config_json["transfer_manager"].get<std::string>();
            std::unique_lock<tl::mutex> lock{provider->m_transfer_managers_mtx};
            auto it = provider->m_transfer_managers.find(tm_name);
            tm = it->second;
        } else {
            tm_name = "__default__";
            tm = provider->m_transfer_managers["__default__"];
        }

        std::vector<std::string> files;
        rret = remi_fileset_walkthrough(fileset,
            [](const char* filename, void* uargs) {
                static_cast<std::vector<std::string>*>(uargs)->emplace_back(filename);
            }, &files);
        if(rret != REMI_SUCCESS) return 2;

        size_t root_size = 0;
        std::vector<char> root;
        rret = remi_fileset_get_root(fileset, nullptr, &root_size);
        if(rret != REMI_SUCCESS) return 3;
        root.resize(root_size);
        rret = remi_fileset_get_root(fileset, root.data(), &root_size);
        if(rret != REMI_SUCCESS) return 3;

        auto root_str = std::string{root.data()};
        if(root_str.empty() || root_str.back() != '/')
            root_str += "/";
        for(auto& filename : files) {
            filename = root_str + filename;
        }

        auto target = TargetFactory::recoverTarget(type, provider->m_engine, config_json, files);
        if(!target.success()) {
            provider->error("{}", target.error());
            return 4;
        }

        provider->m_targets[target_id] = std::make_shared<TargetEntry>(
                std::shared_ptr<Backend>(std::move(target.value())), tm, tm_name);

        return 0;
    }
#endif
#endif
};

}

#endif
