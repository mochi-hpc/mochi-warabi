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
    tl::auto_remote_procedure m_get_remi_provider_id;

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
    : tl::provider<ProviderImpl>(engine, provider_id, "warabi")
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
    , m_get_remi_provider_id(define("warabi_get_remi_provider_id",  &ProviderImpl::getREMIproviderIdRPC, pool))
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

#ifdef WARABI_HAS_REMI
        if(m_remi_client && !m_remi_provider) {
            warn("Warabi provider initialized with only a REMI client"
                 " will only be able to *send* targets to other providers");
        } else if(!m_remi_client && m_remi_provider) {
            warn("Warabi provider initialized with only a REMI provider"
                 " will only be able to *receive* targets from other providers");
        }
        if(m_remi_provider) {
            int rret = remi_provider_register_provider_migration_class(
                    m_remi_provider, "warabi", provider_id,
                    beforeMigrationCallback,
                    afterMigrationCallback,
                    [](void*){}, this);
            if(rret != REMI_SUCCESS) {
                throw Exception(fmt::format(
                    "Failed to register migration class in REMI:"
                    " remi_provider_register_propvider_migration_class returned {}",
                    rret));
            }
        }
#else
        if(m_remi_client || m_remi_provider) {
            error("Provided REMI client or provider will be ignored because"
                  " Warabi wasn't built with REMI support");
        }
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
#ifdef WARABI_HAS_REMI
        if(m_remi_provider) {
            remi_provider_deregister_provider_migration_class(
                m_remi_provider, "warabi", get_provider_id());
        }
#endif
        if(m_target) m_target->destroy();
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

    void getREMIproviderIdRPC(const tl::request& req) {
        trace("Received getREMIproviderId request");
        Result<uint16_t> result;
        tl::auto_respond<decltype(result)> response{req, result};
        if(!m_remi_provider) {
            result.success() = false;
            result.error() = "No target found in the provider";
            return;
        }
        uint16_t id;
        int rret = remi_provider_get_provider_id(m_remi_provider, &id);
        if(rret != REMI_SUCCESS) {
            result.success() = false;
            result.error() = "remi_provider_get_provider_id returned " + std::to_string(rret);
            return;
        }
        result.value() = id;
        trace("Successfully executed getREMIproviderId request");
    }

    void migrateTarget(const std::string& dest_address,
                       uint16_t dest_provider_id,
                       const std::string& options) {
#ifndef WARABI_HAS_REMI
        throw Exception{"Warabi was not compiled with REMI support"};
#else
        // check we have a REMI client we can use
        if(!m_remi_client) throw Exception{"No REMI client available to send target"};

        // check if there is a target to transfer
        if(!m_target) throw Exception{"No target to migration"};

        // check that the options is valid JSON
        json optionsJson;
        try {
            optionsJson = options.empty() ? json::object() : json::parse(options);
        } catch(const std::exception& ex) {
            throw Exception{ex.what()};
        }

        // check that the options comply with the schema
        static const auto migrationJsonSchemaStr = R"({
            "type": "object",
            "properties": {
                "new_root": {"type": "string"},
                "transfer_size": {"type": "integer", "minimum": 0},
                "merge_config": {"type": "object"},
                "remove_source": {"type": "boolean"}
            }
        })";
        static const json migrationJsonSchema = json::parse(migrationJsonSchemaStr);

        valijson::Schema schemaValidator;
        valijson::SchemaParser schemaParser;
        valijson::adapters::NlohmannJsonAdapter schemaAdapter(migrationJsonSchema);
        schemaParser.populateSchema(schemaAdapter, schemaValidator);

        valijson::Validator validator;
        valijson::adapters::NlohmannJsonAdapter jsonAdapter(optionsJson);

        valijson::ValidationResults validationResults;
        validator.validate(schemaValidator, jsonAdapter, &validationResults);

        if(validationResults.numErrors() != 0) {
            error("Error(s) while validating JSON config for warabi provider:");
            for(auto& err : validationResults) {
                error("\t{}", err.description);
            }
            throw Exception("Invalid JSON configuration (see error logs for information)");
        }

        // lookup destination address
        tl::provider_handle dest_provider;
        try {
            dest_provider = tl::provider_handle{
                m_engine.lookup(dest_address),
                dest_provider_id
            };
            if(dest_provider.get_identity() != "warabi")
                throw Exception{"Destination provider for migration is not a Warabi provider"};
        } catch(const std::exception& ex) {
            throw Exception{
                fmt::format("Failed to lookup destination address: {}", ex.what())};
        }

        // get the REMI provider ID used by the destination provider
        uint16_t dest_remi_provider_id;
        Result<uint16_t> res = m_get_remi_provider_id.on(dest_provider)();
        dest_remi_provider_id = res.valueOrThrow();

        std::unique_ptr<MigrationHandle> migrationHandle;

        #define HANDLE_REMI_ERROR(func, rret, ...) do {                         \
            if(rret != REMI_SUCCESS) {                                          \
                if(migrationHandle) migrationHandle->cancel();                  \
                throw Exception{fmt::format(__VA_ARGS__) +                      \
                                fmt::format(" ({} returned {})", #func, rret)}; \
            }                                                                   \
        } while(0)

        // create remi provider handle
        remi_provider_handle_t remi_ph = NULL;
        int rret = remi_provider_handle_create(
                m_remi_client, dest_provider.get_addr(),
                dest_remi_provider_id, &remi_ph);
        HANDLE_REMI_ERROR(remi_provider_handle_create, rret,
                          "Failed to create REMI provider handle");
        DEFER(remi_provider_handle_release(remi_ph));

        // get a MigrationHandle
        bool remove_source = optionsJson.value("remove_source", true);
        auto startMigration = m_target->startMigration(remove_source);
        migrationHandle = std::move(startMigration.valueOrThrow());

        // create REMI fileset
        remi_fileset_t fileset = REMI_FILESET_NULL;
        rret = remi_fileset_create("warabi", migrationHandle->getRoot().c_str(), &fileset);
        HANDLE_REMI_ERROR(remi_fileset_create, rret, "Failed to create REMI fileset");
        DEFER(remi_fileset_free(fileset));

        // set its destination provider
        remi_fileset_set_provider_id(fileset, dest_provider_id);

        // fill REMI fileset
        for(const auto& file : migrationHandle->getFiles()) {
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

        // get the config to send to by merging the target config with the merge config
        auto target_config = json::parse(m_target->getConfig());
        target_config.update(optionsJson.value("merge_config", json::object()), true);

        // register REMI metadata
        rret = remi_fileset_register_metadata(fileset, "config", target_config.dump().c_str());
        HANDLE_REMI_ERROR(remi_fileset_register_metadata, rret, "Failed to register metadata in REMI fileset");
        rret = remi_fileset_register_metadata(fileset, "type", m_target->name().c_str());
        HANDLE_REMI_ERROR(remi_fileset_register_metadata, rret, "Failed to register metadata in REMI fileset");

        // set block transfer size
        if(optionsJson.contains("transfer_size")) {
            rret = remi_fileset_set_xfer_size(fileset, optionsJson["transfer_size"].get<size_t>());
            HANDLE_REMI_ERROR(remi_fileset_set_xfer_size, rret, "Failed to set transfer size for REMI fileset");
        }

        // issue migration RPC
        auto newRoot     = optionsJson.value("new_root", migrationHandle->getRoot());
        auto keep_source = optionsJson.value("keep_source", false) ? REMI_REMOVE_SOURCE : REMI_KEEP_SOURCE;
        int remi_status = 0;
        rret = remi_fileset_migrate(remi_ph, fileset, newRoot.c_str(),
                                    keep_source, REMI_USE_MMAP, &remi_status);
        HANDLE_REMI_ERROR(remi_fileset_migrate, rret, "REMI failed to migrate fileset");

        migrationHandle.reset(); // this will cause the target to be destroyed
        m_target.reset();        // we still need to make it unavailable
#endif
    }

#ifdef WARABI_HAS_REMI
    static int32_t beforeMigrationCallback(remi_fileset_t fileset, void* uargs) {
        // the goal this callback is just to make sure the required metadata
        // is available and there  isn't any database with the same name yet,
        // so we can do the migration safely.
        ProviderImpl* provider = static_cast<ProviderImpl*>(uargs);
        if(!provider) return 1;
        return provider->beforeMigrationCallback(fileset);
    }

    int32_t beforeMigrationCallback(remi_fileset_t fileset) {

        const char* type = nullptr;
        const char* config = nullptr;

        int rret;
        rret = remi_fileset_get_metadata(fileset, "type", &type);
        if(rret != REMI_SUCCESS) return rret;
        rret = remi_fileset_get_metadata(fileset, "config", &config);
        if(rret != REMI_SUCCESS) return rret;

        // note: this JSON string has been validated on the sender
        // so we don't need to try/catch and validate again
        json config_json = json::parse(config);

        if(m_target) {
            error("Cannot accept migration: target already attached to provider");
            return 2;
        }
        auto validation = TargetFactory::validateConfig(type, config_json);
        if(!validation.success()) {
            error(validation.error());
            return 3;
        }
        return 0;
    }

    static int32_t afterMigrationCallback(remi_fileset_t fileset, void* uargs) {
        ProviderImpl* provider = static_cast<ProviderImpl*>(uargs);
        if(!provider) return 1;
        return provider->afterMigrationCallback(fileset);
    }

    int32_t afterMigrationCallback(remi_fileset_t fileset) {

        const char* type = nullptr;
        const char* config = nullptr;

        int rret;
        rret = remi_fileset_get_metadata(fileset, "type", &type);
        if(rret != REMI_SUCCESS) return rret;
        rret = remi_fileset_get_metadata(fileset, "config", &config);
        if(rret != REMI_SUCCESS) return rret;

        json config_json = json::parse(config);

        std::vector<std::string> files;
        rret = remi_fileset_walkthrough(fileset,
            [](const char* filename, void* uargs) {
                static_cast<std::vector<std::string>*>(uargs)->emplace_back(filename);
            }, &files);
        if(rret != REMI_SUCCESS) return 4;

        size_t root_size = 0;
        std::vector<char> root;
        rret = remi_fileset_get_root(fileset, nullptr, &root_size);
        if(rret != REMI_SUCCESS) return 5;
        root.resize(root_size);
        rret = remi_fileset_get_root(fileset, root.data(), &root_size);
        if(rret != REMI_SUCCESS) return 6;

        auto root_str = std::string{root.data()};
        if(root_str.empty() || root_str.back() != '/')
            root_str += "/";
        for(auto& filename : files) {
            filename = root_str + filename;
        }

        auto target = TargetFactory::recoverTarget(type, m_engine, config_json, files);
        if(!target.success()) {
            error("{}", target.error());
            return 7;
        }

        m_target = std::move(target.value());

        return 0;
    }
#endif
};

}

#endif
