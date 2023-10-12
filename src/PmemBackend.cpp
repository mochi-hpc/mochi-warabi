/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <valijson/adapters/nlohmann_json_adapter.hpp>
#include <valijson/schema.hpp>
#include <valijson/schema_parser.hpp>
#include <valijson/validator.hpp>
#include "PmemBackend.hpp"
#include <fmt/format.h>
#include <filesystem>
#include <iostream>

namespace warabi {

WARABI_REGISTER_BACKEND(pmdk, PmemTarget);

static inline RegionID PMEMoidToRegionID(const PMEMoid& oid) {
    return RegionID{static_cast<const void*>(&oid), sizeof(oid)};
}

static inline Result<PMEMoid> RegionIDtoPMEMoid(const RegionID& regionID) {
    Result<PMEMoid> result;
    if(!regionID.content || regionID.content[0] != sizeof(PMEMoid)+1) {
        result.error() = "Invalid RegionID format";
        result.success() = false;
    } else {
        std::memcpy(&result.value(), regionID.content+1, sizeof(PMEMoid));
    }
    return result;
}

struct PmemRegion : public WritableRegion, public ReadableRegion {

    PmemRegion(
            thallium::engine engine,
            PMEMobjpool* pool,
            RegionID id,
            char* regionPtr)
    : m_engine(std::move(engine))
    , m_pmem_pool(pool)
    , m_id(std::move(id))
    , m_region_ptr(regionPtr) {}

    thallium::engine m_engine;
    PMEMobjpool*     m_pmem_pool;
    RegionID         m_id;
    char*            m_region_ptr;

    std::vector<std::pair<void*, size_t>> convertToSegments(
        const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes) {
        std::vector<std::pair<void*, size_t>> segments;
        segments.reserve(regionOffsetSizes.size());
        for(size_t i=0; i < regionOffsetSizes.size(); ++i) {
            if(regionOffsetSizes[i].second == 0) continue;
            segments.push_back({m_region_ptr + regionOffsetSizes[i].first,
                                regionOffsetSizes[i].second});
        }
        return segments;
    }

    Result<RegionID> getRegionID() override {
        Result<RegionID> result;
        result.value() = m_id;
        return result;
    }

    Result<bool> write(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            thallium::bulk remoteBulk,
            const thallium::endpoint& address,
            size_t remoteBulkOffset,
            bool persist) override {
        (void)persist;
        Result<bool> result;
        auto segments = convertToSegments(regionOffsetSizes);
        if(segments.size() == 0) return result;
        size_t totalSize = std::accumulate(
            segments.begin(), segments.end(), (size_t)0,
            [](size_t acc, const auto& pair) { return acc + pair.second; });
        auto localBulk = m_engine.expose(segments, thallium::bulk_mode::write_only);
        localBulk << remoteBulk.on(address)(remoteBulkOffset, totalSize);
        return result;
    }

    Result<bool> write(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            const void* data, bool persist) override {
        (void)persist;
        Result<bool> result;
        auto segments = convertToSegments(regionOffsetSizes);
        size_t offset = 0;
        const char* ptr = (const char*)data;
        if(persist) {
            for(auto& segment : segments) {
                pmemobj_memcpy_persist(m_pmem_pool, segment.first, ptr + offset, segment.second);
                offset += segment.second;
            }
        } else {
            for(auto& segment : segments) {
                std::memcpy(segment.first, ptr + offset, segment.second);
                offset += segment.second;
            }
        }
        return result;
    }

    Result<bool> persist(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes) override {
        Result<bool> result;
        for(size_t i=0; i < regionOffsetSizes.size(); ++i) {
            if(regionOffsetSizes[i].second > 0) {
                pmemobj_persist(m_pmem_pool, m_region_ptr + regionOffsetSizes[i].first, regionOffsetSizes[i].second);
            }
        }
        return result;
    }

    Result<bool> read(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            thallium::bulk remoteBulk,
            const thallium::endpoint& address,
            size_t remoteBulkOffset) override {
        Result<bool> result;
        auto segments = convertToSegments(regionOffsetSizes);
        if(segments.size() == 0) return result;
        size_t totalSize = std::accumulate(
            segments.begin(), segments.end(), (size_t)0,
            [](size_t acc, const auto& pair) { return acc + pair.second; });
        auto localBulk = m_engine.expose(segments, thallium::bulk_mode::read_only);
        localBulk >> remoteBulk.on(address)(remoteBulkOffset, totalSize);
        return result;
     }

    Result<bool> read(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            void* data) override {
        Result<bool> result;
        auto segments = convertToSegments(regionOffsetSizes);
        if(segments.size() == 0) return result;
        size_t offset = 0;
        char* ptr = (char*)data;
        for(auto& segment : segments) {
            std::memcpy(ptr + offset, segment.first, segment.second);
            offset += segment.second;
        }
        return result;
    }
};

PmemTarget::PmemTarget(thallium::engine engine, const json& config, PMEMobjpool* pool)
: m_engine(std::move(engine))
, m_config(config)
, m_pmem_pool(pool)
, m_filename(config["path"].get_ref<const std::string&>()) {}

PmemTarget::~PmemTarget() {
    if(m_pmem_pool)
        pmemobj_close(m_pmem_pool);
}

std::string PmemTarget::getConfig() const {
    return m_config.dump();
}

Result<bool> PmemTarget::destroy() {
    pmemobj_close(m_pmem_pool);
    m_pmem_pool = nullptr;
    std::filesystem::remove(m_filename.c_str());
    return Result<bool>{};
}

Result<std::unique_ptr<WritableRegion>> PmemTarget::create(size_t size) {
    Result<std::unique_ptr<WritableRegion>> result;
    PMEMoid oid;
    int ret = pmemobj_alloc(m_pmem_pool, &oid, size, 0, NULL, NULL);
    if(ret != 0) {
        result.success() = false;
        result.error() = fmt::format("pmemobj_alloc failed: {}", pmemobj_errormsg());
        return result;
    }
    RegionID regionID = PMEMoidToRegionID(oid);
    char* ptr = (char*)pmemobj_direct_inline(oid);
    result.value() = std::make_unique<PmemRegion>(m_engine, m_pmem_pool, regionID, ptr);
    return result;
}

Result<std::unique_ptr<WritableRegion>> PmemTarget::write(const RegionID& region_id, bool persist) {
    (void)persist;
    Result<std::unique_ptr<WritableRegion>> result;
    Result<PMEMoid> oid = RegionIDtoPMEMoid(region_id);
    if(!oid.success()) {
        result.success() = false;
        result.error() = oid.error();
        return result;
    }
    char* ptr = (char*)pmemobj_direct_inline(oid.value());
    result.value() = std::make_unique<PmemRegion>(m_engine, m_pmem_pool, region_id, ptr);
    return result;
}

Result<std::unique_ptr<ReadableRegion>> PmemTarget::read(const RegionID& region_id) {
    Result<PMEMoid> oid = RegionIDtoPMEMoid(region_id);
    Result<std::unique_ptr<ReadableRegion>> result;
    if(!oid.success()) {
        result.success() = false;
        result.error() = oid.error();
        return result;
    }
    char* ptr = (char*)pmemobj_direct_inline(oid.value());
    result.value() = std::make_unique<PmemRegion>(m_engine, m_pmem_pool, region_id, ptr);
    return result;
}

Result<bool> PmemTarget::erase(const RegionID& region_id) {
    Result<PMEMoid> oid = RegionIDtoPMEMoid(region_id);
    Result<bool> result;
    if(!oid.success()) {
        result.success() = false;
        result.error() = oid.error();
        return result;
    }
    pmemobj_free(&oid.value());
    return result;
}

Result<std::unique_ptr<MigrationHandle>> PmemTarget::startMigration() {
    Result<std::unique_ptr<MigrationHandle>> result;
    result.success() = false;
    result.error() = "startMigration operation not implemented";
    // TODO
    return result;
}

Result<std::unique_ptr<warabi::Backend>> PmemTarget::create(const thallium::engine& engine, const json& config) {
    const auto& path = config["path"].get_ref<const std::string&>();
    size_t create_if_missing_with_size = config.value("create_if_missing_with_size", 0);
    bool override_if_exists = config.value("override_if_exists", false);

    Result<std::unique_ptr<warabi::Backend>> result;

    PMEMobjpool* pool = nullptr;

    bool file_exists = std::filesystem::exists(path);
    if(file_exists && override_if_exists) {
        std::filesystem::remove(path.c_str());
        file_exists = false;
    }
    if(!file_exists) {
        std::filesystem::create_directories(std::filesystem::path{path}.parent_path());
        pool = pmemobj_create(path.c_str(), NULL, create_if_missing_with_size, 0644);
    } else {
        pool = pmemobj_open(path.c_str(), NULL);
    }
    if(!pool) {
        result.success() = false;
        result.error() = fmt::format(
            "Failed to create or open pmemobj target: {}",
            pmemobj_errormsg());
        return result;
    }

    result.value() = std::make_unique<warabi::PmemTarget>(engine, config, pool);
    return result;
}

static constexpr const char* configSchema = R"(
{
  "type": "object",
  "properties": {
    "path": {"type": "string"},
    "create_if_missing_with_size": {"type": "integer", "minimum": 8388608},
    "override_if_exists": {"type": "boolean"}
  },
  "required": ["path"]
}
)";

Result<bool> PmemTarget::validate(const json& config) {
    static json schemaDocument = json::parse(configSchema);

    valijson::Schema schemaValidator;
    valijson::SchemaParser schemaParser;
    valijson::adapters::NlohmannJsonAdapter schemaAdapter(schemaDocument);
    schemaParser.populateSchema(schemaAdapter, schemaValidator);

    valijson::Validator validator;
    valijson::adapters::NlohmannJsonAdapter jsonAdapter(config);

    valijson::ValidationResults validationResults;
    validator.validate(schemaValidator, jsonAdapter, &validationResults);

    Result<bool> result;

    std::stringstream ss;
    if(validationResults.numErrors() != 0) {
        ss << "Error(s) while validating JSON config for warabi PmemTarget:\n";
        for(auto& err : validationResults) {
            ss << "\t" << err.description;
        }
        result.error() = ss.str();
        result.success() = false;
        return result;
    }

    const auto& path = config["path"].get_ref<const std::string&>();
    size_t create_if_missing_with_size = config.value("create_if_missing_with_size", 0);
    bool override_if_exists = config.value("override_if_exists", false);
    bool file_exists = std::filesystem::exists(path);

    if(!file_exists && !create_if_missing_with_size) {
        result.error() = fmt::format(
            "File {} does not exist but"
            " \"create_if_missing_with_size\""
            " was not specified in configuration", path);
        result.success() = false;
        return result;
    }

    if(override_if_exists && !create_if_missing_with_size) {
        result.error() = fmt::format(
            "\"override_if_exists\" set to true but"
            " \"create_if_missing_with_size\" not specified");
        result.success() = false;
        return result;
    }

    return result;
}

}
