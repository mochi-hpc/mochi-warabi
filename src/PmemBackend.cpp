/*
 * (C) 2020 The University of Chicago
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
    if(regionID.content[0] != sizeof(PMEMoid)+1) {
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
            char* regionPtr,
            size_t regionSize)
    : m_engine(std::move(engine))
    , m_pmem_pool(pool)
    , m_id(std::move(id))
    , m_region_ptr(regionPtr)
    , m_region_size(regionSize) {}

    thallium::engine m_engine;
    PMEMobjpool*     m_pmem_pool;
    RegionID         m_id;
    char*            m_region_ptr;
    size_t           m_region_size;

    Result<std::vector<std::pair<void*, size_t>>> convertToSegments(
        const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes) {
        Result<std::vector<std::pair<void*, size_t>>> result;
        auto& segments = result.value();
        segments.reserve(regionOffsetSizes.size());
        for(size_t i=0; i < regionOffsetSizes.size(); ++i) {
            if(regionOffsetSizes[i].second == 0) continue;
            if(regionOffsetSizes[i].first + regionOffsetSizes[i].second > m_region_size) {
                result.success() = false;
                result.error() = "Trying to access region outside of its bounds";
                return result;
            }
            segments.push_back({m_region_ptr + regionOffsetSizes[i].first,
                                regionOffsetSizes[i].second});
        }
        return result;
    }

    Result<RegionID> getRegionID() override {
        Result<RegionID> result;
        result.value() = m_id;
        return result;
    }

    Result<size_t> getSize() override {
        Result<size_t> result;
        result.value() = m_region_size;
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
        if(!segments.success()) {
            result.error() = segments.error();
            result.success() = false;
            return result;
        }
        if(segments.value().size() == 0) return result;
        size_t totalSize = std::accumulate(
            segments.value().begin(), segments.value().end(), (size_t)0,
            [](size_t acc, const auto& pair) { return acc + pair.second; });
        auto localBulk = m_engine.expose(segments.value(), thallium::bulk_mode::write_only);
        localBulk << remoteBulk.on(address)(remoteBulkOffset, totalSize);
        return result;
    }

    Result<bool> write(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            const void* data, bool persist) override {
        (void)persist;
        Result<bool> result;
        auto segments = convertToSegments(regionOffsetSizes);
        if(!segments.success()) {
            result.error() = segments.error();
            result.success() = false;
            return result;
        }
        size_t offset = 0;
        const char* ptr = (const char*)data;
        if(persist) {
            for(auto& segment : segments.value()) {
                pmemobj_memcpy_persist(m_pmem_pool, segment.first, ptr + offset, segment.second);
                offset += segment.second;
            }
        } else {
            for(auto& segment : segments.value()) {
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
            if(regionOffsetSizes[i].first + regionOffsetSizes[i].second > m_region_size) {
                result.success() = false;
                result.error() = "Trying to access region outside of its bounds";
                return result;
            }
        }
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
        if(!segments.success()) {
            result.error() = segments.error();
            result.success() = false;
            return result;
        }
        if(segments.value().size() == 0) return result;
        size_t totalSize = std::accumulate(
            segments.value().begin(), segments.value().end(), (size_t)0,
            [](size_t acc, const auto& pair) { return acc + pair.second; });
        auto localBulk = m_engine.expose(segments.value(), thallium::bulk_mode::read_only);
        localBulk >> remoteBulk.on(address)(remoteBulkOffset, totalSize);
        return result;
     }

    Result<bool> read(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            void* data) override {
        Result<bool> result;
        auto segments = convertToSegments(regionOffsetSizes);
        if(!segments.success()) {
            result.error() = segments.error();
            result.success() = false;
            return result;
        }
        if(segments.value().size() == 0) return result;
        size_t offset = 0;
        char* ptr = (char*)data;
        for(auto& segment : segments.value()) {
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
    Result<bool> result;
    if(m_pmem_pool) {
        pmemobj_close(m_pmem_pool);
        m_pmem_pool = nullptr;
        std::filesystem::remove(m_filename.c_str());
    } else {
        result.value() = false;
        result.error() = "No pmem pool to close";
    }
    return result;
}

std::unique_ptr<WritableRegion> PmemTarget::create(size_t size) {
    if(!m_pmem_pool) return nullptr;
    PMEMoid oid;
    int ret = pmemobj_alloc(m_pmem_pool, &oid, size + sizeof(size_t), 0, NULL, NULL);
    if(ret != 0) {
        // TODO handle error properly
        return nullptr;
    }
    RegionID regionID = PMEMoidToRegionID(oid);
    char* ptr = (char*)pmemobj_direct_inline(oid);
    std::memcpy(ptr, &size, sizeof(size));
    pmemobj_persist(m_pmem_pool, ptr, sizeof(size));
    char* regionPtr = ptr + sizeof(size);
    return std::make_unique<PmemRegion>(m_engine, m_pmem_pool, regionID, regionPtr, size);
}

std::unique_ptr<WritableRegion> PmemTarget::write(const RegionID& region_id, bool persist) {
    (void)persist;
    Result<PMEMoid> oid = RegionIDtoPMEMoid(region_id);
    if(!oid.success()) {
        return nullptr;
    }
    char* ptr = (char*)pmemobj_direct_inline(oid.value());
    size_t size = 0;
    std::memcpy(&size, ptr, sizeof(size));
    char* regionPtr = ptr + sizeof(size);
    return std::make_unique<PmemRegion>(m_engine, m_pmem_pool, region_id, regionPtr, size);
}

std::unique_ptr<ReadableRegion> PmemTarget::read(const RegionID& region_id) {
    Result<PMEMoid> oid = RegionIDtoPMEMoid(region_id);
    if(!oid.success()) {
        return nullptr;
    }
    char* ptr = (char*)pmemobj_direct_inline(oid.value());
    size_t size = 0;
    std::memcpy(&size, ptr, sizeof(size));
    char* regionPtr = ptr + sizeof(size);
    return std::make_unique<PmemRegion>(m_engine, m_pmem_pool, region_id, regionPtr, size);
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

std::unique_ptr<warabi::Backend> PmemTarget::create(const thallium::engine& engine, const json& config) {
    const auto& path = config["path"].get_ref<const std::string&>();
    size_t create_if_missing_with_size = config.value("create_if_missing_with_size", 0);
    bool override_if_exists = config.value("override_if_exists", false);

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
        std::cerr << pmemobj_errormsg() << std::endl;
        return nullptr;
    }

    return std::make_unique<warabi::PmemTarget>(engine, config, pool);
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
