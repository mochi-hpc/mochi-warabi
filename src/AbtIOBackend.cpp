/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <valijson/adapters/nlohmann_json_adapter.hpp>
#include <valijson/schema.hpp>
#include <valijson/schema_parser.hpp>
#include <valijson/validator.hpp>
#include "AbtIOBackend.hpp"
#include <fmt/format.h>
#include <filesystem>
#include <iostream>

namespace warabi {

WARABI_REGISTER_BACKEND(pmdk, AbtIOTarget);

static inline RegionID OffsetToRegionID(const size_t offset) {
    return RegionID{static_cast<const void*>(&offset), sizeof(offset)};
}

static inline Result<size_t> RegionIDtoOffset(const RegionID& regionID) {
    Result<size_t> result;
    if(regionID.content[0] != sizeof(size_t)+1) {
        result.error() = "Invalid RegionID format";
        result.success() = false;
    } else {
        std::memcpy(&result.value(), regionID.content+1, sizeof(size_t));
    }
    return result;
}

struct AbtIORegion : public WritableRegion, public ReadableRegion {

    AbtIORegion(
            AbtIOTarget* owner,
            RegionID id,
            size_t regionOffset,
            size_t regionSize)
    : m_owner(owner)
    , m_id(std::move(id))
    , m_region_offset(regionOffset)
    , m_region_size(regionSize) {}

    AbtIOTarget*     m_owner;
    RegionID         m_id;
    size_t           m_region_offset;
    size_t           m_region_size;

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
#if 0
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
#endif
        return result;
    }

    Result<bool> write(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            const void* data, bool persist) override {
        (void)persist;
        Result<bool> result;
#if 0
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
#endif
        return result;
    }

    Result<bool> persist(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes) override {
        Result<bool> result;
#if 0
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
#endif
        return result;
    }

    Result<bool> read(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            thallium::bulk remoteBulk,
            const thallium::endpoint& address,
            size_t remoteBulkOffset) override {
        Result<bool> result;
#if 0
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
#endif
        return result;
     }

    Result<bool> read(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            void* data) override {
        Result<bool> result;
#if 0
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
#endif
        return result;
    }
};

AbtIOTarget::AbtIOTarget(thallium::engine engine, const json& config,
                         abt_io_instance_id abtio, int fd, size_t file_size)
: m_engine(std::move(engine))
, m_config(config)
, m_abtio(abtio)
, m_fd(fd)
, m_file_size(file_size)
, m_filename(config["path"].get_ref<const std::string&>()) {}

AbtIOTarget::~AbtIOTarget() {
    if(m_fd) abt_io_close(m_abtio, m_fd);
}

std::string AbtIOTarget::getConfig() const {
    return m_config.dump();
}

Result<bool> AbtIOTarget::destroy() {
    Result<bool> result;
    if(m_fd) {
        abt_io_close(m_abtio, m_fd);
        std::filesystem::remove(m_filename.c_str());
    } else {
        result.value() = false;
        result.error() = "Target already destroyed";
    }
    return result;
}

Result<std::unique_ptr<WritableRegion>> AbtIOTarget::create(size_t size) {
    Result<std::unique_ptr<WritableRegion>> result;
    if(!m_fd) {
        result.success() = false;
        result.error() = fmt::format("File {} has been destroyed", m_filename);
        return result;
    }
    // TODO
    //result.value() = std::make_unique<AbtIORegion>(m_engine, m_pmem_pool, regionID, regionPtr, size);
    return result;
}

Result<std::unique_ptr<WritableRegion>> AbtIOTarget::write(const RegionID& region_id, bool persist) {
    Result<std::unique_ptr<WritableRegion>> result;
    if(!m_fd) {
        result.success() = false;
        result.error() = fmt::format("File {} has been destroyed", m_filename);
        return result;
    }
    // TODO
    // result.value() = std::make_unique<AbtIORegion>(m_engine, m_pmem_pool, region_id, regionPtr, size);
    return result;
}

Result<std::unique_ptr<ReadableRegion>> AbtIOTarget::read(const RegionID& region_id) {
    Result<size_t> regionOffset = RegionIDtoOffset(region_id);
    Result<std::unique_ptr<ReadableRegion>> result;
    if(!regionOffset.success()) {
        result.success() = false;
        result.error() = regionOffset.error();
        return result;
    }
    // TODO
    //result.value() = std::make_unique<AbtIORegion>(m_engine, m_pmem_pool, region_id, regionPtr, size);
    return result;
}

Result<bool> AbtIOTarget::erase(const RegionID& region_id) {
    Result<size_t> regionOffset = RegionIDtoOffset(region_id);
    Result<bool> result;
    if(!regionOffset.success()) {
        result.success() = false;
        result.error() = regionOffset.error();
        return result;
    }
    return result;
}

Result<std::unique_ptr<warabi::Backend>> AbtIOTarget::create(const thallium::engine& engine, const json& cfg) {
    auto config              = cfg;
    const auto& path         = config["path"].get_ref<const std::string&>();
    bool override_if_exists  = config.value("override_if_exists", false);
    bool directio            = config.value("directio", true);
    abt_io_instance_id abtio = ABT_IO_INSTANCE_NULL;

    Result<std::unique_ptr<warabi::Backend>> result;

    if(config.contains("abt_io")) {
        auto abtio_config = config["abt_io"].dump();
        struct abt_io_init_info args = {
            abtio_config.c_str(),
            ABT_POOL_NULL
        };
        abtio = abt_io_init_ext(&args);
    } else {
        abtio = abt_io_init(1);
    }
    if(abtio == ABT_IO_INSTANCE_NULL) {
        result.success() = false;
        result.error() = "Could not create ABT-IO instance";
        return result;
    }

    bool file_exists = std::filesystem::exists(path);
    if(file_exists && override_if_exists) {
        std::filesystem::remove(path.c_str());
        file_exists = false;
    }
    int fd = 0;
    if(!file_exists) {
        std::filesystem::create_directories(std::filesystem::path{path}.parent_path());
        fd = open(path.c_str(), O_EXCL|O_WRONLY|O_CREAT, 0644);
        if(!fd) {
            result.success() = false;
            result.error() = fmt::format("Could not open file {}: {}", path, strerror(errno));
            return result;
        }
        close(fd);
    }
    int oflags = O_RDWR;
    if(directio) oflags |= O_DIRECT;
retry_without_odirect:
    fd = abt_io_open(abtio, path.c_str(), oflags, 0);
    if(fd == -EINVAL && directio) {
        oflags = O_RDWR;
        config["directio"] = false;
        goto retry_without_odirect;
    }

    if(fd <= 0) {
        result.success() = false;
        result.error() = fmt::format(
            "Failed to open file {} using abt_io_open: {}", path, strerror(-fd));
        return result;
    }

    size_t file_size = 0;
    struct stat statbuf;
    int ret = fstat(fd, &statbuf);
    if(ret < 0) {
        result.success() = false;
        result.error() = fmt::format("Could not fstat {}: {}", path, strerror(errno));
        return result;
    }
    file_size = statbuf.st_size;

    result.value() = std::make_unique<warabi::AbtIOTarget>(
        engine, config, abtio, fd, file_size);
    return result;
}

static constexpr const char* configSchema = R"(
{
  "type": "object",
  "properties": {
    "path": {"type": "string"},
    "create_if_missing": {"type": "boolean"},
    "override_if_exists": {"type": "boolean"},
    "alignment": {"type": "integer", "minimum": 0},
    "sync": {"type": "boolean"},
    "directio": {"type": "boolean"},
    "abt_io": {"type": "object"}
  },
  "required": ["path"]
}
)";

Result<bool> AbtIOTarget::validate(const json& config) {
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
        ss << "Error(s) while validating JSON config for warabi AbtIOTarget:\n";
        for(auto& err : validationResults) {
            ss << "\t" << err.description;
        }
        result.error() = ss.str();
        result.success() = false;
        return result;
    }

    const auto& path = config["path"].get_ref<const std::string&>();
    bool create_if_missing = config.value("create_if_missing", false);
    bool file_exists = std::filesystem::exists(path);

    if(!file_exists && !create_if_missing) {
        result.error() = fmt::format("File {} does not exist", path);
        result.success() = false;
        return result;
    }

    return result;
}

}
