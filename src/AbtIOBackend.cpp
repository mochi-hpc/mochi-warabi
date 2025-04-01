/*
 * (C) 2023 The University of Chicago
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
#include "AbtIOBackend.hpp"
#include "Defer.hpp"
#include <nlohmann/json.hpp>
#include <nlohmann/json-schema.hpp>
#include <fmt/format.h>
#include <filesystem>
#include <iostream>

namespace warabi {

using nlohmann::json;
using nlohmann::json_schema::json_validator;

WARABI_REGISTER_BACKEND(abtio, AbtIOTarget);

static inline auto OffsetSizeToRegionID(const size_t offset, const size_t size) {
    const uint64_t o = offset, s = size;
    RegionID rid;
    std::memcpy(rid.data(), &o, sizeof(o));
    std::memcpy(rid.data() + sizeof(o), &s, sizeof(s));
    return rid;
}

static inline auto RegionIDtoOffsetSize(const RegionID& rid) {
    std::pair<uint64_t,uint64_t> p;
    std::memcpy(&p.first, rid.data(), sizeof(p.first));
    std::memcpy(&p.second, rid.data() + sizeof(p.first), sizeof(p.second));
    return p;
}

struct AbtIORegion : public WritableRegion, public ReadableRegion {

    AbtIORegion(
            AbtIOTarget* owner,
            RegionID id,
            size_t regionOffset)
    : m_owner(owner)
    , m_id(std::move(id))
    , m_region_offset(regionOffset) {}

    AbtIOTarget*     m_owner;
    RegionID         m_id;
    size_t           m_region_offset;

    ~AbtIORegion() {
        m_owner->m_migration_lock.unlock();
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
        void* data = nullptr;
        size_t size = std::accumulate(
            regionOffsetSizes.begin(), regionOffsetSizes.end(), (size_t)0,
            [](size_t acc, const std::pair<size_t, size_t>& p) { return acc + p.second; });
        int ret = posix_memalign((void**)(&data), m_owner->m_alignment, size);
        // LCOV_EXCL_START
        if(ret != 0) {
            result.error() = fmt::format("posix_memalign failed in write: {}", strerror(ret));
            result.success() = false;
            return result;
        }
        // LCOV_EXCL_STOP
        auto localBulk = m_owner->m_engine.expose({{data, size}}, thallium::bulk_mode::write_only);
        localBulk << remoteBulk.on(address)(remoteBulkOffset, size);
        result = write(regionOffsetSizes, data, persist);
        free(data);
        return result;
    }

    Result<bool> write(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            const void* data, bool persist) override {
        (void)persist;
        Result<bool> result;
        std::vector<abt_io_op*> ops;
        ops.reserve(regionOffsetSizes.size());

        const char* ptr = static_cast<const char*>(data);
        size_t offset = 0;
        for(const auto& seg : regionOffsetSizes) {
            ssize_t remaining = seg.second;
            while(remaining) {
                auto s = abt_io_pwrite(
                    m_owner->m_abtio,
                    m_owner->m_fd,
                    ptr + offset,
                    remaining,
                    m_region_offset + seg.first);
                if(s <= 0) {
                    result.success() = false;
                    result.error() = fmt::format(
                        "abt_io_pwrite failed in write: {}", strerror(-s));
                    return result;
                }
                offset += s;
                remaining -= s;
            }
        }
        if(persist) {
            auto ret = abt_io_fdatasync(m_owner->m_abtio, m_owner->m_fd);
            if(ret != 0) {
                result.success() = false;
                result.error() = "Persist failed (abt_io_fdatasync returned -1)";
            }
        }
        return result;
    }

    Result<bool> persist(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes) override {
        (void)regionOffsetSizes;
        Result<bool> result;
        int ret = abt_io_fdatasync(m_owner->m_abtio, m_owner->m_fd);
        if(ret != 0) {
            result.success() = false;
            result.error() = "Persist failed (abt_io_fdatasync returned -1)";
        }
        return result;
    }

    Result<bool> read(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            thallium::bulk remoteBulk,
            const thallium::endpoint& address,
            size_t remoteBulkOffset) override {
        Result<bool> result;
        void* data = nullptr;
        size_t size = std::accumulate(
            regionOffsetSizes.begin(), regionOffsetSizes.end(), (size_t)0,
            [](size_t acc, const std::pair<size_t, size_t>& p) { return acc + p.second; });
        int ret = posix_memalign((void**)(&data), m_owner->m_alignment, size);
        if(ret != 0) {
            result.error() = fmt::format("posix_memalign failed in write: {}", strerror(ret));
            result.success() = false;
            return result;
        }
        result = read(regionOffsetSizes, data);
        if(!result.success()) {
            free(data);
            return result;
        }
        auto localBulk = m_owner->m_engine.expose({{data, size}}, thallium::bulk_mode::read_only);
        localBulk >> remoteBulk.on(address)(remoteBulkOffset, size);
        free(data);
        return result;
     }

    Result<bool> read(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            void* data) override {
        Result<bool> result;
        std::vector<abt_io_op*> ops(regionOffsetSizes.size());
        std::vector<ssize_t> rets(regionOffsetSizes.size());

        char* ptr = static_cast<char*>(data);
        size_t offset = 0;
        int i = 0;
        for(const auto& seg : regionOffsetSizes) {
            abt_io_op* op = abt_io_pread_nb(
                m_owner->m_abtio,
                m_owner->m_fd,
                ptr + offset,
                seg.second,
                m_region_offset + seg.first,
                rets.data() + i);
            ops[i] = op;
            offset += seg.second;
            i += 1;
        }
        int ret = 0;
        for(auto& op : ops) {
            ret = abt_io_op_wait(op);
            abt_io_op_free(op);
            if(ret != 0) {
                result.success() = false;
                result.error() = "Read failed (abt_io_op_wait returned -1)";
            }
        }
        if(!result.success())
            return result;
        for(auto& r : rets) {
            if(r < 0) {
                result.success() = false;
                result.error() = fmt::format("Read failed: {}", strerror(-r));
            }
        }
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
, m_filename(config["path"].get_ref<const std::string&>())
, m_alignment(config.value("alignment", 8))
{}

AbtIOTarget::~AbtIOTarget() {
    if(m_fd && m_abtio) abt_io_close(m_abtio, m_fd);
    if(m_abtio) abt_io_finalize(m_abtio);
}

std::string AbtIOTarget::getConfig() const {
    return m_config.dump();
}

Result<bool> AbtIOTarget::destroy() {
    Result<bool> result;
    abt_io_close(m_abtio, m_fd);
    m_fd = 0;
    std::filesystem::remove(m_filename.c_str());
    abt_io_finalize(m_abtio);
    m_abtio = nullptr;
    return result;
}

#define WARABI_ALIGN_UP(x, _alignment) \
    ((((unsigned long)(x)) + (_alignment - 1)) & (~(_alignment - 1)))

Result<std::unique_ptr<WritableRegion>> AbtIOTarget::create(size_t size) {
    Result<std::unique_ptr<WritableRegion>> result;
    size_t alignedSize = WARABI_ALIGN_UP(size, m_alignment);
    size_t offset = m_file_size.fetch_add(alignedSize);
    auto regionID = OffsetSizeToRegionID(offset, alignedSize);

    void* zero_block = nullptr;
    int ret = posix_memalign((void**)(&zero_block), m_alignment, size);
    if(ret != 0) {
        result.error() = fmt::format("posix_memalign failed in create: {}", strerror(ret));
        result.success() = false;
        return result;
    }
    m_migration_lock.rdlock();
    ssize_t remaining = alignedSize;
    size_t off = offset;
    while(remaining) {
        ssize_t s = abt_io_pwrite(m_abtio, m_fd, zero_block, remaining, off);
        if(s <= 0) {
            result.error() = fmt::format("abt_io_pwrite failed in create: {}", strerror(-s));
            result.success() = false;
            m_migration_lock.unlock();
            return result;
        }
        remaining -= s;
        off += s;
    }
    result.value() = std::make_unique<AbtIORegion>(this, regionID, offset);
    return result;
}

Result<std::unique_ptr<WritableRegion>> AbtIOTarget::write(const RegionID& region_id, bool persist) {
    (void)persist;
    Result<std::unique_ptr<WritableRegion>> result;
    m_migration_lock.rdlock();
    if(!m_fd) {
        result.success() = false;
        result.error() = fmt::format("File {} has been destroyed", m_filename);
        m_migration_lock.unlock();
        return result;
    }
    auto regionOffsetSize = RegionIDtoOffsetSize(region_id);
    result.value() = std::make_unique<AbtIORegion>(
        this, region_id, regionOffsetSize.first);
    return result;
}

Result<std::unique_ptr<ReadableRegion>> AbtIOTarget::read(const RegionID& region_id) {
    Result<std::unique_ptr<ReadableRegion>> result;
    auto regionOffsetSize = RegionIDtoOffsetSize(region_id);
    m_migration_lock.rdlock();
    result.value() = std::make_unique<AbtIORegion>(
        this, region_id, regionOffsetSize.first);
    return result;
}

Result<bool> AbtIOTarget::erase(const RegionID& region_id) {
    Result<bool> result;
    auto regionOffsetSize = RegionIDtoOffsetSize(region_id);
    m_migration_lock.rdlock();
    int ret = abt_io_fallocate(
        m_abtio, m_fd,
        FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
        regionOffsetSize.first, regionOffsetSize.second);
    if(ret != 0) {
        result.error() = "abt_io_fallocate failed to erase region";
        result.success() = false;
    }
    m_migration_lock.unlock();

    return result;
}

Result<std::unique_ptr<MigrationHandle>> AbtIOTarget::startMigration(bool removeSource) {
    Result<std::unique_ptr<MigrationHandle>> result;
    result.value() = std::make_unique<AbtIOMigrationHandle>(this, removeSource);
    return result;
}

Result<std::unique_ptr<warabi::Backend>> AbtIOTarget::recover(
        const thallium::engine& engine, const json& cfg,
        const std::vector<std::string>& filenames) {
    Result<std::unique_ptr<warabi::Backend>> result;
    if(filenames.size() == 0) {
        result.error() = "No file to recover from";
        result.success() = false;
        return result;
    }
    if(filenames.size() > 1) {
        result.error() = "AbtIO backend cannot recover from multiple files";
        result.success() = false;
        return result;
    }
    auto config              = cfg;
    config["path"]           = filenames[0];
    bool directio            = config.value("directio", false);
    const auto& path         = filenames[0];
    abt_io_instance_id abtio = ABT_IO_INSTANCE_NULL;

    bool file_exists = std::filesystem::exists(path);
    if(!file_exists) {
        result.error() = fmt::format("File {} not found", path);
        result.success() = false;
        return result;
    }

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

    int fd = 0;
    int oflags = O_RDWR;
    if(directio) oflags |= O_DIRECT;
retry_without_odirect:
    fd = abt_io_open(abtio, path.c_str(), oflags, 0);
    if(fd == -EINVAL && directio) {
        oflags = O_RDWR;
        config["directio"] = false;
        directio = false;
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

Result<std::unique_ptr<warabi::Backend>> AbtIOTarget::create(const thallium::engine& engine, const json& cfg) {
    auto config              = cfg;
    const auto& path         = config["path"].get_ref<const std::string&>();
    bool override_if_exists  = config.value("override_if_exists", false);
    bool directio            = config.value("directio", false);
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
            abt_io_finalize(abtio);
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
        directio = false;
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

Result<bool> AbtIOTarget::validate(const json& config) {
    static const json schema = R"(
    {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "create_if_missing": {"type": "boolean"},
            "override_if_exists": {"type": "boolean"},
            "alignment": {"type": "integer", "minimum": 8, "multipleOf": 8},
            "sync": {"type": "boolean"},
            "directio": {"type": "boolean"},
            "abt_io": {"type": "object"}
        },
        "required": ["path"]
    }
    )"_json;

    Result<bool> result;

    json_validator validator;
    validator.set_root_schema(schema);
    try {
        validator.validate(config);
    } catch(const std::exception& ex) {
        result.success() = false;
        result.error() = fmt::format(
            "Error(s) while validating JSON config for warabi AbtIOTarget: {}", ex.what());
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
