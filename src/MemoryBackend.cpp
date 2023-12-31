/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "MemoryBackend.hpp"
#include <iostream>

namespace warabi {

WARABI_REGISTER_BACKEND(memory, MemoryTarget);

struct MemoryRegion : public WritableRegion, public ReadableRegion {

    MemoryRegion(
            thallium::engine engine,
            RegionID id,
            std::vector<char>& region,
            std::unique_lock<thallium::mutex>&& lock)
    : m_engine(std::move(engine))
    , m_id(std::move(id))
    , m_region(region)
    , m_lock(std::move(lock)) {}

    thallium::engine                  m_engine;
    RegionID                          m_id;
    std::vector<char>&                m_region;
    std::unique_lock<thallium::mutex> m_lock;

    std::vector<std::pair<void*, size_t>> convertToSegments(
        const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes) {
        std::vector<std::pair<void*, size_t>> segments;
        segments.reserve(regionOffsetSizes.size());
        for(size_t i=0; i < regionOffsetSizes.size(); ++i) {
            if(regionOffsetSizes[i].second == 0) continue;
            segments.push_back({m_region.data() + regionOffsetSizes[i].first,
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
        for(auto& segment : segments) {
            std::memcpy(segment.first, ptr + offset, segment.second);
            offset += segment.second;
        }
        return result;
    }

    Result<bool> persist(
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes) override {
        (void)regionOffsetSizes;
        return Result<bool>{};
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

MemoryTarget::MemoryTarget(thallium::engine engine, const json& config)
: m_engine(std::move(engine))
, m_config(config) {}

std::string MemoryTarget::getConfig() const {
    return m_config.dump();
}

Result<bool> MemoryTarget::destroy() {
    Result<bool> result;
    result.value() = true;
    return result;
}

Result<std::unique_ptr<WritableRegion>> MemoryTarget::create(size_t size) {
    Result<std::unique_ptr<WritableRegion>> result;
    auto lock = std::unique_lock<thallium::mutex>{m_mutex};
    m_regions.emplace_back(size);
    auto& region = m_regions.back();
    uint64_t index = m_regions.size() - 1;
    RegionID region_id;
    uint64_t s = size;
    std::memcpy(region_id.data(), static_cast<void*>(&index), sizeof(index));
    std::memcpy(region_id.data() + sizeof(index), static_cast<void*>(&s), sizeof(s));
    result.value() = std::make_unique<MemoryRegion>(m_engine, region_id, region, std::move(lock));
    return result;
}

ssize_t MemoryTarget::regiondIDtoIndex(const RegionID& regionID) {
    uint64_t r = 0;
    std::memcpy(&r, regionID.data(), sizeof(r));
    return static_cast<ssize_t>(r);
}

Result<std::unique_ptr<WritableRegion>> MemoryTarget::write(const RegionID& region_id, bool persist) {
    (void)persist;
    Result<std::unique_ptr<WritableRegion>> result;
    auto index = regiondIDtoIndex(region_id);
    if(index < 0) {
        result.error() = "Invalid RegionID information";
        result.success() = false;
        return result;
    }
    auto lock = std::unique_lock<thallium::mutex>{m_mutex};
    if(index >= (ssize_t)m_regions.size()) {
        result.error() = "Invalid RegionID information";
        result.success() = false;
        return result;
    }
    result.value() = std::make_unique<MemoryRegion>(m_engine, region_id, m_regions[index], std::move(lock));
    return result;
}

Result<std::unique_ptr<ReadableRegion>> MemoryTarget::read(const RegionID& region_id) {
    Result<std::unique_ptr<ReadableRegion>> result;
    auto index = regiondIDtoIndex(region_id);
    if(index < 0) {
        result.error() = "Invalid RegionID information";
        result.success() = false;
        return result;
    }
    auto lock = std::unique_lock<thallium::mutex>{m_mutex};
    if(index >= (ssize_t)m_regions.size()) {
        result.error() = "Invalid RegionID information";
        result.success() = false;
        return result;
    }
    result.value() = std::make_unique<MemoryRegion>(m_engine, region_id, m_regions[index], std::move(lock));
    return result;
}

Result<bool> MemoryTarget::erase(const RegionID& region_id) {
    auto index = regiondIDtoIndex(region_id);
    Result<bool> result;
    auto lock = std::unique_lock<thallium::mutex>{m_mutex};
    if(index < 0 || index >= (ssize_t)m_regions.size()) {
        result.error() = "Invalid RegionID";
        result.success() = false;
        return result;
    }
    m_regions[index].clear();
    return result;
}

Result<std::unique_ptr<MigrationHandle>> MemoryTarget::startMigration(bool removeSource) {
    Result<std::unique_ptr<MigrationHandle>> result;
    result.success() = false;
    result.error() = "startMigration operation not implemented";
    (void)removeSource;
    // TODO
    return result;
}

Result<std::unique_ptr<warabi::Backend>> MemoryTarget::recover(
        const thallium::engine& engine, const json& config,
        const std::vector<std::string>& filenames) {
    Result<std::unique_ptr<warabi::Backend>> result;
    (void)engine;
    (void)config;
    (void)filenames;
    // TODO
    return result;
}

Result<std::unique_ptr<warabi::Backend>> MemoryTarget::create(const thallium::engine& engine, const json& config) {
    Result<std::unique_ptr<warabi::Backend>> result;
    result.value() = std::unique_ptr<warabi::Backend>(new MemoryTarget(engine, config));
    return result;
}

Result<bool> MemoryTarget::validate(const json& config) {
    (void)config;
    return Result<bool>{};
}

}
