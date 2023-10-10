/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/TransferManager.hpp"

#include <valijson/adapters/nlohmann_json_adapter.hpp>
#include <valijson/schema.hpp>
#include <valijson/schema_parser.hpp>
#include <valijson/validator.hpp>

#include <margo-bulk-pool.h>
#include <thallium.hpp>

namespace warabi {

static constexpr const char* configSchema = R"(
{
  "type": "object",
  "properties": {
    "num_pools": {"type": "integer", "minimum": 1},
    "num_buffers_per_pool": {"type": "integer", "minimum": 1},
    "first_buffer_size": {"type": "integer", "minimum": 1},
    "buffer_size_multiple": {"type": "integer", "exclusiveMinimum": 1}
  },
  "required": ["num_pools", "num_buffers_per_pool", "first_buffer_size", "buffer_size_multiple"]
}
)";

namespace tl = thallium;

class PipelineTransferManager : public TransferManager {

    using json = nlohmann::json;

    tl::engine           m_engine;
    json                 m_config;
    margo_bulk_poolset_t m_poolset;

    public:

    PipelineTransferManager(tl::engine engine, json config, margo_bulk_poolset_t poolset)
    : m_engine(std::move(engine))
    , m_config(std::move(config))
    , m_poolset(poolset) {}

    PipelineTransferManager(PipelineTransferManager&&) = default;
    PipelineTransferManager(const PipelineTransferManager&) = default;
    PipelineTransferManager& operator=(PipelineTransferManager&&) = default;
    PipelineTransferManager& operator=(const PipelineTransferManager&) = default;

    ~PipelineTransferManager() {
        margo_bulk_poolset_destroy(m_poolset);
    }

    std::string getConfig() const override {
        return m_config.dump();
    }

    Result<bool> pull(
            WritableRegion& region,
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            thallium::bulk data,
            thallium::endpoint address,
            size_t bulkOffset,
            bool persist) override {
        // get the maximum size of buffers we can get from the poolset
        hg_size_t maxBufferSize = 0;
        margo_bulk_poolset_get_max(m_poolset, &maxBufferSize);
        // make a copy of the regionOffsetSizes vector with segments
        // that are guaranteed to be small enough for a single buffer
        // (i.e. split segments that are too big)
        size_t currentListSize = maxBufferSize;
        std::vector<std::vector<std::pair<size_t, size_t>>> regionOffsetSizesSets;
        std::vector<size_t>                                 bulkOffsets;

        for(auto& p : regionOffsetSizes) {
            size_t offset = p.first;
            size_t remaining = p.second;
            while(remaining) {
                auto size = std::min(remaining, maxBufferSize);
                if(currentListSize + size > maxBufferSize) {
                    regionOffsetSizesSets.emplace_back();
                    regionOffsetSizesSets.back().reserve(regionOffsetSizes.size());
                    bulkOffsets.push_back(bulkOffset);
                    currentListSize = 0;
                } else {
                    currentListSize += size;
                    bulkOffset += size;
                }
                regionOffsetSizesSets.back().push_back({offset, size});
                remaining -= size;
            }
        }
        // issue the transfers and write calls in parallel
        std::vector<tl::managed<tl::thread>> ults;
        std::vector<Result<bool>> ultResults;
        ults.reserve(bulkOffsets.size());
        ultResults.resize(bulkOffsets.size());
        for(size_t i = 0; i < bulkOffsets.size(); ++i) {
            ults.push_back(
                tl::thread::self().get_last_pool().make_thread(
                    [&region, &data, &address, persist, i, this,
                     &regionOffsetSizesSets, bulkOffset=bulkOffsets[i],
                     &ultResults]() mutable {
                    auto& regionOffsetSizes = regionOffsetSizesSets[i];
                    auto& result = ultResults[i];
                    // compute the size of this list of segments
                    auto size = std::accumulate(
                        regionOffsetSizes.begin(), regionOffsetSizes.end(), (size_t)0,
                        [](size_t s, const auto& p) { return s + p.second; });
                    // get a buffer into which to receive the data
                    hg_bulk_t bulk = HG_BULK_NULL;
                    margo_bulk_poolset_get(m_poolset, size, &bulk);
                    // wrap it in a thallium bulk
                    auto localBulk = m_engine.wrap(bulk, true);
                    // issue the transfer
                    localBulk << data.on(address).select(bulkOffset, size);
                    // access the underlying memory
                    void* bufPtr = nullptr;
                    hg_size_t bufSize = 0;
                    hg_uint32_t actualCount = 0;
                    margo_bulk_access(bulk, 0, size, HG_BULK_READWRITE, 1, &bufPtr, &bufSize, &actualCount);
                    // write the data into the region
                    result = region.write(regionOffsetSizes, bufPtr, persist);
                    // release the buffer
                    margo_bulk_poolset_release(m_poolset, bulk);
            }));
        }
        Result<bool> result;
        for(size_t i = 0; i < ults.size(); ++i) {
            ults[i]->join();
            if(!ultResults[i].success())
                result = ultResults[i];
        }
        return result;
    }

    Result<bool> push(
            ReadableRegion& region,
            const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
            thallium::bulk data,
            thallium::endpoint address,
            size_t bulkOffset) override {
        // get the maximum size of buffers we can get from the poolset
        hg_size_t maxBufferSize = 0;
        margo_bulk_poolset_get_max(m_poolset, &maxBufferSize);
        // make a copy of the regionOffsetSizes vector with segments
        // that are guaranteed to be small enough for a single buffer
        // (i.e. split segments that are too big)
        size_t currentListSize = maxBufferSize;
        std::vector<std::vector<std::pair<size_t, size_t>>> regionOffsetSizesSets;
        std::vector<size_t>                                 bulkOffsets;

        for(auto& p : regionOffsetSizes) {
            size_t offset = p.first;
            size_t remaining = p.second;
            while(remaining) {
                auto size = std::min(remaining, maxBufferSize);
                if(currentListSize + size > maxBufferSize) {
                    regionOffsetSizesSets.emplace_back();
                    regionOffsetSizesSets.back().reserve(regionOffsetSizes.size());
                    bulkOffsets.push_back(bulkOffset);
                    currentListSize = 0;
                } else {
                    currentListSize += size;
                    bulkOffset += size;
                }
                regionOffsetSizesSets.back().push_back({offset, size});
                remaining -= size;
            }
        }
        // issue the transfers and write calls in parallel
        std::vector<tl::managed<tl::thread>> ults;
        std::vector<Result<bool>> ultResults;
        ults.reserve(bulkOffsets.size());
        ultResults.resize(bulkOffsets.size());
        for(size_t i = 0; i < bulkOffsets.size(); ++i) {
            ults.push_back(
                tl::thread::self().get_last_pool().make_thread(
                    [&region, &data, &address, i, this,
                     &regionOffsetSizesSets, bulkOffset=bulkOffsets[i],
                     &ultResults]() mutable {
                    auto& regionOffsetSizes = regionOffsetSizesSets[i];
                    auto& result = ultResults[i];
                    // compute the size of this list of segments
                    auto size = std::accumulate(
                        regionOffsetSizes.begin(), regionOffsetSizes.end(), (size_t)0,
                        [](size_t s, const auto& p) { return s + p.second; });
                    // get a buffer into which to send the data
                    hg_bulk_t bulk = HG_BULK_NULL;
                    margo_bulk_poolset_get(m_poolset, size, &bulk);
                    // access the underlying memory
                    void* bufPtr = nullptr;
                    hg_size_t bufSize = 0;
                    hg_uint32_t actualCount = 0;
                    margo_bulk_access(bulk, 0, size, HG_BULK_READWRITE, 1, &bufPtr, &bufSize, &actualCount);
                    // read the data from the region
                    result = region.read(regionOffsetSizes, bufPtr);
                    // wrap it in a thallium bulk
                    auto localBulk = m_engine.wrap(bulk, true);
                    // issue the transfer
                    localBulk >> data.on(address).select(bulkOffset, size);
                    // release the buffer
                    margo_bulk_poolset_release(m_poolset, bulk);
            }));
        }
        Result<bool> result;
        for(size_t i = 0; i < ults.size(); ++i) {
            ults[i]->join();
            if(!ultResults[i].success())
                result = ultResults[i];
        }
        return result;
    }

    static Result<std::unique_ptr<TransferManager>> create(
            const thallium::engine& engine, const json& config) {
        Result<std::unique_ptr<TransferManager>> result;

        auto num_pools            = config["num_pools"].get<size_t>();
        auto num_buffers_per_pool = config["num_buffers_per_pool"].get<size_t>();
        auto first_buffer_size    = config["first_buffer_size"].get<size_t>();
        auto buffer_size_multiple = config["buffer_size_multiple"].get<size_t>();

        margo_bulk_poolset_t poolset;

        hg_return_t hret = margo_bulk_poolset_create(
            engine.get_margo_instance(),
            num_pools, num_buffers_per_pool,
            first_buffer_size,
            buffer_size_multiple, HG_BULK_READWRITE,
            &poolset);
        if(hret != HG_SUCCESS) {
            result.success() = false;
            result.error() = "Could not create margo bulk poolset. margo_bulk_poolset_create returned ";
            result.error() += HG_Error_to_string(hret);
            return result;
        }

        result.value() = std::make_unique<PipelineTransferManager>(engine, config, poolset);
        return result;
    }

    static Result<bool> validate(const json& config) {
        static json schemaDocument = json::parse(configSchema);
        Result<bool> result;

        valijson::Schema schemaValidator;
        valijson::SchemaParser schemaParser;
        valijson::adapters::NlohmannJsonAdapter schemaAdapter(schemaDocument);
        schemaParser.populateSchema(schemaAdapter, schemaValidator);

        valijson::Validator validator;
        valijson::adapters::NlohmannJsonAdapter jsonAdapter(config);

        valijson::ValidationResults validationResults;
        validator.validate(schemaValidator, jsonAdapter, &validationResults);

        if(validationResults.numErrors() != 0) {
            std::stringstream ss;
            ss << "Error(s) while validating JSON config for warabi provider:\n";
            for(auto& err : validationResults) {
                ss << err.description << "\n";
            }
            result.error() = ss.str();
        }
        return result;
    }
};

WARABI_REGISTER_TRANSFER_MANAGER(pipeline, PipelineTransferManager);

}
