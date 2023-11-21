/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "error.hpp"
#include "warabi/client.h"
#include <warabi/Client.hpp>
#include <warabi/TargetHandle.hpp>
#include <warabi/AsyncRequest.hpp>

struct warabi_client : public warabi::Client {
    template<typename... Args>
    warabi_client(Args&&... args)
    :  warabi::Client(std::forward<Args>(args)...) {}
};

struct warabi_target_handle : public warabi::TargetHandle {
    template<typename... Args>
    warabi_target_handle(Args&&... args)
    :  warabi::TargetHandle(std::forward<Args>(args)...) {}
};

struct warabi_async_request : public warabi::AsyncRequest {
    template<typename... Args>
    warabi_async_request(Args&&... args)
    :  warabi::AsyncRequest(std::forward<Args>(args)...) {}
};

extern "C" warabi_err_t warabi_client_create(
        margo_instance_id mid,
        warabi_client_t* client) {
    try {
        *client = new warabi_client{mid};
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_client_free(
        warabi_client_t client) {
    delete client;
    return nullptr;
}

extern "C" warabi_err_t warabi_client_make_target_handle(
        warabi_client_t client,
        const char* address,
        uint16_t provider_id,
        warabi_target_handle_t* th) {
    try {
        auto t = client->makeTargetHandle(address, provider_id);
        *th = new warabi_target_handle{std::move(t)};
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_target_handle_free(warabi_target_handle_t th) {
    delete th;
    return nullptr;
}

extern "C" char* warabi_client_get_config(warabi_client_t client) {
    auto config = client->getConfig();
    return strdup(config.c_str());
}

extern "C" warabi_err_t warabi_region_free(warabi_region_t region) {
    delete region.opaque;
    return nullptr;
}

extern "C" warabi_err_t warabi_region_serialize(
        warabi_region_t region,
        void (*serialization_fn)(void*, const void*, size_t),
        void* uargs) {
    if(!region.opaque) {
        return new warabi_err{"Invalid Region ID"};
    }
    serialization_fn(uargs, region.opaque + 1, region.opaque[0] - 1);
    return nullptr;
}

extern "C" warabi_err_t warabi_region_deserialize(
        warabi_region_t* region,
        const void* data, size_t size) {
    new((void*)region) warabi::RegionID{data, (uint8_t)size};
    return nullptr;
}

extern "C" warabi_err_t warabi_create(
        warabi_target_handle_t th,
        size_t size,
        warabi_region_t* region,
        warabi_async_request_t* req) {
    try {
        auto region_id = reinterpret_cast<warabi::RegionID*>(region);
        region_id->content = nullptr;
        if(req) {
            warabi::AsyncRequest async_req;
            th->create(region_id, size, &async_req);
            *req = new warabi_async_request{std::move(async_req)};
        } else {
            th->create(region_id, size);
        }
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_write(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t regionOffset,
        const char* data, size_t size,
        bool persist,
        warabi_async_request_t* req) {
    return warabi_write_multi(th, region, 1, &regionOffset, &size, data, persist, req);
}

extern "C" warabi_err_t warabi_write_multi(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t count,
        const size_t* regionOffsets,
        const size_t* regionSizes,
        const char* data,
        bool persist,
        warabi_async_request_t* req) {
    try {
        auto region_id = reinterpret_cast<warabi::RegionID*>(&region);
        std::vector<std::pair<size_t, size_t>> segments(count);
        for(size_t i=0; i < count; ++i) {
            segments[i].first = regionOffsets[i];
            segments[i].second = regionSizes[i];
        }
        if(req) {
            warabi::AsyncRequest async_req;
            th->write(*region_id, segments, data, persist, &async_req);
            *req = new warabi_async_request{std::move(async_req)};
        } else {
            th->write(*region_id, segments, data, persist);
        }

    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_write_bulk(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t regionOffset,
        hg_bulk_t bulk, const char* address,
        size_t bulkOffset, size_t size,
        bool persist,
        warabi_async_request_t* req) {
    return warabi_write_multi_bulk(th, region, 1, &regionOffset, &size,
                                   bulk, address, bulkOffset, persist, req);
}

extern "C" warabi_err_t warabi_write_multi_bulk(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t count,
        const size_t* regionOffsets,
        const size_t* regionSizes,
        hg_bulk_t bulk, const char* address,
        size_t bulkOffset, bool persist,
        warabi_async_request_t* req) {
    try {
        auto region_id = reinterpret_cast<warabi::RegionID*>(&region);
        auto engine = th->client().engine();
        std::vector<std::pair<size_t, size_t>> segments(count);
        for(size_t i=0; i < count; ++i) {
            segments[i].first = regionOffsets[i];
            segments[i].second = regionSizes[i];
        }
        if(req) {
            warabi::AsyncRequest async_req;
            th->write(*region_id, segments, engine.wrap(bulk, false),
                      address, bulkOffset, persist, &async_req);
            *req = new warabi_async_request{std::move(async_req)};
        } else {
            th->write(*region_id, segments, engine.wrap(bulk, false),
                      address, bulkOffset, persist);
        }

    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_persist(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t regionOffset,
        size_t size,
        warabi_async_request_t* req) {
    return warabi_persist_multi(th, region, 1, &regionOffset, &size, req);
}

extern "C" warabi_err_t warabi_persist_multi(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t count,
        const size_t* regionOffsets,
        const size_t* regionSizes,
        warabi_async_request_t* req) {
    try {
        auto region_id = reinterpret_cast<warabi::RegionID*>(&region);
        std::vector<std::pair<size_t, size_t>> segments(count);
        for(size_t i=0; i < count; ++i) {
            segments[i].first = regionOffsets[i];
            segments[i].second = regionSizes[i];
        }
        if(req) {
            warabi::AsyncRequest async_req;
            th->persist(*region_id, segments, &async_req);
            *req = new warabi_async_request{std::move(async_req)};
        } else {
            th->persist(*region_id, segments);
        }

    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_create_write(
        warabi_target_handle_t th,
        const char* data, size_t size,
        bool persist,
        warabi_region_t* region,
        warabi_async_request_t* req) {
    try {
        auto region_id = reinterpret_cast<warabi::RegionID*>(region);
        region_id->content = nullptr;
        if(req) {
            warabi::AsyncRequest async_req;
            th->createAndWrite(region_id, data, size, persist, &async_req);
            *req = new warabi_async_request{std::move(async_req)};
        } else {
            th->createAndWrite(region_id, data, size, persist);
        }
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_create_write_bulk(
        warabi_target_handle_t th,
        hg_bulk_t bulk, const char* address,
        size_t bulkOffset, size_t size,
        bool persist,
        warabi_region_t* region,
        warabi_async_request_t* req) {
    try {
        auto region_id = reinterpret_cast<warabi::RegionID*>(region);
        region_id->content = nullptr;
        auto engine = th->client().engine();
        if(req) {
            warabi::AsyncRequest async_req;
            th->createAndWrite(region_id, engine.wrap(bulk, false), address, bulkOffset, size, persist, &async_req);
            *req = new warabi_async_request{std::move(async_req)};
        } else {
            th->createAndWrite(region_id, engine.wrap(bulk, false), address, bulkOffset, size, persist);
        }
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_read(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t regionOffset,
        char* data, size_t size,
        warabi_async_request_t* req) {
    return warabi_read_multi(th, region, 1, &regionOffset, &size, data, req);
}

extern "C" warabi_err_t warabi_read_multi(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t count,
        const size_t* regionOffsets,
        const size_t* regionSizes,
        char* data,
        warabi_async_request_t* req) {
    try {
        auto region_id = reinterpret_cast<warabi::RegionID*>(&region);
        std::vector<std::pair<size_t, size_t>> segments(count);
        for(size_t i=0; i < count; ++i) {
            segments[i].first = regionOffsets[i];
            segments[i].second = regionSizes[i];
        }
        if(req) {
            warabi::AsyncRequest async_req;
            th->read(*region_id, segments, data, &async_req);
            *req = new warabi_async_request{std::move(async_req)};
        } else {
            th->read(*region_id, segments, data);
        }

    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_read_bulk(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t regionOffset,
        const char* address, hg_bulk_t bulk,
        size_t bulkOffset, size_t size,
        warabi_async_request_t* req) {
    try {
        auto region_id = reinterpret_cast<warabi::RegionID*>(&region);
        auto engine = th->client().engine();
        if(req) {
            warabi::AsyncRequest async_req;
            th->read(*region_id, regionOffset, engine.wrap(bulk, false),
                     address, bulkOffset, size, &async_req);
            *req = new warabi_async_request{std::move(async_req)};
        } else {
            th->read(*region_id, regionOffset, engine.wrap(bulk, false),
                     address, bulkOffset, size);
        }

    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_read_multi_bulk(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t count,
        const size_t* regionOffsets,
        const size_t* regionSizes,
        const char* address, hg_bulk_t bulk,
        size_t bulkOffset,
        warabi_async_request_t* req) {
    try {
        auto region_id = reinterpret_cast<warabi::RegionID*>(&region);
        auto engine = th->client().engine();
        std::vector<std::pair<size_t, size_t>> segments(count);
        for(size_t i=0; i < count; ++i) {
            segments[i].first = regionOffsets[i];
            segments[i].second = regionSizes[i];
        }
        if(req) {
            warabi::AsyncRequest async_req;
            th->read(*region_id, segments, engine.wrap(bulk, false),
                     address, bulkOffset, &async_req);
            *req = new warabi_async_request{std::move(async_req)};
        } else {
            th->read(*region_id, segments, engine.wrap(bulk, false),
                     address, bulkOffset);
        }

    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_erase(
        warabi_target_handle_t th,
        warabi_region_t region,
        warabi_async_request_t* req) {
    try {
        auto region_id = reinterpret_cast<warabi::RegionID*>(&region);
        auto engine = th->client().engine();
        if(req) {
            warabi::AsyncRequest async_req;
            th->erase(*region_id, &async_req);
            *req = new warabi_async_request{std::move(async_req)};
        } else {
            th->erase(*region_id);
        }

    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_wait(warabi_async_request_t req) {
    warabi_err_t err = nullptr;
    try {
        req->wait();
    } catch(const std::exception& ex) {
        err = static_cast<warabi_err*>(new warabi::Exception{ex.what()});
    }
    delete req;
    return err;
}

extern "C" warabi_err_t warabi_test(warabi_async_request_t req, bool* flag) {
    try {
        *flag = req->completed();
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_set_eager_write_threshold(
        warabi_target_handle_t th,
        size_t size) {
    try {
        th->setEagerWriteThreshold(size);
    } HANDLE_WARABI_ERROR;
}

extern "C" warabi_err_t warabi_set_eager_read_threshold(
        warabi_target_handle_t th,
        size_t size) {
    try {
        th->setEagerReadThreshold(size);
    } HANDLE_WARABI_ERROR;
}
