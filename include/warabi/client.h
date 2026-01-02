/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_CLIENT_H
#define __WARABI_CLIENT_H

#include <margo.h>
#include <warabi/error.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct warabi_client* warabi_client_t;
#define WARABI_CLIENT_NULL ((warabi_client_t)0)

typedef struct warabi_target_handle* warabi_target_handle_t;
#define WARABI_TARGET_HANDLE_NULL ((warabi_target_handle_t)0)

typedef struct warabi_async_request* warabi_async_request_t;
#define WARABI_ASYNC_REQUEST_NULL ((warabi_async_request_t)0)
#define WARABI_ASYNC_REQUEST_IGNORE ((warabi_async_request_t*)0)

typedef struct warabi_region {
    uint8_t opaque[16];
} warabi_region_t;

/**
 * @brief Create a client.
 *
 * @param[in] mid margo instance id.
 * @param[out] client Resulting client.
 */
warabi_err_t warabi_client_create(margo_instance_id mid, warabi_client_t* client);

/**
 * @brief Free the client handle.
 */
warabi_err_t warabi_client_free(warabi_client_t client);

/**
 * @brief Creates a handle to a remote target.
 *
 * @param[in] client Client.
 * @param[in] address Address of the provider holding the target.
 * @param[in] provider_id Provider id.
 * @param[out] th Target handle.
 */
warabi_err_t warabi_client_make_target_handle(
        warabi_client_t client,
        const char* address,
        uint16_t provider_id,
        warabi_target_handle_t* th);

/**
 * @brief Free the target handle.
 */
warabi_err_t warabi_target_handle_free(warabi_target_handle_t th);

/**
 * @brief Returns the internal configuration of the client
 * as a JSON-formatted string, or NULL if the client is invalid.
 *
 * It is the caller's responsibility to free the returned string.
 *
 * @param client Client.
 */
char* warabi_client_get_config(warabi_client_t client);

/**
 * @brief Create a region.
 *
 * @param[in] th Target handle.
 * @param[in] size Size of the region.
 * @param[out] region Resulting region ID.
 * @param[out] req Optional asynchronous request.
 *
 * @return warabi_err_t handle.
 */
warabi_err_t warabi_create(
        warabi_target_handle_t th,
        size_t size,
        warabi_region_t* region,
        warabi_async_request_t* req);

/**
 * @brief Write data into a region.
 *
 * @param[in] th Target handle.
 * @param[in] region Region to write to.
 * @param[in] regionOffset Offset to write from in the region.
 * @param[in] data Data to write.
 * @param[in] size Size of the data.
 * @param[in] persist Whether to persist the data.
 * @param[out] req Optional asynchronous request.
 *
 * @return warabi_err_t handle.
 */
warabi_err_t warabi_write(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t regionOffset,
        const char* data, size_t size,
        bool persist,
        warabi_async_request_t* req);

/**
 * @brief Same as warabi_write but accesses non-contiguous
 * segments inside a region.
 */
warabi_err_t warabi_write_multi(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t count,
        const size_t* regionOffsets,
        const size_t* regionSizes,
        const char* data,
        bool persist,
        warabi_async_request_t* req);

/**
 * @brief Same as warabi_write but the data is coming from
 * an hg_bulk_t handle at a specified bulkOffset.
 *
 * Note: this function can be used to write from a non-contiguous
 * local memory since hg_bulk_t can represent non-contiguous memory.
 */
warabi_err_t warabi_write_bulk(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t regionOffset,
        hg_bulk_t bulk, const char* address,
        size_t bulkOffset, size_t size,
        bool persist,
        warabi_async_request_t* req);

/**
 * @brief Same as warabi_write_multi but the data is coming from
 * an hg_bulk_t handle at a specified bulkOffset.
 *
 * Note: this function can be used to write from a non-contiguous
 * local memory since hg_bulk_t can represent non-contiguous memory.
 */
warabi_err_t warabi_write_multi_bulk(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t count,
        const size_t* regionOffsets,
        const size_t* regionSizes,
        hg_bulk_t bulk, const char* address,
        size_t bulkOffset, bool persist,
        warabi_async_request_t* req);

/**
 * @brief Persist the content of a region from a given offset.
 *
 * @param[in] th Target handle.
 * @param[in] region Region from which to persist data.
 * @param[in] regionOffset Offset from which to persist data.
 * @param[in] size Size to persist.
 * @param[out] req Optional asynchronous request.
 *
 * @return warabi_err_t handle.
 */
warabi_err_t warabi_persist(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t regionOffset,
        size_t size,
        warabi_async_request_t* req);

/**
 * @brief Same as warabi_persist but allows persisting multiple
 * non-contiguous segments of the underlying region.
 */
warabi_err_t warabi_persist_multi(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t count,
        const size_t* regionOffsets,
        const size_t* regionSizes,
        warabi_async_request_t* req);

/**
 * @brief Combined create + write operation.
 */
warabi_err_t warabi_create_write(
        warabi_target_handle_t th,
        const char* data, size_t size,
        bool persist,
        warabi_region_t* region,
        warabi_async_request_t* req);

/**
 * @brief Combined create + write operation with data
 * coming from a bulk handle.
 */
warabi_err_t warabi_create_write_bulk(
        warabi_target_handle_t th,
        hg_bulk_t bulk, const char* address,
        size_t bulkOffset, size_t size,
        bool persist,
        warabi_region_t* region,
        warabi_async_request_t* req);

/**
 * @brief Read a region from a given offset.
 *
 * @param[in] th Target handle.
 * @param[in] region Region to read from.
 * @param[in] regionOffset Offset from which to read in the region.
 * @param[out] data Buffer in which to place the data read.
 * @param[in] size Size to read.
 * @param[out] req Optional asynchronous request.
 *
 * @return warabi_err_t handle.
 */
warabi_err_t warabi_read(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t regionOffset,
        char* data, size_t size,
        warabi_async_request_t* req);

/**
 * @brief Same as warabi_read but allows reading non-contiguous
 * segments from a region.
 */
warabi_err_t warabi_read_multi(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t count,
        const size_t* regionOffsets,
        const size_t* regionSizes,
        char* data,
        warabi_async_request_t* req);

/**
 * @brief Same as warabi_read but will push the data to a provided
 * hg_bulk_t handle.
 */
warabi_err_t warabi_read_bulk(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t regionOffset,
        const char* address, hg_bulk_t bulk,
        size_t bulkOffset, size_t size,
        warabi_async_request_t* req);

/**
 * @brief Same as warabi_read_multi but will push the data to a provided
 * hg_bulk_t handle.
 */
warabi_err_t warabi_read_multi_bulk(
        warabi_target_handle_t th,
        warabi_region_t region,
        size_t count,
        const size_t* regionOffsets,
        const size_t* regionSizes,
        const char* address, hg_bulk_t bulk,
        size_t bulkOffset,
        warabi_async_request_t* req);

/**
 * @brief Erase a region.
 *
 * @param[in] th Target handle.
 * @param[in] region Region to erase.
 * @param[out] req Optional asynchronous request.
 *
 * @return warabi_err_t handle.
 */
warabi_err_t warabi_erase(
        warabi_target_handle_t th,
        warabi_region_t region,
        warabi_async_request_t* req);

/**
 * @brief Wait on an asynchronous request. This will also free the
 * underlying handle request handle.
 *
 * @param req Request to wait on.
 *
 * @return warabi_err_t handle.
 */
warabi_err_t warabi_wait(warabi_async_request_t req);

/**
 * @brief Test without blocking whether the asynchronous request has
 * completed. Note that even if the test returns true, the caller still
 * needs to call warabi_wait.
 *
 * @param[in] req Request to test.
 * @param[out] flag Whether the request completed.
 *
 * @return warabi_err_t handle.
 */
warabi_err_t warabi_test(warabi_async_request_t req, bool* flag);

/**
 * @brief Set the thresdhold for using RPC messages instead of RDMA
 * for write operations on this target handle.
 *
 * @param th Target handle.
 * @param size Threshold.
 *
 * @return warabi_err_t handle.
 */
warabi_err_t warabi_set_eager_write_threshold(
        warabi_target_handle_t th,
        size_t size);

/**
 * @brief Set the thresdhold for using RPC messages instead of RDMA
 * for read operations on this target handle.
 *
 * @param th Target handle.
 * @param size Threshold.
 *
 * @return warabi_err_t handle.
 */
warabi_err_t warabi_set_eager_read_threshold(
        warabi_target_handle_t th,
        size_t size);

#ifdef __cplusplus
}
#endif

#endif
