/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_ERROR_H
#define __WARABI_ERROR_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct warabi_err* warabi_err_t;
#define WARABI_SUCCESS ((warabi_err_t)0)

/**
 * @brief Return the error message associated with the error handle.
 */
const char* warabi_err_message(warabi_err_t err);

/**
 * @brief Free the error handle.
 */
void warabi_err_free(warabi_err_t err);

#ifdef __cplusplus
}
#endif

#endif
