/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_TARGET_H
#define __WARABI_TARGET_H

#include <uuid.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uuid_t warabi_target_id;

/**
 * @brief Converts a Warabi target ID into a 37-byte null-terminated string.
 *
 * @param[in] id Target ID.
 * @param[out] str[37] Buffer into which to write the result.
 */
static inline void warabi_target_id_to_string(const warabi_target_id id, char str[37])
{
    uuid_unparse(id, str);
}

/**
 * @brief Converts a 37-byte null-terminated string into a target ID.
 *
 * @param[out] id Resulting target ID.
 * @param[in] str String representation.
 */
static inline void warabi_target_id_from_string(warabi_target_id id, const char* str)
{
    uuid_parse(str, id);
}

#ifdef __cplusplus
}
#endif

#endif
