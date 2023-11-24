/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_REGION_ID_HPP
#define __WARABI_REGION_ID_HPP

#include <stdint.h>
#include <array>

namespace warabi {

/**
 * @brief How a RegionID is described depends on the backend,
 * so from a client's point of view it's abstracted 16 byte
 * opaque structure.
 */
typedef std::array<uint8_t, 16> RegionID;

}

#endif
