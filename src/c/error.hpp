/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/error.h"
#include "warabi/Exception.hpp"

struct warabi_err : public warabi::Exception {};

#define HANDLE_WARABI_ERROR                                         \
    catch(const warabi::Exception& ex) {                            \
        return static_cast<warabi_err*>(new warabi::Exception{ex}); \
    }                                                               \
    return nullptr
