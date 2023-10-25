/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/error.h"
#include "warabi/Exception.hpp"

struct warabi_err : public warabi::Exception {
    template<typename ... Args>
    warabi_err(Args&&... args)
    : warabi::Exception(std::forward<Args>(args)...) {}
};

#define HANDLE_WARABI_ERROR                                                \
    catch(const std::exception& ex) {                                      \
        return static_cast<warabi_err*>(new warabi::Exception{ex.what()}); \
    }                                                                      \
    return nullptr
