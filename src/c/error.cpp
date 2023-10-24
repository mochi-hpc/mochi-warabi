/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include "error.hpp"

extern "C" const char* warabi_err_message(warabi_err_t err) {
    return err->what();
}

extern "C" void warabi_err_free(warabi_err_t err) {
    delete static_cast<warabi::Exception*>(err);
}
