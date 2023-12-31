/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_TARGET_HANDLE_IMPL_H
#define __WARABI_TARGET_HANDLE_IMPL_H

#include <thallium.hpp>
#include "ClientImpl.hpp"

namespace tl = thallium;

namespace warabi {

class TargetHandleImpl {

    public:

    std::shared_ptr<ClientImpl> m_client;
    tl::provider_handle         m_ph;

    size_t m_eager_write_threshold = 2048;
    size_t m_eager_read_threshold = 2048;

    TargetHandleImpl() = default;

    TargetHandleImpl(const std::shared_ptr<ClientImpl>& client,
                       tl::provider_handle&& ph)
    : m_client(client)
    , m_ph(std::move(ph)) {}
};

}

#endif
