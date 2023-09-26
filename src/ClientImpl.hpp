/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_CLIENT_IMPL_H
#define __WARABI_CLIENT_IMPL_H

#include <thallium.hpp>
#include <thallium/serialization/stl/unordered_set.hpp>
#include <thallium/serialization/stl/unordered_map.hpp>
#include <thallium/serialization/stl/string.hpp>

namespace warabi {

namespace tl = thallium;

class ClientImpl {

    public:

    tl::engine           m_engine;
    tl::remote_procedure m_check_target;
    tl::remote_procedure m_compute_sum;

    ClientImpl(const tl::engine& engine)
    : m_engine(engine)
    , m_check_target(m_engine.define("warabi_check_target"))
    , m_compute_sum(m_engine.define("warabi_compute_sum"))
    {}

    ClientImpl(margo_instance_id mid)
    : ClientImpl(tl::engine(mid)) {}

    ~ClientImpl() {}
};

}

#endif
