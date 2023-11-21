/*
 * (C) 2023 The University of Chicago
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
    tl::remote_procedure m_create;
    tl::remote_procedure m_write;
    tl::remote_procedure m_write_eager;
    tl::remote_procedure m_persist;
    tl::remote_procedure m_create_write;
    tl::remote_procedure m_create_write_eager;
    tl::remote_procedure m_read;
    tl::remote_procedure m_read_eager;
    tl::remote_procedure m_erase;

    ClientImpl(const tl::engine& engine)
    : m_engine(engine)
    , m_create(m_engine.define("warabi_create"))
    , m_write(m_engine.define("warabi_write"))
    , m_write_eager(m_engine.define("warabi_write_eager"))
    , m_persist(m_engine.define("warabi_persist"))
    , m_create_write(m_engine.define("warabi_create_write"))
    , m_create_write_eager(m_engine.define("warabi_create_write_eager"))
    , m_read(m_engine.define("warabi_read"))
    , m_read_eager(m_engine.define("warabi_read_eager"))
    , m_erase(m_engine.define("warabi_erase"))
    {}

    ClientImpl(margo_instance_id mid)
    : ClientImpl(tl::engine(mid)) {}

    ~ClientImpl() {}
};

}

#endif
