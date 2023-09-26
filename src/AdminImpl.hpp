/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_ADMIN_IMPL_H
#define __WARABI_ADMIN_IMPL_H

#include <thallium.hpp>

namespace warabi {

namespace tl = thallium;

class AdminImpl {

    public:

    tl::engine           m_engine;
    tl::remote_procedure m_create_target;
    tl::remote_procedure m_open_target;
    tl::remote_procedure m_close_target;
    tl::remote_procedure m_destroy_target;

    AdminImpl(const tl::engine& engine)
    : m_engine(engine)
    , m_create_target(m_engine.define("warabi_create_target"))
    , m_open_target(m_engine.define("warabi_open_target"))
    , m_close_target(m_engine.define("warabi_close_target"))
    , m_destroy_target(m_engine.define("warabi_destroy_target"))
    {}

    AdminImpl(margo_instance_id mid)
    : AdminImpl(tl::engine(mid)) {
    }

    ~AdminImpl() {}
};

}

#endif
