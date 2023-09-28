/*
 * (C) 2023 The University of Chicago
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
    tl::remote_procedure m_add_target;
    tl::remote_procedure m_remove_target;
    tl::remote_procedure m_destroy_target;

    AdminImpl(const tl::engine& engine)
    : m_engine(engine)
    , m_add_target(m_engine.define("warabi_add_target"))
    , m_remove_target(m_engine.define("warabi_remove_target"))
    , m_destroy_target(m_engine.define("warabi_destroy_target"))
    {}

    AdminImpl(margo_instance_id mid)
    : AdminImpl(tl::engine(mid)) {
    }

    ~AdminImpl() {}
};

}

#endif
