/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_MIGRATION_OPTIONS_HPP
#define __WARABI_MIGRATION_OPTIONS_HPP

#include <string>
#include <thallium/serialization/stl/string.hpp>

namespace warabi {

struct MigrationOptions {

    std::string newRoot;      // new path to the target
    size_t      transferSize; // block size for transfers
    std::string extraConfig;  // extra configuration
    bool        removeSource; // remove target from source

    template<typename Archive>
    void serialize(Archive& ar) {
        ar(newRoot);
        ar(transferSize);
        ar(extraConfig);
        ar(removeSource);
    }

};

}

#endif
