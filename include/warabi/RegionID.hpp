/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_REGION_ID_HPP
#define __WARABI_REGION_ID_HPP

#include <cstring>
#include <stdint.h>

namespace warabi {

/**
 * @brief How a RegionID is described depends on the backend,
 * so from a client's point of view it's abstracted as a contiguous
 * sequence of bytes (up to 255, to keep sizes small). The first byte
 * contains the size of the content.
 */
struct RegionID {

    uint8_t* content = nullptr;

    RegionID() = default;

    RegionID(const void* data, uint8_t size)
    : content{new uint8_t[size + 1]} {
        content[0] = size + 1;
        std::memcpy(content + 1, data, size);
    }

    ~RegionID() {
        delete[] content;
    }

    RegionID(RegionID&& other)
    : content{other.content} {
        other.content = nullptr;
    }

    RegionID(const RegionID& other) {
        if(other.content) {
            content = new uint8_t[other.content[0]];
            std::memcpy(content, other.content, other.content[0]);
        } else {
            content = nullptr;
        }
    }

    RegionID& operator=(RegionID&& other) {
        if(&other == this) return *this;
        if(content) delete[] content;
        content = other.content;
        other.content = nullptr;
        return *this;
    }

    RegionID& operator=(const RegionID& other) {
        if(&other == this) return *this;
        if(content) delete[] content;
        if(other.content) {
            content = new uint8_t[other.content[0]];
            std::memcpy(content, other.content, other.content[0]);
        } else {
            content = nullptr;
        }
        return *this;
    }

    template<typename Archive>
    void save(Archive& ar) const {
        if(content == nullptr) {
            uint8_t size = 1;
            ar.write(&size, 1);
        } else {
            ar.write(content, content[0]);
        }
    }

    template<typename Archive>
    void load(Archive& ar) {
        delete[] content;
        uint8_t size;
        ar.read(&size, 1);
        if(size <= 1) {
            content = nullptr;
        } else {
            content = new uint8_t[size];
            content[0] = size;
            ar.read(content + 1, size - 1);
        }
    }

};

}

#endif
