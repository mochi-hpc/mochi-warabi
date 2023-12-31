/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_EXCEPTION_HPP
#define __WARABI_EXCEPTION_HPP

#include <exception>
#include <string>

namespace warabi {

class Exception : public std::exception {

    std::string m_error;

    public:

    template<typename ... Args>
    Exception(Args&&... args)
    : m_error(std::forward<Args>(args)...) {}

    virtual const char* what() const noexcept override {
        return m_error.c_str();
    }
};

}

#endif
