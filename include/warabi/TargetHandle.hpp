/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_TARGET_HANDLE_HPP
#define __WARABI_TARGET_HANDLE_HPP

#include <thallium.hpp>
#include <memory>
#include <unordered_set>
#include <nlohmann/json.hpp>
#include <warabi/Client.hpp>
#include <warabi/Exception.hpp>
#include <warabi/AsyncRequest.hpp>

namespace warabi {

namespace tl = thallium;

class Client;
class TargetHandleImpl;

/**
 * @brief A TargetHandle object is a handle for a remote target
 * on a server. It enables invoking the target's functionalities.
 */
class TargetHandle {

    friend class Client;

    public:

    /**
     * @brief Constructor. The resulting TargetHandle handle will be invalid.
     */
    TargetHandle();

    /**
     * @brief Copy-constructor.
     */
    TargetHandle(const TargetHandle&);

    /**
     * @brief Move-constructor.
     */
    TargetHandle(TargetHandle&&);

    /**
     * @brief Copy-assignment operator.
     */
    TargetHandle& operator=(const TargetHandle&);

    /**
     * @brief Move-assignment operator.
     */
    TargetHandle& operator=(TargetHandle&&);

    /**
     * @brief Destructor.
     */
    ~TargetHandle();

    /**
     * @brief Returns the client this target has been opened with.
     */
    Client client() const;


    /**
     * @brief Checks if the TargetHandle instance is valid.
     */
    operator bool() const;

    /**
     * @brief Requests the target target to compute the sum of two numbers.
     * If result is null, it will be ignored. If req is not null, this call
     * will be non-blocking and the caller is responsible for waiting on
     * the request.
     *
     * @param[in] x first integer
     * @param[in] y second integer
     * @param[out] result result
     * @param[out] req request for a non-blocking operation
     */
    void computeSum(int32_t x, int32_t y,
                    int32_t* result = nullptr,
                    AsyncRequest* req = nullptr) const;

    private:

    /**
     * @brief Constructor is private. Use a Client object
     * to create a TargetHandle instance.
     *
     * @param impl Pointer to implementation.
     */
    TargetHandle(const std::shared_ptr<TargetHandleImpl>& impl);

    std::shared_ptr<TargetHandleImpl> self;
};

}

#endif
