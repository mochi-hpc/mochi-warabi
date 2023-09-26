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
     * @brief Create a region in the target, with the given size.
     *
     * @param[out] region Created region ID.
     * @param[in] size Size of the region to create.
     * @param[in] req Optional request to make the call asynchronous.
     */
    void create(RegionID* region, size_t size,
                AsyncRequest* req = nullptr) const;

    /**
     * @brief Write data in a region.
     *
     * @param[in] region Region to write into.
     * @param[in] regionOffset Offset in the region at which to start writing.
     * @param[in] data Pointer to the data to write.
     * @param[in] size Size to write.
     * @param[in] persist Whether to also persist to data.
     * @param[in] req Optional request to make the call asynchronous.
     */
    void write(const RegionID& region,
               size_t regionOffset,
               const char* data, size_t size,
               bool persist = false,
               AsyncRequest* req = nullptr) const;

    /**
     * @brief Write data in multiple non-contiguous segments of a region.
     *
     * @param[in] region Region to write into.
     * @param[in] regionOffsetSizes Offset/size pairs in the region at which write.
     * @param[in] data Pointer to the data to write.
     * @param[in] persist Whether to also persist to data.
     * @param[in] req Optional request to make the call asynchronous.
     *
     * Note: the local data pointer is assumed to be contiguous. Its size
     * is computed from the sum of the sizes in the regionOffsetSizes vector.
     */
    void write(const RegionID& region,
               const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
               const char* data,
               bool persist = false,
               AsyncRequest* req = nullptr) const;

    /**
     * @brief Write data in a region.
     *
     * @param[in] region Region to write into.
     * @param[in] regionOffset Offset in the region at which to start writing.
     * @param[in] data Bulk handle from which to pull the data.
     * @param[in] address Address of the process in which the data is.
     * @param[in] bulkOffset Offset at which the data starts in the bulk handle.
     * @param[in] size Size to write.
     * @param[in] persist Whether to also persist to data.
     * @param[in] req Optional request to make the call asynchronous.
     *
     * Note: if address is an empty string, it is assumed that the data
     * comes from the calling process.
     */
    void write(const RegionID& region,
               size_t regionOffset,
               thallium::bulk data,
               const std::string& address,
               size_t bulkOffset, size_t size,
               bool persist = false,
               AsyncRequest* req = nullptr) const;

    /**
     * @brief Write data in multiple non-contiguous segments of a region.
     *
     * @param[in] region Region to write into.
     * @param[in] regionOffsetSizes Offset/size pairs in the region at which write.
     * @param[in] data Bulk handle from which to pull the data.
     * @param[in] address Address of the process in which the data is.
     * @param[in] bulkOffset Offset at which the data starts in the bulk handle.
     * @param[in] size Size to write.
     * @param[in] persist Whether to also persist to data.
     * @param[in] req Optional request to make the call asynchronous.
     *
     * Note: if address is an empty string, it is assumed that the data
     * comes from the calling process.
     */
    void write(const RegionID& region,
               const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
               thallium::bulk data,
               const std::string& address,
               size_t bulkOffset,
               bool persist = false,
               AsyncRequest* req = nullptr) const;

    /**
     * @brief Persist a segment of the specified region.
     *
     * @param region Region to persist.
     * @param offset Offset from which to persist.
     * @param size Size to persist.
     * @param req Optional request to make the call asynchronous.
     */
    void persist(const RegionID& region,
                 size_t offset, size_t size,
                 AsyncRequest* req = nullptr) const;

    /**
     * @brief Persist non-contiguous segments of the specified region.
     *
     * @param region Region to persist.
     * @param regionOffsetSizes Offset/size pairs in the region to persist.
     * @param size Size to persist.
     * @param req Optional request to make the call asynchronous.
     */
    void persist(const RegionID& region,
                 const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
                 AsyncRequest* req = nullptr) const;

    /**
     * @brief Combines create and write.
     */
    void createAndWrite(
        RegionID* region,
        const char* data, size_t size,
        bool persist = false,
        AsyncRequest* req = nullptr) const;

    /**
     * @brief Combines create and write.
     */
    void createAndWrite(
        RegionID* region,
        thallium::bulk data,
        const std::string& address,
        size_t bulkOffset, size_t size,
        bool persist = false,
        AsyncRequest* req = nullptr) const;

    /**
     * @brief Read part of a region into the provided local
     * memory buffer.
     *
     * @param region Region to read.
     * @param regionOffset Offset at which to read.
     * @param data Buffer into which to read.
     * @param size Size to read.
     * @param req Optional request to make the call asynchronous.
     */
    void read(const RegionID& region,
              size_t regionOffset,
              char* data, size_t size,
              AsyncRequest* req = nullptr) const;

    /**
     * @brief Read non-contiguous part of a region into the provided non-contiguous,
     * local memory buffer.
     *
     * @param region Region to read.
     * @param regionOffsetSizes Offset/size pairs in the region to read.
     * @param data Buffer into which to read.
     * @param req Optional request to make the call asynchronous.
     *
     * Note: the size to read is computed as the sum of the sizes in the
     * regionOffsetSizes parameter.
     */
    void read(const RegionID& region,
              const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
              char* data,
              AsyncRequest* req = nullptr) const;

    /**
     * @brief Read part of a region into the provided local
     * memory buffer.
     *
     * @param region Region to read.
     * @param regionOffset Offset at which to read.
     * @param data Bulk handle into which to push the data.
     * @param address Address of the process owning the bulk handle.
     * @param bulkOffset Offset at which to push in the provided bulk handle.
     * @param size Size to read.
     * @param req Optional request to make the call asynchronous.
     */
    void read(const RegionID& region,
              size_t regionOffset,
              thallium::bulk data,
              const std::string& address,
              size_t bulkOffset, size_t size,
              AsyncRequest* req = nullptr) const;

    /**
     * @brief Read part of a region into the provided local
     * memory buffer.
     *
     * @param region Region to read.
     * @param regionOffsetSizes Offset/size pairs in the region to read.
     * @param data Bulk handle into which to push the data.
     * @param address Address of the process owning the bulk handle.
     * @param bulkOffset Offset at which to push in the provided bulk handle.
     * @param req Optional request to make the call asynchronous.
     *
     * Note: the size to read is computed as the sum of the sizes in the
     * regionOffsetSizes parameter.
     */
    void read(const RegionID& region,
              const std::vector<std::pair<size_t, size_t>>& regionOffsetSizes,
              thallium::bulk data,
              const std::string& address,
              size_t bulkOffset,
              AsyncRequest* req = nullptr) const;

    /**
     * @brief Erase a region.
     *
     * @param region Region to erase.
     * @param req Optional request to make the call asynchronous.
     */
    void erase(const RegionID& region,
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
