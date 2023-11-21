/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_BUFFER_WRAPPER_H
#define __WARABI_BUFFER_WRAPPER_H

namespace warabi {

class BufferWrapper {

    BufferWrapper(char* data, size_t size)
    : m_data{data}
    , m_size{size}
    , m_owned{false} {}

    public:

    BufferWrapper()
    : BufferWrapper{nullptr, 0} {}

    BufferWrapper(const BufferWrapper&) = delete;

    BufferWrapper(BufferWrapper&& other)
    : m_data(other.m_data)
    , m_size(other.m_size)
    , m_owned(other.m_owned) {
        other.m_owned = false;
    }

    static auto Ref(char* data, size_t size) {
        return BufferWrapper{data, size};
    }

    static auto Ref(const char* data, size_t size) {
        return BufferWrapper{const_cast<char*>(data), size};
    }

    void allocate(size_t size) {
        if(m_owned) delete[] m_data;
        m_size = size;
        if(m_size) {
            m_owned = true;
            m_data = new char[m_size];
        }
    }

    char* data() {
        return m_data;
    }

    const char* data() const {
        return m_data;
    }

    size_t size() const {
        return m_size;
    }

    ~BufferWrapper() {
        if(m_owned) delete[] m_data;
    }

    template<typename Archive>
    void save(Archive& ar) const {
        ar & m_size;
        ar.write(m_data, m_size);
    }

    template<typename Archive>
    void load(Archive& ar) {
        if(m_owned) delete[] m_data;
        ar & m_size;
        if(m_size) {
            m_owned = true;
            m_data = new char[m_size];
            ar.read(m_data, m_size);
        }
    }

    private:

    char*  m_data;
    size_t m_size;
    bool   m_owned;

};

}

#endif
