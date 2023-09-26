/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __ALPHA_RESULT_HPP
#define __ALPHA_RESULT_HPP

#include <alpha/Exception.hpp>
#include <string>

namespace alpha {

/**
 * @brief The Result object is a generic object
 * used to hold and send back the result of an RPC.
 * It contains three fields:
 * - success must be set to true if the request succeeded, false otherwise
 * - error must be set to an error string if an error occured
 * - value must be set to the result of the request if it succeeded
 *
 * This class is specialized for two types: bool and std::string.
 * If bool is used, both the value and the success fields will be
 * managed by the same underlying variable. If std::string is used,
 * both the value and the error fields will be managed by the same
 * underlying variable.
 *
 * @tparam T Type of the result.
 */
template<typename T>
class Result {

    public:

    Result() = default;
    Result(Result&&) = default;
    Result(const Result&) = default;
    Result& operator=(Result&&) = default;
    Result& operator=(const Result&) = default;

    /**
     * @brief Whether the request succeeded.
     */
    bool& success() {
        return m_success;
    }

    /**
     * @brief Whether the request succeeded.
     */
    const bool& success() const {
        return m_success;
    }

    /**
     * @brief Error string if the request failed.
     */
    std::string& error() {
        return m_error;
    }

    /**
     * @brief Error string if the request failed.
     */
    const std::string& error() const {
        return m_error;
    }

    /**
     * @brief Value if the request succeeded.
     */
    T& value() {
        return m_value;
    }

    /**
     * @brief Value if the request succeeded.
     */
    const T& value() const {
        return m_value;
    }

    /**
     * @brief Value if the request succeeded,
     * throws otherwise.
     */
    const T& valueOrThrow() const & {
        check();
        return m_value;
    }

    /**
     * @brief Value if the request succeeded,
     * throws otherwise.
     */
    T&& valueOrThrow() && {
        check();
        return std::move(m_value);
    }

    /**
     * @brief Execute a function on the value
     * if the value is present, otherwise throws
     * an Exception.
     */
    template<typename F>
    decltype(auto) andThen(F&& f) const & {
        return std::forward<F>(f)(valueOrThrow());
    }

    /**
     * @brief Execute a function on the value
     * if the value is present, otherwise throws
     * an Exception.
     */
    template<typename F>
    decltype(auto) andThen(F&& f) && {
        return std::forward<F>(f)(valueOrThrow());
    }

    /**
     * @brief Throw an Exception if the Result
     * contains an error.
     */
    void check() const {
        if(!m_success)
            throw Exception(m_error);
    }

    /**
     * @brief Serialization function for Thallium.
     *
     * @tparam Archive Archive type.
     * @param a Archive instance.
     */
    template<typename Archive>
    void serialize(Archive& a) {
        a & m_success;
        if(m_success) {
            a & m_value;
        } else {
            a & m_error;
        }
    }

    private:

    bool        m_success = true;
    std::string m_error   = "";
    T           m_value;
};

template<>
class Result<std::string> {

    public:

    Result() = default;
    Result(Result&&) = default;
    Result(const Result&) = default;
    Result& operator=(Result&&) = default;
    Result& operator=(const Result&) = default;

    bool& success() {
        return m_success;
    }

    const bool& success() const {
        return m_success;
    }

    std::string& error() {
        return m_content;
    }

    const std::string& error() const {
        return m_content;
    }

    std::string& value() {
        return m_content;
    }

    const std::string& value() const {
        return m_content;
    }

    const std::string& valueOrThrow() const & {
        check();
        return m_content;
    }

    std::string&& valueOrThrow() && {
        check();
        return std::move(m_content);
    }

    template<typename F>
    decltype(auto) andThen(F&& f) const & {
        return std::forward<F>(f)(valueOrThrow());
    }

    template<typename F>
    decltype(auto) andThen(F&& f) && {
        return std::forward<F>(f)(valueOrThrow());
    }

    void check() const {
        if(!m_success)
            throw Exception(m_content);
    }

    template<typename Archive>
    void serialize(Archive& a) {
        a & m_success;
        a & m_content;
    }

    private:

    bool        m_success = true;
    std::string m_content = "";
};

template<>
class Result<bool> {

    public:

    Result() = default;
    Result(Result&&) = default;
    Result(const Result&) = default;
    Result& operator=(Result&&) = default;
    Result& operator=(const Result&) = default;

    bool& success() {
        return m_success;
    }

    const bool& success() const {
        return m_success;
    }

    std::string& error() {
        return m_error;
    }

    const std::string& error() const {
        return m_error;
    }

    bool& value() {
        return m_success;
    }

    const bool& value() const {
        return m_success;
    }

    bool valueOrThrow() const {
        check();
        return true;
    }

    template<typename F>
    decltype(auto) andThen(F&& f) const {
        check();
        return std::forward<F>(f)();
    }

    void check() const {
        if(!m_success)
            throw Exception(m_error);
    }

    template<typename Archive>
    void serialize(Archive& a) {
        a & m_success;
        if(!m_success)
            a & m_error;
    }

    private:

    bool        m_success = true;
    std::string m_error   = "";
};

}

#endif
