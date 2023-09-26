/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_BACKEND_HPP
#define __WARABI_BACKEND_HPP

#include <warabi/Result.hpp>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <nlohmann/json.hpp>
#include <thallium.hpp>

/**
 * @brief Helper class to register backend types into the backend factory.
 */
template<typename BackendType>
class __WarabiBackendRegistration;

namespace warabi {

/**
 * @brief Interface for target backends. To build a new backend,
 * implement a class MyBackend that inherits from Backend, and put
 * WARABI_REGISTER_BACKEND(mybackend, MyBackend); in a cpp file
 * that includes your backend class' header file.
 *
 * Your backend class should also have two static functions to
 * respectively create and open a target:
 *
 * std::unique_ptr<Backend> create(const json& config)
 * std::unique_ptr<Backend> attach(const json& config)
 */
class Backend {

    template<typename BackendType>
    friend class ::__WarabiBackendRegistration;

    std::string m_name;

    public:

    /**
     * @brief Constructor.
     */
    Backend() = default;

    /**
     * @brief Move-constructor.
     */
    Backend(Backend&&) = default;

    /**
     * @brief Copy-constructor.
     */
    Backend(const Backend&) = default;

    /**
     * @brief Move-assignment operator.
     */
    Backend& operator=(Backend&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    Backend& operator=(const Backend&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~Backend() = default;

    /**
     * @brief Return the name of backend.
     */
    const std::string& name() const {
        return m_name;
    }

    /**
     * @brief Returns a JSON-formatted configuration string.
     */
    virtual std::string getConfig() const = 0;

    /**
     * @brief Prints Hello World.
     */
    virtual void sayHello() = 0;

    /**
     * @brief Compute the sum of two integers.
     *
     * @param x first integer
     * @param y second integer
     *
     * @return a Result containing the result.
     */
    virtual Result<int32_t> computeSum(int32_t x, int32_t y) = 0;

    /**
     * @brief Destroys the underlying target.
     *
     * @return a Result<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    virtual Result<bool> destroy() = 0;

};

/**
 * @brief The TargetFactory contains functions to create
 * or open targets.
 */
class TargetFactory {

    template<typename BackendType>
    friend class ::__WarabiBackendRegistration;

    using json = nlohmann::json;

    public:

    TargetFactory() = delete;

    /**
     * @brief Creates a target and returns a unique_ptr to the created instance.
     *
     * @param backend_name Name of the backend to use.
     * @param engine Thallium engine.
     * @param config Configuration object to pass to the backend's create function.
     *
     * @return a unique_ptr to the created Target.
     */
    static std::unique_ptr<Backend> createTarget(const std::string& backend_name,
                                                   const thallium::engine& engine,
                                                   const json& config);

    /**
     * @brief Opens an existing target and returns a unique_ptr to the
     * created backend instance.
     *
     * @param backend_name Name of the backend to use.
     * @param engine Thallium engine.
     * @param config Configuration object to pass to the backend's open function.
     *
     * @return a unique_ptr to the created Backend.
     */
    static std::unique_ptr<Backend> openTarget(const std::string& backend_name,
                                                const thallium::engine& engine,
                                                const json& config);

    private:

    static std::unordered_map<std::string,
                std::function<std::unique_ptr<Backend>(const thallium::engine&, const json&)>> create_fn;

    static std::unordered_map<std::string,
                std::function<std::unique_ptr<Backend>(const thallium::engine&, const json&)>> open_fn;
};

} // namespace warabi


#define WARABI_REGISTER_BACKEND(__backend_name, __backend_type) \
    static __WarabiBackendRegistration<__backend_type> __warabi ## __backend_name ## _backend( #__backend_name )

template<typename BackendType>
class __WarabiBackendRegistration {

    using json = nlohmann::json;

    public:

    __WarabiBackendRegistration(const std::string& backend_name)
    {
        warabi::TargetFactory::create_fn[backend_name] = [backend_name](const thallium::engine& engine, const json& config) {
            auto p = BackendType::create(engine, config);
            p->m_name = backend_name;
            return p;
        };
        warabi::TargetFactory::open_fn[backend_name] = [backend_name](const thallium::engine& engine, const json& config) {
            auto p = BackendType::open(engine, config);
            p->m_name = backend_name;
            return p;
        };
    }
};

#endif
