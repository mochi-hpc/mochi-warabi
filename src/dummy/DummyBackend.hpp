/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __DUMMY_BACKEND_HPP
#define __DUMMY_BACKEND_HPP

#include <alpha/Backend.hpp>

using json = nlohmann::json;

/**
 * Dummy implementation of an alpha Backend.
 */
class DummyResource : public alpha::Backend {

    thallium::engine m_engine;
    json             m_config;

    public:

    /**
     * @brief Constructor.
     */
    DummyResource(thallium::engine engine, const json& config);

    /**
     * @brief Move-constructor.
     */
    DummyResource(DummyResource&&) = default;

    /**
     * @brief Copy-constructor.
     */
    DummyResource(const DummyResource&) = default;

    /**
     * @brief Move-assignment operator.
     */
    DummyResource& operator=(DummyResource&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    DummyResource& operator=(const DummyResource&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~DummyResource() = default;

    /**
     * @brief Get the resource's configuration as a JSON-formatted string.
     */
    std::string getConfig() const override;

    /**
     * @brief Prints Hello World.
     */
    void sayHello() override;

    /**
     * @brief Compute the sum of two integers.
     *
     * @param x first integer
     * @param y second integer
     *
     * @return a Result containing the result.
     */
    alpha::Result<int32_t> computeSum(int32_t x, int32_t y) override;

    /**
     * @brief Destroys the underlying resource.
     *
     * @return a Result<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    alpha::Result<bool> destroy() override;

    /**
     * @brief Static factory function used by the ResourceFactory to
     * create a DummyResource.
     *
     * @param engine Thallium engine
     * @param config JSON configuration for the resource
     *
     * @return a unique_ptr to a resource
     */
    static std::unique_ptr<alpha::Backend> create(const thallium::engine& engine, const json& config);

    /**
     * @brief Static factory function used by the ResourceFactory to
     * open a DummyResource.
     *
     * @param engine Thallium engine
     * @param config JSON configuration for the resource
     *
     * @return a unique_ptr to a resource
     */
    static std::unique_ptr<alpha::Backend> open(const thallium::engine& engine, const json& config);
};

#endif
