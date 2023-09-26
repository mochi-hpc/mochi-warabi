/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __DUMMY_BACKEND_HPP
#define __DUMMY_BACKEND_HPP

#include <warabi/Backend.hpp>

using json = nlohmann::json;

/**
 * Dummy implementation of an warabi Backend.
 */
class DummyTarget : public warabi::Backend {

    thallium::engine m_engine;
    json             m_config;

    public:

    /**
     * @brief Constructor.
     */
    DummyTarget(thallium::engine engine, const json& config);

    /**
     * @brief Move-constructor.
     */
    DummyTarget(DummyTarget&&) = default;

    /**
     * @brief Copy-constructor.
     */
    DummyTarget(const DummyTarget&) = default;

    /**
     * @brief Move-assignment operator.
     */
    DummyTarget& operator=(DummyTarget&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    DummyTarget& operator=(const DummyTarget&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~DummyTarget() = default;

    /**
     * @brief Get the target's configuration as a JSON-formatted string.
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
    warabi::Result<int32_t> computeSum(int32_t x, int32_t y) override;

    /**
     * @brief Destroys the underlying target.
     *
     * @return a Result<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    warabi::Result<bool> destroy() override;

    /**
     * @brief Static factory function used by the TargetFactory to
     * create a DummyTarget.
     *
     * @param engine Thallium engine
     * @param config JSON configuration for the target
     *
     * @return a unique_ptr to a target
     */
    static std::unique_ptr<warabi::Backend> create(const thallium::engine& engine, const json& config);

    /**
     * @brief Static factory function used by the TargetFactory to
     * open a DummyTarget.
     *
     * @param engine Thallium engine
     * @param config JSON configuration for the target
     *
     * @return a unique_ptr to a target
     */
    static std::unique_ptr<warabi::Backend> open(const thallium::engine& engine, const json& config);
};

#endif
