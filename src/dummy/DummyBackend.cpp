/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "DummyBackend.hpp"
#include <iostream>

WARABI_REGISTER_BACKEND(dummy, DummyTarget);

DummyTarget::DummyTarget(thallium::engine engine, const json& config)
: m_engine(std::move(engine)),
  m_config(config) {

}

void DummyTarget::sayHello() {
    std::cout << "Hello World" << std::endl;
}

std::string DummyTarget::getConfig() const {
    return m_config.dump();
}

warabi::Result<int32_t> DummyTarget::computeSum(int32_t x, int32_t y) {
    warabi::Result<int32_t> result;
    result.value() = x + y;
    return result;
}

warabi::Result<bool> DummyTarget::destroy() {
    warabi::Result<bool> result;
    result.value() = true;
    // or result.success() = true
    return result;
}

std::unique_ptr<warabi::Backend> DummyTarget::create(const thallium::engine& engine, const json& config) {
    (void)engine;
    return std::unique_ptr<warabi::Backend>(new DummyTarget(engine, config));
}

std::unique_ptr<warabi::Backend> DummyTarget::open(const thallium::engine& engine, const json& config) {
    (void)engine;
    return std::unique_ptr<warabi::Backend>(new DummyTarget(engine, config));
}
