/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/Provider.hpp"

#include "ProviderImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace warabi {

Provider::Provider(
        const tl::engine& engine,
        uint16_t provider_id,
        const std::string& config,
        const tl::pool& p,
        remi_client_t remi_cl,
        remi_provider_t remi_pr)
: self(std::make_shared<ProviderImpl>(engine, provider_id, config, p, remi_cl, remi_pr)) {
    self->get_engine().push_finalize_callback(this, [p=this]() { p->self.reset(); });
}

Provider::Provider(
        margo_instance_id mid,
        uint16_t provider_id,
        const std::string& config,
        const tl::pool& p,
        remi_client_t remi_cl,
        remi_provider_t remi_pr)
: self(std::make_shared<ProviderImpl>(mid, provider_id, config, p, remi_cl, remi_pr)) {
    self->get_engine().push_finalize_callback(this, [p=this]() { p->self.reset(); });
}

Provider::Provider(Provider&& other) {
    other.self->get_engine().pop_finalize_callback(this);
    self = std::move(other.self);
    self->get_engine().push_finalize_callback(this, [p=this]() { p->self.reset(); });
}

Provider::~Provider() {
    if(self) {
        self->get_engine().pop_finalize_callback(this);
    }
}

std::string Provider::getConfig() const {
    return self ? self->getConfig() : "null";
}

Provider::operator bool() const {
    return static_cast<bool>(self);
}

}
