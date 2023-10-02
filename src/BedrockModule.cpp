/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/Client.hpp"
#include "warabi/Provider.hpp"
#include "warabi/ProviderHandle.hpp"
#include <bedrock/AbstractServiceFactory.hpp>

namespace tl = thallium;

class WarabiFactory : public bedrock::AbstractServiceFactory {

    public:

    WarabiFactory() {}

    void *registerProvider(const bedrock::FactoryArgs &args) override {
        auto provider = new warabi::Provider(args.mid, args.provider_id,
                args.config, tl::pool(args.pool));
        return static_cast<void *>(provider);
    }

    void deregisterProvider(void *p) override {
        auto provider = static_cast<warabi::Provider *>(p);
        delete provider;
    }

    std::string getProviderConfig(void *p) override {
        auto provider = static_cast<warabi::Provider *>(p);
        return provider->getConfig();
    }

    void *initClient(const bedrock::FactoryArgs& args) override {
        return static_cast<void *>(new warabi::Client(args.mid));
    }

    void finalizeClient(void *client) override {
        delete static_cast<warabi::Client *>(client);
    }

    std::string getClientConfig(void* c) override {
        auto client = static_cast<warabi::Client*>(c);
        return client->getConfig();
    }

    void *createProviderHandle(void *c, hg_addr_t address,
            uint16_t provider_id) override {
        auto client = static_cast<warabi::Client *>(c);
        auto ph = new warabi::ProviderHandle(
                client->engine(),
                address,
                provider_id,
                false);
        return static_cast<void *>(ph);
    }

    void destroyProviderHandle(void *providerHandle) override {
        auto ph = static_cast<warabi::ProviderHandle *>(providerHandle);
        delete ph;
    }

    const std::vector<bedrock::Dependency> &getProviderDependencies() override {
        static const std::vector<bedrock::Dependency> no_dependency;
        return no_dependency;
    }

    const std::vector<bedrock::Dependency> &getClientDependencies() override {
        static const std::vector<bedrock::Dependency> no_dependency;
        return no_dependency;
    }
};

BEDROCK_REGISTER_MODULE_FACTORY(warabi, WarabiFactory)
