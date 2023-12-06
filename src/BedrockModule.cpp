/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "warabi/Client.hpp"
#include "warabi/Provider.hpp"
#include "warabi/TargetHandle.hpp"
#include <bedrock/AbstractServiceFactory.hpp>

namespace tl = thallium;

class WarabiFactory : public bedrock::AbstractServiceFactory {

    public:

    WarabiFactory() {}

    void *registerProvider(const bedrock::FactoryArgs &args) override {
        remi_client_t remi_cl = nullptr;
        remi_provider_t remi_pr = nullptr;
        auto it = args.dependencies.find("remi_client");
        if(it != args.dependencies.end() && it->second.dependencies.size() != 0) {
            remi_cl = it->second.dependencies[0]->getHandle<remi_client_t>();
        }
        it = args.dependencies.find("remi_provider");
        if(it != args.dependencies.end() && it->second.dependencies.size() != 0) {
            remi_pr = it->second.dependencies[0]->getHandle<remi_provider_t>();
        }
        auto provider = new warabi::Provider(args.mid, args.provider_id,
                args.config, tl::pool(args.pool), remi_cl, remi_pr);
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

    void migrateProvider(
        void* p, const char* dest_addr,
        uint16_t dest_provider_id,
        const char* options_json, bool remove_source) override {
        auto provider = static_cast<warabi::Provider *>(p);
        if(!remove_source) {
            throw warabi::Exception{
                "Migrating a provider without removing the source is not supported"};
        }
        provider->migrateTarget(dest_addr, dest_provider_id, options_json);
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
        auto addr_str = static_cast<std::string>(tl::endpoint{client->engine(), address, false});
        auto th = new warabi::TargetHandle{
            client->makeTargetHandle(addr_str, provider_id)};
        return static_cast<void *>(th);
    }

    void destroyProviderHandle(void *providerHandle) override {
        auto th = static_cast<warabi::TargetHandle *>(providerHandle);
        delete th;
    }

    const std::vector<bedrock::Dependency> &getProviderDependencies() override {
        static const std::vector<bedrock::Dependency> no_dependency = {
            {"remi_client", "remi", BEDROCK_OPTIONAL},
            {"remi_provider", "remi", BEDROCK_OPTIONAL}
        };
        return no_dependency;
    }

    const std::vector<bedrock::Dependency> &getClientDependencies() override {
        static const std::vector<bedrock::Dependency> no_dependency;
        return no_dependency;
    }
};

BEDROCK_REGISTER_MODULE_FACTORY(warabi, WarabiFactory)
