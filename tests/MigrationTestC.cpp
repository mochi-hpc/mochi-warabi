/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <warabi/client.h>
#include <warabi/server.h>
#include <thallium.hpp>
#include <remi/remi-server.h>
#include <remi/remi-client.h>
#include "defer.hpp"
#include "configs.hpp"

TEST_CASE("Target migration test in C", "[migration]") {

    auto target_type = GENERATE(as<std::string>{}, "pmdk", "abtio");
    auto tm_type = std::string{"__default__"};

    CAPTURE(target_type);

    auto pr_config = makeConfigForProvider(target_type, tm_type);

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    DEFER(engine.finalize());

    remi_client_t remi_client;
    REQUIRE(REMI_SUCCESS == remi_client_init(engine.get_margo_instance(), ABT_IO_INSTANCE_NULL, &remi_client));
    DEFER(remi_client_finalize(remi_client));

    remi_provider_t remi_provider;
    REQUIRE(REMI_SUCCESS == remi_provider_register(engine.get_margo_instance(),
            ABT_IO_INSTANCE_NULL, 3, ABT_POOL_NULL, &remi_provider));

    warabi_err_t err = WARABI_SUCCESS;
    DEFER(warabi_err_free(err));

    warabi_provider_t provider1, provider2;
    warabi_provider_init_args args1 = {
        ABT_POOL_NULL, remi_client, REMI_PROVIDER_NULL
    };
    warabi_provider_init_args args2 = {
        ABT_POOL_NULL, REMI_CLIENT_NULL, remi_provider
    };
    err = warabi_provider_register(&provider1, engine.get_margo_instance(), 1, pr_config.c_str(), &args1);
    REQUIRE(err == WARABI_SUCCESS);
    DEFER(warabi_provider_deregister(provider1));
    err = warabi_provider_register(&provider2, engine.get_margo_instance(), 2, "{}", &args2);
    REQUIRE(err == WARABI_SUCCESS);
    DEFER(warabi_provider_deregister(provider2));

    std::string addr = engine.self();

    SECTION("Fill target, migrate, and read") {

        warabi_client_t client;
        err = warabi_client_create(engine.get_margo_instance(), &client);
        REQUIRE(err == WARABI_SUCCESS);
        DEFER(warabi_client_free(client));

        // check that we can already create a TargetHandle for
        // provider 2 but that we cannot do anything with it yet
        warabi_target_handle_t th2;
        err = warabi_client_make_target_handle(client, addr.c_str(), 2, &th2);
        REQUIRE(err == WARABI_SUCCESS);
        DEFER(warabi_target_handle_free(th2));
        warabi_region_t rid;
        err = warabi_create_write(th2, "abcd", 4, true, &rid, nullptr);
        REQUIRE(err != WARABI_SUCCESS);
        warabi_err_free(err); err = WARABI_SUCCESS;

        // create a target handle for provider 1
        warabi_target_handle_t th1;
        err = warabi_client_make_target_handle(client, addr.c_str(), 1, &th1);
        REQUIRE(err == WARABI_SUCCESS);
        DEFER(warabi_target_handle_free(th1));

        // use provider 1 to create a bunch of regions
        auto data_size = 196;
        std::vector<warabi_region_t> regionIDs;
        for(unsigned i=0; i < 16; ++i) {
            std::vector<char> in(data_size);
            for(size_t j = 0; j < in.size(); ++j) in[j] = 'A' + (i+j % 26);

            warabi_region_t regionID;
            warabi_create_write(th1, in.data(), in.size(), true, &regionID, nullptr);
            REQUIRE(err == WARABI_SUCCESS);

            regionIDs.push_back(regionID);
        }

        // issue a migration from provider 1 to provider 2
        auto migrationOptions = R"({
            "new_root": "/tmp/warabi-migrated-targets",
            "transfer_size": 1024,
            "merge_config": {},
            "remove_source": true
        })";
        err = warabi_provider_migrate(provider1, addr.c_str(), 2, migrationOptions);
        REQUIRE(err == WARABI_SUCCESS);

        // check that we can read back all the regions from provider 2
        for(unsigned i=0; i < 16; ++i) {
            std::vector<char> out(data_size);
            const warabi_region_t& regionID = regionIDs[i];
            err = warabi_read(th2, regionID, 0, out.data(), out.size(), nullptr);
            REQUIRE(err == WARABI_SUCCESS);
            for(size_t j = 0; j < out.size(); ++j)
                REQUIRE(out[j] == 'A' + (i+j % 26));
        }

        // check that we are now not allowed to access provider 1
        err = warabi_create_write(th1, "abcd", 4, true, &rid, nullptr);
        REQUIRE(err != WARABI_SUCCESS);
        warabi_err_free(err); err = WARABI_SUCCESS;
    }
}
