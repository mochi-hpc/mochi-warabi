/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <warabi/Client.hpp>
#include <warabi/Provider.hpp>
#include <remi/remi-server.h>
#include <remi/remi-client.h>
#include "defer.hpp"
#include "configs.hpp"

TEST_CASE("Target migration test", "[migration]") {

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

    warabi::Provider provider1(engine, 1, pr_config, engine.get_handler_pool(), remi_client, REMI_PROVIDER_NULL);
    warabi::Provider provider2(engine, 2, "{}", engine.get_handler_pool(), REMI_CLIENT_NULL, remi_provider);
    std::string addr = engine.self();

    SECTION("Fill target, migrate, and read") {
        warabi::Client client(engine);
        std::string addr = engine.self();

        // check that we can already create a TargetHandle for
        // provider 2 but that we cannot do anything with it yet
        auto th2 = client.makeTargetHandle(addr, 2);
        warabi::RegionID rid;
        REQUIRE_THROWS_AS(th2.createAndWrite(&rid, "abcd", 4),
                          warabi::Exception);

        // use provider 1 to create a bunch of regions
        auto data_size = 196;
        auto th1 = client.makeTargetHandle(addr, 1);
        std::vector<warabi::RegionID> regionIDs;
        for(unsigned i=0; i < 16; ++i) {
            std::vector<char> in(data_size);
            for(size_t j = 0; j < in.size(); ++j) in[j] = 'A' + (i+j % 26);

            warabi::RegionID regionID;
            REQUIRE_NOTHROW(th1.createAndWrite(&regionID, in.data(), in.size(), true));
            regionIDs.push_back(regionID);
        }

        // issue a migration from provider 1 to provider 2
        auto migrationOptions = R"({
            "new_root": "/tmp/warabi-migrated-targets",
            "transfer_size": 1024,
            "merge_config": {},
            "remove_source": true,
            "remove_destination": false
        })";
        REQUIRE_NOTHROW(provider1.migrateTarget(addr, 2, migrationOptions));

        // check that we can read back all the regions from provider 2
        for(unsigned i=0; i < 16; ++i) {
            std::vector<char> out(data_size);
            const warabi::RegionID& regionID = regionIDs[i];
            REQUIRE_NOTHROW(th2.read(regionID, 0, out.data(), out.size()));
            for(size_t j = 0; j < out.size(); ++j)
                REQUIRE(out[j] == 'A' + (i+j % 26));
        }

        // check that we are now not allowed to access provider 1
        REQUIRE_THROWS_AS(th1.createAndWrite(&rid, "abcd", 4),
                          warabi::Exception);
    }
}
