/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <warabi/Client.hpp>
#include <warabi/Provider.hpp>
#include <warabi/admin.h>
#include <remi/remi-server.h>
#include <remi/remi-client.h>
#include "defer.hpp"
#include "configs.hpp"

TEST_CASE("Target migration test", "[migration]") {

    auto target_type = GENERATE(as<std::string>{}, "pmdk", "abtio");

    CAPTURE(target_type);

    auto target_config = makeConfigForBackend(target_type);
    auto tm_config = makeConfigForTransferManager("__default__");

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    engine.set_log_level(thallium::logger::level::trace);
    DEFER(engine.finalize());

    remi_client_t remi_client;
    REQUIRE(REMI_SUCCESS == remi_client_init(engine.get_margo_instance(), ABT_IO_INSTANCE_NULL, &remi_client));
    DEFER(remi_client_finalize(remi_client));

    remi_provider_t remi_provider;
    REQUIRE(REMI_SUCCESS == remi_provider_register(engine.get_margo_instance(),
            ABT_IO_INSTANCE_NULL, 2, ABT_POOL_NULL, &remi_provider));

    warabi_err_t err = WARABI_SUCCESS;
    DEFER(warabi_err_free(err));

    warabi_admin_t admin = nullptr;
    err = warabi_admin_create(engine.get_margo_instance(), &admin);
    REQUIRE(err == WARABI_SUCCESS);
    DEFER(warabi_admin_free(admin));

    warabi::Provider provider1(engine, 1, "{}", engine.get_handler_pool(), remi_client, REMI_PROVIDER_NULL);
    warabi::Provider provider2(engine, 2, "{}", engine.get_handler_pool(), REMI_CLIENT_NULL, remi_provider);
    std::string addr = engine.self();
    err = warabi_admin_add_transfer_manager(admin, addr.c_str(), 1, "tm", "__default__", tm_config.c_str());
    REQUIRE(err == WARABI_SUCCESS);
    err = warabi_admin_add_transfer_manager(admin, addr.c_str(), 2, "tm", "__default__", tm_config.c_str());
    REQUIRE(err == WARABI_SUCCESS);
    warabi_target_id target_id;
    err = warabi_admin_add_target(admin, addr.c_str(), 1, target_type.c_str(), target_config.c_str(), target_id);
    REQUIRE(err == WARABI_SUCCESS);

    SECTION("Fill target, migrate, and read") {
        warabi::Client client(engine);
        std::string addr = engine.self();

        auto data_size = 196;
        auto th = client.makeTargetHandle(addr, 1, target_id);
        std::vector<warabi::RegionID> regionIDs;

        for(unsigned i=0; i < 16; ++i) {
            std::vector<char> in(data_size);
            for(size_t j = 0; j < in.size(); ++j) in[j] = 'A' + (i+j % 26);

            warabi::RegionID regionID;
            REQUIRE_NOTHROW(th.createAndWrite(&regionID, in.data(), in.size(), true));
            regionIDs.push_back(regionID);
        }
        warabi_migration_options options = {"/tmp/warabi-migrated-targets", 1024, "{}", true};
        err = warabi_admin_migrate_target(admin, addr.c_str(), 1, target_id, addr.c_str(), 2, &options);
        REQUIRE(err == WARABI_SUCCESS);
        DEFER(warabi_admin_destroy_target(admin, addr.c_str(), 2, target_id));

        th = client.makeTargetHandle(addr, 2, target_id);

        for(unsigned i=0; i < 16; ++i) {
            std::vector<char> out(data_size);
            const warabi::RegionID& regionID = regionIDs[i];
            REQUIRE_NOTHROW(th.read(regionID, 0, out.data(), out.size()));
            for(size_t j = 0; j < out.size(); ++j)
                REQUIRE(out[j] == 'A' + (i+j % 26));
        }
    }
}
