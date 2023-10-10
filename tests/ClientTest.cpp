/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <warabi/Client.hpp>
#include <warabi/Provider.hpp>
#include <warabi/TargetHandle.hpp>
#include <warabi/Admin.hpp>
#include "defer.hpp"
#include "configs.hpp"

TEST_CASE("Client test", "[client]") {

    auto target_type = GENERATE(as<std::string>{}, "memory", "pmdk", "abtio");
    auto tm_type = GENERATE(as<std::string>{}, "__default__", "pipeline");

    CAPTURE(target_type);
    CAPTURE(tm_type);

    auto target_config = makeConfigForBackend(target_type);
    auto tm_config = makeConfigForTransferManager(tm_type);

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    DEFER(engine.finalize());

    // Initialize the provider
    warabi::Provider provider(engine);
    warabi::Admin admin(engine);
    std::string addr = engine.self();
    admin.addTransferManager(addr, 0, "tm", tm_type, tm_config);
    auto target_id = admin.addTarget(addr, 0, target_type, target_config);
    DEFER(admin.destroyTarget(addr, 0, target_id));

    SECTION("Open target") {
        warabi::Client client(engine);
        std::string addr = engine.self();

        warabi::TargetHandle my_target = client.makeTargetHandle(addr, 0, target_id);
        REQUIRE(static_cast<bool>(my_target));

        auto bad_id = warabi::UUID::generate();
        REQUIRE_THROWS_AS(client.makeTargetHandle(addr, 0, bad_id), warabi::Exception);
    }
}
