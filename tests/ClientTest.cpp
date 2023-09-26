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

static constexpr const char* target_config = "{ \"path\" : \"mydb\" }";
static const std::string target_type = "dummy";

TEST_CASE("Client test", "[client]") {

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    // Initialize the provider
    warabi::Provider provider(engine);
    warabi::Admin admin(engine);
    std::string addr = engine.self();
    auto target_id = admin.createTarget(addr, 0, target_type, target_config);

    SECTION("Open target") {
        warabi::Client client(engine);
        std::string addr = engine.self();

        warabi::TargetHandle my_target = client.makeTargetHandle(addr, 0, target_id);
        REQUIRE(static_cast<bool>(my_target));

        auto bad_id = warabi::UUID::generate();
        REQUIRE_THROWS_AS(client.makeTargetHandle(addr, 0, bad_id), warabi::Exception);
    }

    admin.destroyTarget(addr, 0, target_id);
    engine.finalize();
}
