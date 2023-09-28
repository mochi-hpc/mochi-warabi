/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <warabi/Admin.hpp>
#include <warabi/Provider.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>

static constexpr const char* target_config = "{ \"path\" : \"mydb\" }";

TEST_CASE("Admin tests", "[admin]") {

    auto target_type = GENERATE(as<std::string>{}, "memory");

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    // Initialize the provider
    warabi::Provider provider(engine);

    SECTION("Create an admin") {
        warabi::Admin admin(engine);
        std::string addr = engine.self();

        SECTION("Create and destroy targets") {
            warabi::UUID target_id = admin.createTarget(addr, 0, target_type, target_config);

            REQUIRE_THROWS_AS(admin.createTarget(addr, 0, "blabla", target_config),
                              warabi::Exception);

            admin.destroyTarget(addr, 0, target_id);

            warabi::UUID bad_id;
            REQUIRE_THROWS_AS(admin.destroyTarget(addr, 0, bad_id), warabi::Exception);
        }
    }
    // Finalize the engine
    engine.finalize();
}
