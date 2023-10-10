/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <warabi/Admin.hpp>
#include <warabi/Provider.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include "defer.hpp"
#include "configs.hpp"

TEST_CASE("Admin tests", "[admin]") {

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

    SECTION("Create an admin") {
        warabi::Admin admin(engine);
        std::string addr = engine.self();

        SECTION("Create and destroy targets") {
            /* add a transfer manager */
            REQUIRE_NOTHROW(admin.addTransferManager(addr, 0, "tm", tm_type, tm_config));

            /* add a transfer manager with a name that already exists */
            REQUIRE_THROWS_AS(admin.addTransferManager(addr, 0, "tm", tm_type, tm_config),
                              warabi::Exception);

            /* add a transfer manager with an invalid type */
            REQUIRE_THROWS_AS(admin.addTransferManager(addr, 0, "tm2", "bla", tm_config),
                              warabi::Exception);

            /* add a transfer manager with an invalid config */
            REQUIRE_THROWS_AS(admin.addTransferManager(addr, 0, "my_tm2", tm_type, "{["),
                              warabi::Exception);

            /* correct target creation */
            warabi::UUID target_id;
            warabi::UUID bad_id;
            REQUIRE_NOTHROW([&]() {
                target_id = admin.addTarget(addr, 0, target_type, target_config);
            }());

            /* target creation with a bad target type */
            REQUIRE_THROWS_AS(admin.addTarget(addr, 0, "blabla", target_config),
                              warabi::Exception);

            /* target creation with a bad configuration */
            REQUIRE_THROWS_AS(admin.addTarget(addr, 0, target_type, "{["),
                              warabi::Exception);

            /* target removal */
            REQUIRE_NOTHROW(admin.removeTarget(addr, 0, target_id));

            /* target removal with invalid id */
            REQUIRE_THROWS_AS(admin.removeTarget(addr, 0, bad_id), warabi::Exception);

            /* re-add the target so we can destroy it */
            REQUIRE_NOTHROW([&]() {
                target_id = admin.addTarget(addr, 0, target_type, target_config);
            }());

            /* correctly destroy target */
            REQUIRE_NOTHROW(admin.destroyTarget(addr, 0, target_id));

            /* destroy a target with an ID that does not exist */
            REQUIRE_THROWS_AS(admin.destroyTarget(addr, 0, bad_id), warabi::Exception);

        }
    }
}
