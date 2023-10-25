/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <warabi/Provider.hpp>
#include <warabi/admin.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include "defer.hpp"
#include "configs.hpp"

TEST_CASE("Admin tests in C", "[c/admin]") {

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

        warabi_err_t err = WARABI_SUCCESS;
        DEFER(warabi_err_free(err));

        warabi_admin_t admin = nullptr;
        warabi_admin_create(engine.get_margo_instance(), &admin);
        REQUIRE(admin != nullptr);
        DEFER(warabi_admin_free(admin));
        std::string addr = engine.self();

        SECTION("Create and destroy targets") {
            /* add a transfer manager */
            err = warabi_admin_add_transfer_manager(admin, addr.c_str(), 0, "tm", tm_type.c_str(), tm_config.c_str());
            REQUIRE(err == WARABI_SUCCESS);

            /* add a transfer manager with a name that already exists */
            err = warabi_admin_add_transfer_manager(admin, addr.c_str(), 0, "tm", tm_type.c_str(), tm_config.c_str());
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;

            /* add a transfer manager with an invalid type */
            err = warabi_admin_add_transfer_manager(admin, addr.c_str(), 0, "tm2", "bla", tm_config.c_str());
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;

            /* add a transfer manager with an invalid config */
            err = warabi_admin_add_transfer_manager(admin, addr.c_str(), 0, "my_tm2", tm_type.c_str(), "{[");
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;

            /* correct target creation */
            warabi_target_id target_id;
            warabi_target_id dummy_id;
            warabi_target_id bad_id;
            err = warabi_admin_add_target(admin, addr.c_str(), 0, target_type.c_str(), target_config.c_str(), target_id);
            REQUIRE(err == WARABI_SUCCESS);

            /* target creation with a bad target type */
            err = warabi_admin_add_target(admin, addr.c_str(), 0, "blabla", target_config.c_str(), dummy_id);
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;

            /* target creation with a bad configuration */
            err = warabi_admin_add_target(admin, addr.c_str(), 0, target_type.c_str(), "{[", dummy_id);
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;

            /* target removal */
            err = warabi_admin_remove_target(admin, addr.c_str(), 0, target_id);
            REQUIRE(err == WARABI_SUCCESS);

            /* target removal with invalid id */
            err = warabi_admin_remove_target(admin, addr.c_str(), 0, bad_id);
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;

            /* re-add the target so we can destroy it */
            err = warabi_admin_add_target(admin, addr.c_str(), 0, target_type.c_str(), target_config.c_str(), target_id);
            REQUIRE(err == WARABI_SUCCESS);

            /* correctly destroy target */
            err = warabi_admin_destroy_target(admin, addr.c_str(), 0, target_id);
            REQUIRE(err == WARABI_SUCCESS);

            /* destroy a target with an ID that does not exist */
            err = warabi_admin_destroy_target(admin, addr.c_str(), 0, bad_id);
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;
        }
    }
}
