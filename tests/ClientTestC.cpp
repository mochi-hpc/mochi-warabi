/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <warabi/client.h>
#include <warabi/Provider.hpp>
#include "defer.hpp"
#include "configs.hpp"

TEST_CASE("Client tests in C", "[c/client]") {

    auto target_type = GENERATE(as<const char*>{}, "memory", "pmdk", "abtio");
    auto tm_type = GENERATE(as<const char*>{}, "__default__", "pipeline");

    CAPTURE(target_type);
    CAPTURE(tm_type);

    auto pr_config = makeConfigForProvider(target_type, tm_type);

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    DEFER(engine.finalize());

    // Initialize the provider
    warabi::Provider provider(engine, 42, pr_config);

    SECTION("Open target") {

        warabi_err_t err = WARABI_SUCCESS;
        DEFER(warabi_err_free(err));

        warabi_client_t client = nullptr;
        err = warabi_client_create(engine.get_margo_instance(), &client);
        REQUIRE(err == WARABI_SUCCESS);
        DEFER(warabi_client_free(client));

        std::string addr = engine.self();

        warabi_target_handle_t th = nullptr;
        err = warabi_client_make_target_handle(client, addr.c_str(), 42, &th);
        REQUIRE(th != nullptr);
        REQUIRE(err == WARABI_SUCCESS);

        err = warabi_target_handle_free(th);
        REQUIRE(err == WARABI_SUCCESS);
    }
}
