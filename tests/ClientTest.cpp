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
#include "defer.hpp"
#include "configs.hpp"

TEST_CASE("Client test", "[client]") {

    auto target_type = GENERATE(as<std::string>{}, "memory", "pmdk", "abtio");
    auto tm_type = GENERATE(as<std::string>{}, "__default__", "pipeline");

    CAPTURE(target_type);
    CAPTURE(tm_type);

    auto pr_config = makeConfigForProvider(target_type, tm_type);

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    DEFER(engine.finalize());

    // Initialize the provider
    warabi::Provider provider(engine, 42, pr_config);

    SECTION("Open target") {
        warabi::Client client(engine);
        std::string addr = engine.self();

        warabi::TargetHandle my_target = client.makeTargetHandle(addr, 42);
        REQUIRE(static_cast<bool>(my_target));
    }
}
