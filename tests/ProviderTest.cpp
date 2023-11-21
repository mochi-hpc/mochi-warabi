/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <warabi/Provider.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <nlohmann/json.hpp>
#include "defer.hpp"

using json = nlohmann::json;

TEST_CASE("Provider tests", "[provider]") {

    // in this test, we initialize the server with a margo_instance_id
    margo_instance_id mid = margo_init("na+sm", MARGO_SERVER_MODE, 0, 0);
    DEFER(margo_finalize(mid));

    SECTION("Create a provider with a target and transfer manager") {

        std::string input_config = R"(
            {
                "target": {
                    "type": "memory",
                    "config": {}
                },
                "transfer_manager": {
                    "type": "pipeline",
                    "config": {
                        "num_pools":4,
                        "num_buffers_per_pool": 4,
                        "first_buffer_size": 128,
                        "buffer_size_multiple": 2
                    }
                }
            }
        )";

        // Initialize the provider
        warabi::Provider provider(mid, 42, input_config);
        REQUIRE(static_cast<bool>(provider));

        // Get the configuration
        auto config = json::parse(provider.getConfig());

        REQUIRE(config.contains("target"));
        REQUIRE(config["target"].is_object());
        REQUIRE(config["target"].contains("type"));
        REQUIRE(config["target"]["type"].is_string());
        REQUIRE(config["target"].contains("config"));
        REQUIRE(config["target"]["config"].is_object());

        REQUIRE(config.contains("transfer_manager"));
        REQUIRE(config["transfer_manager"].is_object());
        REQUIRE(config["transfer_manager"].contains("type"));
        REQUIRE(config["transfer_manager"]["type"].is_string());
        REQUIRE(config["transfer_manager"].contains("config"));
        REQUIRE(config["transfer_manager"]["config"].is_object());
    }
}
