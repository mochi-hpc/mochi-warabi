/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <warabi/Admin.hpp>
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

    SECTION("Create a provider with an empty configuration") {

        // Initialize the provider
        warabi::Provider provider(mid);
        REQUIRE(static_cast<bool>(provider));

        // Get the configuration
        auto config = json::parse(provider.getConfig());
        json expected = {
            {"targets", json::array()},
            {"transfer_managers", {
                {"__default__", {
                    {"type", "__default__"},
                    {"config", json::object()}
                  }
                }
              }
            }
        };
        REQUIRE(config == expected);
    }

    SECTION("Create a provider with targets and transfer managers") {

        std::string input_config = R"(
            {
                "targets": [
                    {
                        "type": "memory",
                        "config": {
                            "transfer_manager": "my_tm"
                        }
                    }
                ],
                "transfer_managers": {
                    "my_tm": {
                        "type": "pipeline",
                        "config": {
                            "num_pools":4,
                            "num_buffers_per_pool": 4,
                            "first_buffer_size": 128,
                            "buffer_size_multiple": 2
                        }
                    }
                }
            }
        )";

        // Initialize the provider
        warabi::Provider provider(mid, 0, input_config);
        REQUIRE(static_cast<bool>(provider));

        // Get the configuration
        auto config = json::parse(provider.getConfig());

        REQUIRE((config.contains("targets") && config["targets"].is_array()));
        REQUIRE(config["targets"].size() == 1);
        REQUIRE(config["targets"][0].is_object());
        REQUIRE(config["targets"][0].contains("__id__"));
        REQUIRE(config["targets"][0]["__id__"].is_string());
        REQUIRE(config["targets"][0]["__id__"].get<std::string>().size() == 36);
        config["targets"][0].erase("__id__"); // the id changes every time
                                              //
        json expected = json::parse(input_config);
        expected["transfer_managers"]["__default__"] =
            {{"type", "__default__"},
             {"config", json::object()}};
        REQUIRE(config == expected);
    }
}
