/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <warabi/server.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <nlohmann/json.hpp>
#include "defer.hpp"

using json = nlohmann::json;

TEST_CASE("Provider tests in C", "[c/provider]") {

    margo_instance_id mid = margo_init("na+sm", MARGO_SERVER_MODE, 0, 0);
    DEFER(margo_finalize(mid));

    SECTION("Create a provider with an empty configuration") {

        // Initialize the provider
        warabi_provider_t provider = nullptr;
        warabi_err_t err = warabi_provider_register(&provider, mid, 0, nullptr);
        REQUIRE(err == WARABI_SUCCESS);
        REQUIRE(provider != nullptr);
        DEFER(warabi_provider_deregister(provider));

        // Get the configuration
        auto config_str = warabi_provider_get_config(provider);
        REQUIRE(config_str != nullptr);
        DEFER(free(config_str));

        auto config = json::parse(config_str);
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

        warabi_provider_t provider = nullptr;
        warabi_provider_init_args args = {
            input_config.c_str(),
            ABT_POOL_NULL,
            nullptr,
            nullptr
        };
        warabi_err_t err = warabi_provider_register(&provider, mid, 0, &args);
        REQUIRE(err == WARABI_SUCCESS);
        REQUIRE(provider != nullptr);
        DEFER(warabi_provider_deregister(provider));

        // Get the configuration
        auto config_str = warabi_provider_get_config(provider);
        REQUIRE(config_str != nullptr);
        DEFER(free(config_str));

        auto config = json::parse(config_str);

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
