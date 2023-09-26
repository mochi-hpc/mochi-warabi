/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <warabi/Client.hpp>
#include <warabi/Provider.hpp>
#include <warabi/Admin.hpp>

static const std::string target_type = "dummy";
static constexpr const char* target_config = "{ \"path\" : \"mydb\" }";

TEST_CASE("Target test", "[target]") {
    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    warabi::Admin admin(engine);
    warabi::Provider provider(engine);
    std::string addr = engine.self();
    auto target_id = admin.createTarget(addr, 0, target_type, target_config);

    SECTION("Create TargetHandle") {
        warabi::Client client(engine);
        std::string addr = engine.self();

        auto rh = client.makeTargetHandle(addr, 0, target_id);

        SECTION("Send Sum RPC") {
            int32_t result = 0;
            REQUIRE_NOTHROW(rh.computeSum(42, 51, &result));
            REQUIRE(result == 93);

            REQUIRE_NOTHROW(rh.computeSum(42, 51));

            warabi::AsyncRequest request;
            REQUIRE_NOTHROW(rh.computeSum(42, 52, &result, &request));
            REQUIRE_NOTHROW(request.wait());
            REQUIRE(result == 94);
        }

        auto bad_id = warabi::UUID::generate();
        REQUIRE_THROWS_AS(client.makeTargetHandle(addr, 0, bad_id),
                          warabi::Exception);

        REQUIRE_THROWS_AS(client.makeTargetHandle(addr, 1, target_id),
                         std::exception);
        REQUIRE_NOTHROW(client.makeTargetHandle(addr, 0, bad_id, false));
        REQUIRE_NOTHROW(client.makeTargetHandle(addr, 1, target_id, false));
    }

    admin.destroyTarget(addr, 0, target_id);
    engine.finalize();
}
