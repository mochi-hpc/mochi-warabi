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
#include "defer.hpp"

static constexpr const char* target_config = "{ \"path\" : \"mydb\" }";

TEST_CASE("Target test", "[target]") {

    auto target_type = GENERATE(as<std::string>{}, "memory");

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    DEFER(engine.finalize());

    warabi::Admin admin(engine);
    warabi::Provider provider(engine);
    std::string addr = engine.self();
    auto target_id = admin.addTarget(addr, 0, target_type, target_config);
    DEFER(admin.destroyTarget(addr, 0, target_id));

    SECTION("Create bad TargetHandles") {
        warabi::Client client(engine);
        std::string addr = engine.self();

        /* make a TargetHandle with a bad target ID */
        auto bad_id = warabi::UUID::generate();
        REQUIRE_THROWS_AS(client.makeTargetHandle(addr, 0, bad_id),
                          warabi::Exception);

        /* make a TargetHandle with a bad provider ID */
        REQUIRE_THROWS_AS(client.makeTargetHandle(addr, 1, target_id),
                         std::exception);

        /* make a TargetHandle with a bad target ID but check = false */
        REQUIRE_NOTHROW(client.makeTargetHandle(addr, 0, bad_id, false));

        /* make a TargetHandle with a bad provider ID but check = false */
        REQUIRE_NOTHROW(client.makeTargetHandle(addr, 1, target_id, false));
    }

    SECTION("Access target via a TargetHandle") {
        warabi::Client client(engine);
        std::string addr = engine.self();

        auto th = client.makeTargetHandle(addr, 0, target_id);

        SECTION("With blocking API") {
            std::vector<char> in(64);
            for(size_t i = 0; i < in.size(); ++i) in[i] = 'A' + (i % 26);

            /* create region */
            warabi::RegionID regionID;
            REQUIRE_NOTHROW(th.create(&regionID, in.size()));

            /* check the size of the region */
            size_t regionSize = 0;
            REQUIRE_NOTHROW(th.getSize(regionID, &regionSize));
            REQUIRE(regionSize == in.size());

            /* write into the region */
            REQUIRE_NOTHROW(th.write(regionID, 0, in.data(), in.size()));

            /* persist the region */
            REQUIRE_NOTHROW(th.persist(regionID, 0, in.size()));

            /* read the data */
            std::vector<char> out(in.size());
            REQUIRE_NOTHROW(th.read(regionID, 0, out.data(), out.size()));
            REQUIRE(std::memcmp(in.data(), out.data(), in.size()) == 0);

            for(size_t i = 0; i < in.size(); ++i) in[i] = 'a' + (i % 26);

            /* use createWrite */
            REQUIRE_NOTHROW(th.createAndWrite(&regionID, in.data(), in.size(), true));

            /* read the second region */
            REQUIRE_NOTHROW(th.read(regionID, 0, out.data(), out.size()));
            REQUIRE(std::memcmp(in.data(), out.data(), in.size()) == 0);
        }
    }
}
