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
#include "configs.hpp"

TEST_CASE("Target test", "[target]") {

    auto target_type = GENERATE(as<std::string>{}, "memory", "pmdk", "abtio");
    auto tm_type = GENERATE(as<std::string>{}, "__default__", "pipeline");

    CAPTURE(target_type);
    CAPTURE(tm_type);

    auto target_config = makeConfigForBackend(target_type);
    auto tm_config = makeConfigForTransferManager(tm_type);

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    DEFER(engine.finalize());

    warabi::Admin admin(engine);
    warabi::Provider provider(engine);
    std::string addr = engine.self();
    admin.addTransferManager(addr, 0, "tm", tm_type, tm_config);
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
        th.setEagerReadThreshold(128);
        th.setEagerWriteThreshold(128);

        SECTION("With blocking API") {

            // testing both eager and bulk paths
            auto data_size = GENERATE(64, 196);
            CAPTURE(data_size);

            std::vector<char> in(data_size);
            for(size_t i = 0; i < in.size(); ++i) in[i] = 'A' + (i % 26);

            /* create region */
            warabi::RegionID regionID;
            REQUIRE_NOTHROW(th.create(&regionID, in.size()));

            /* write into the region */
            REQUIRE_NOTHROW(th.write(regionID, 0, in.data(), in.size()));

            /* write into a region with an invalid ID */
            warabi::RegionID invalidID;
            REQUIRE_THROWS_AS(th.write(invalidID, 0, in.data(), in.size()), warabi::Exception);

            /* persist the region */
            REQUIRE_NOTHROW(th.persist(regionID, 0, in.size()));

            /* persist region with invalid ID */
            REQUIRE_THROWS_AS(th.persist(invalidID, 0, in.size()), warabi::Exception);

            /* read the data */
            std::vector<char> out(in.size());
            REQUIRE_NOTHROW(th.read(regionID, 0, out.data(), out.size()));
            REQUIRE(std::memcmp(in.data(), out.data(), in.size()) == 0);

            for(size_t i = 0; i < in.size(); ++i) in[i] = 'a' + (i % 26);

            /* read data from invalid ID */
            REQUIRE_THROWS_AS(th.read(invalidID, 0, out.data(), out.size()), warabi::Exception);

            /* use createWrite */
            REQUIRE_NOTHROW(th.createAndWrite(&regionID, in.data(), in.size(), true));

            /* read the second region */
            REQUIRE_NOTHROW(th.read(regionID, 0, out.data(), out.size()));
            REQUIRE(std::memcmp(in.data(), out.data(), in.size()) == 0);

            /* erase the region */
            REQUIRE_NOTHROW(th.erase(regionID));

            /* erase region with invalid ID */
            REQUIRE_THROWS_AS(th.erase(invalidID), warabi::Exception);
        }

        SECTION("With non-blocking API") {

            // testing both eager and bulk paths
            auto data_size = GENERATE(64, 196);
            CAPTURE(data_size);

            std::vector<char> in(data_size);
            for(size_t i = 0; i < in.size(); ++i) in[i] = 'A' + (i % 26);

            warabi::AsyncRequest req;

            /* create region */
            warabi::RegionID regionID;
            REQUIRE_NOTHROW(th.create(&regionID, in.size(), &req));
            req.completed(); // called for code coverage
            REQUIRE_NOTHROW(req.wait());

            /* write into the region */
            REQUIRE_NOTHROW(th.write(regionID, 0, in.data(), in.size(), false, &req));
            REQUIRE_NOTHROW(req.wait());

            /* write in a region with an invalid ID */
            warabi::RegionID invalidID;
            REQUIRE_NOTHROW(th.write(invalidID, 0, in.data(), in.size(), false, &req));
            REQUIRE_THROWS_AS(req.wait(), warabi::Exception);

            /* persist the region */
            REQUIRE_NOTHROW(th.persist(regionID, 0, in.size(), &req));
            REQUIRE_NOTHROW(req.wait());

            /* persist region with invalid ID */
            REQUIRE_NOTHROW(th.persist(invalidID, 0, in.size(), &req));
            REQUIRE_THROWS_AS(req.wait(), warabi::Exception);

            /* read the data */
            std::vector<char> out(in.size());
            REQUIRE_NOTHROW(th.read(regionID, 0, out.data(), out.size(), &req));
            REQUIRE_NOTHROW(req.wait());
            REQUIRE(std::memcmp(in.data(), out.data(), in.size()) == 0);

            for(size_t i = 0; i < in.size(); ++i) in[i] = 'a' + (i % 26);

            /* read data with invalid ID */
            REQUIRE_NOTHROW(th.read(invalidID, 0, out.data(), out.size(), &req));
            REQUIRE_THROWS_AS(req.wait(), warabi::Exception);

            /* use createWrite */
            REQUIRE_NOTHROW(th.createAndWrite(&regionID, in.data(), in.size(), true, &req));
            REQUIRE_NOTHROW(req.wait());

            /* read the second region */
            REQUIRE_NOTHROW(th.read(regionID, 0, out.data(), out.size(), &req));
            REQUIRE_NOTHROW(req.wait());
            REQUIRE(std::memcmp(in.data(), out.data(), in.size()) == 0);

            /* erase the region */
            REQUIRE_NOTHROW(th.erase(regionID, &req));
            REQUIRE_NOTHROW(req.wait());

            /* erase region with invalid ID*/
            REQUIRE_NOTHROW(th.erase(invalidID, &req));
            REQUIRE_THROWS_AS(req.wait(), warabi::Exception);
        }
    }
}
