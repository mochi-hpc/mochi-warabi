/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <warabi/Client.hpp>
#include <warabi/Provider.hpp>
#include "defer.hpp"
#include "configs.hpp"

TEST_CASE("Target test", "[target]") {

    auto target_type = GENERATE(as<std::string>{}, "memory", "pmdk", "abtio");
    auto tm_type = GENERATE(as<std::string>{}, "__default__", "pipeline");

    CAPTURE(target_type);
    CAPTURE(tm_type);

    auto pr_config = makeConfigForProvider(target_type, tm_type);

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    DEFER(engine.finalize());

    warabi::Provider provider(engine, 42, pr_config);

    SECTION("Access target via a TargetHandle") {

        warabi::Client client(engine);
        std::string addr = engine.self();

        auto th = client.makeTargetHandle(addr, 42);
        th.setEagerReadThreshold(128);
        th.setEagerWriteThreshold(128);

        warabi::RegionID invalidID;
        std::memset(invalidID.data(), 234, invalidID.size());

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
            REQUIRE_THROWS_AS(th.write(invalidID, 0, in.data(), in.size()), warabi::Exception);

            /* persist the region */
            REQUIRE_NOTHROW(th.persist(regionID, 0, in.size()));

            /* persist region with invalid ID */
            if(target_type == "abtio") {
                REQUIRE_NOTHROW(th.persist(invalidID, 0, in.size()));
            } else {
                REQUIRE_THROWS_AS(th.persist(invalidID, 0, in.size()), warabi::Exception);
            }

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
            REQUIRE_NOTHROW(th.write(invalidID, 0, in.data(), in.size(), false, &req));
            REQUIRE_THROWS_AS(req.wait(), warabi::Exception);

            /* persist the region */
            REQUIRE_NOTHROW(th.persist(regionID, 0, in.size(), &req));
            REQUIRE_NOTHROW(req.wait());

            /* persist region with invalid ID */
            REQUIRE_NOTHROW(th.persist(invalidID, 0, in.size(), &req));
            if(target_type != "abtio") {
                REQUIRE_THROWS_AS(req.wait(), warabi::Exception);
            } else {
                REQUIRE_NOTHROW(req.wait());
            }

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
