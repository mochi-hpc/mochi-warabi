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

TEST_CASE("Target tests in C", "[c/target]") {

    auto target_type = GENERATE(as<std::string>{}, "memory", "pmdk", "abtio");
    auto tm_type = GENERATE(as<std::string>{}, "__default__", "pipeline");

    CAPTURE(target_type);
    CAPTURE(tm_type);

    auto pr_config = makeConfigForProvider(target_type, tm_type);

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    DEFER(engine.finalize());

    // Initialize the provider
    warabi::Provider provider(engine, 42, pr_config);

    SECTION("Access target via a TargetHandle") {

        warabi_err_t err = WARABI_SUCCESS;
        DEFER(warabi_err_free(err));

        warabi_client_t client = nullptr;
        err = warabi_client_create(engine.get_margo_instance(), &client);
        REQUIRE(err == WARABI_SUCCESS);
        DEFER(warabi_client_free(client));

        std::string addr = engine.self();

        warabi_target_handle_t th = nullptr;;

        err = warabi_client_make_target_handle(client, addr.c_str(), 42, &th);
        REQUIRE(err == WARABI_SUCCESS);
        DEFER(warabi_target_handle_free(th));

        err = warabi_set_eager_read_threshold(th, 128);
        REQUIRE(err == WARABI_SUCCESS);
        err = warabi_set_eager_write_threshold(th, 128);
        REQUIRE(err == WARABI_SUCCESS);

        SECTION("With blocking API") {

            // testing both eager and bulk paths
            auto data_size = GENERATE(64, 196);
            CAPTURE(data_size);

            std::vector<char> in(data_size);
            for(size_t i = 0; i < in.size(); ++i) in[i] = 'A' + (i % 26);

            /* create region */
            warabi_region_t region;
            err = warabi_create(th, in.size(), &region, nullptr);
            REQUIRE(err == WARABI_SUCCESS);

            /* write into the region */
            err = warabi_write(th, region, 0, in.data(), in.size(), false, nullptr);
            REQUIRE(err == WARABI_SUCCESS);

            /* write into a region with an invalid ID */
            warabi_region_t invalid_region = {0};
            err = warabi_write(th, invalid_region, 0, in.data(), in.size(), false, nullptr);
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;

            /* persist the region */
            err = warabi_persist(th, region, 0, in.size(), nullptr);
            REQUIRE(err == WARABI_SUCCESS);

            /* persist region with invalid ID */
            err = warabi_persist(th, invalid_region, 0, in.size(), nullptr);
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;

            /* read the data */
            std::vector<char> out(in.size());
            err = warabi_read(th, region, 0, out.data(), out.size(), nullptr);
            REQUIRE(err == WARABI_SUCCESS);
            REQUIRE(std::memcmp(in.data(), out.data(), in.size()) == 0);

            for(size_t i = 0; i < in.size(); ++i) in[i] = 'a' + (i % 26);

            /* read data from invalid ID */
            err = warabi_read(th, invalid_region, 0, out.data(), out.size(), nullptr);
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;

            /* use createWrite */
            err = warabi_create_write(th, in.data(), in.size(), true, &region, nullptr);
            REQUIRE(err == WARABI_SUCCESS);

            /* read the second region */
            err = warabi_read(th, region, 0, out.data(), out.size(), nullptr);
            REQUIRE(err == WARABI_SUCCESS);
            REQUIRE(std::memcmp(in.data(), out.data(), in.size()) == 0);

            /* erase the region */
            err = warabi_erase(th, region, nullptr);
            REQUIRE(err == WARABI_SUCCESS);

            /* erase region with invalid ID */
            err = warabi_erase(th, invalid_region, nullptr);
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;
        }

        SECTION("With non-blocking API") {
            // testing both eager and bulk paths
            auto data_size = GENERATE(64, 196);
            CAPTURE(data_size);

            std::vector<char> in(data_size);
            for(size_t i = 0; i < in.size(); ++i) in[i] = 'A' + (i % 26);

            warabi_region_t invalid_region = {0};

            warabi_async_request_t req = nullptr;
            bool b = false;
            /* create region */
            warabi_region_t region;
            err = warabi_create(th, in.size(), &region, &req);
            REQUIRE(err == WARABI_SUCCESS);
            err = warabi_test(req, &b);
            REQUIRE(err == WARABI_SUCCESS);
            err = warabi_wait(req);
            REQUIRE(err == WARABI_SUCCESS);

            /* write into the region */
            err = warabi_write(th, region, 0, in.data(), in.size(), false, &req);
            REQUIRE(err == WARABI_SUCCESS);
            err = warabi_wait(req);
            REQUIRE(err == WARABI_SUCCESS);

            /* write into a region with an invalid ID */
            err = warabi_write(th, invalid_region, 0, in.data(), in.size(), false, &req);
            REQUIRE(err == WARABI_SUCCESS);
            err = warabi_wait(req);
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;

            /* persist the region */
            err = warabi_persist(th, region, 0, in.size(), &req);
            REQUIRE(err == WARABI_SUCCESS);
            err = warabi_wait(req);
            REQUIRE(err == WARABI_SUCCESS);

            /* persist region with invalid ID */
            err = warabi_persist(th, invalid_region, 0, in.size(), &req);
            REQUIRE(err == WARABI_SUCCESS);
            err = warabi_wait(req);
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;

            /* read the data */
            std::vector<char> out(in.size());
            err = warabi_read(th, region, 0, out.data(), out.size(), &req);
            REQUIRE(err == WARABI_SUCCESS);
            err = warabi_wait(req);
            REQUIRE(err == WARABI_SUCCESS);
            REQUIRE(std::memcmp(in.data(), out.data(), in.size()) == 0);

            for(size_t i = 0; i < in.size(); ++i) in[i] = 'a' + (i % 26);

            /* read data from invalid ID */
            err = warabi_read(th, invalid_region, 0, out.data(), out.size(), &req);
            REQUIRE(err == WARABI_SUCCESS);
            err = warabi_wait(req);
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;

            /* use createWrite */
            err = warabi_create_write(th, in.data(), in.size(), true, &region, &req);
            REQUIRE(err == WARABI_SUCCESS);
            err = warabi_wait(req);
            REQUIRE(err == WARABI_SUCCESS);

            /* read the second region */
            err = warabi_read(th, region, 0, out.data(), out.size(), &req);
            REQUIRE(err == WARABI_SUCCESS);
            err = warabi_wait(req);
            REQUIRE(err == WARABI_SUCCESS);
            REQUIRE(std::memcmp(in.data(), out.data(), in.size()) == 0);

            /* erase the region */
            err = warabi_erase(th, region, &req);
            REQUIRE(err == WARABI_SUCCESS);
            err = warabi_wait(req);
            REQUIRE(err == WARABI_SUCCESS);

            /* erase region with invalid ID */
            err = warabi_erase(th, invalid_region, &req);
            REQUIRE(err == WARABI_SUCCESS);
            err = warabi_wait(req);
            REQUIRE(err != WARABI_SUCCESS);
            warabi_err_free(err); err = WARABI_SUCCESS;
        }
    }
}
