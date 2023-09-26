/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <warabi/Client.hpp>
#include <spdlog/spdlog.h>
#include <tclap/CmdLine.h>
#include <iostream>

namespace tl = thallium;

static std::string g_address;
static std::string g_protocol;
static std::string g_target;
static unsigned    g_provider_id;
static std::string g_log_level = "info";

static void parse_command_line(int argc, char** argv);

int main(int argc, char** argv) {
    parse_command_line(argc, argv);
    spdlog::set_level(spdlog::level::from_str(g_log_level));

    // Initialize the thallium server
    tl::engine engine(g_protocol, THALLIUM_CLIENT_MODE);

    try {

        // Initialize a Client
        warabi::Client client(engine);

        // Open the Target "mytarget" from provider 0
        warabi::TargetHandle target =
            client.makeTargetHandle(g_address, g_provider_id,
                    warabi::UUID::from_string(g_target.c_str()));

        int32_t result;
        target.computeSum(32, 54, &result);

    } catch(const warabi::Exception& ex) {
        std::cerr << ex.what() << std::endl;
        exit(-1);
    }

    return 0;
}

void parse_command_line(int argc, char** argv) {
    try {
        TCLAP::CmdLine cmd("Warabi client", ' ', "0.1");
        TCLAP::ValueArg<std::string> addressArg("a","address","Address or server", true,"","string");
        TCLAP::ValueArg<unsigned>    providerArg("p", "provider", "Provider id to contact (default 0)", false, 0, "int");
        TCLAP::ValueArg<std::string> targetArg("r","target","Target id", true, warabi::UUID().to_string(),"string");
        TCLAP::ValueArg<std::string> logLevel("v","verbose", "Log level (trace, debug, info, warning, error, critical, off)", false, "info", "string");
        cmd.add(addressArg);
        cmd.add(providerArg);
        cmd.add(targetArg);
        cmd.add(logLevel);
        cmd.parse(argc, argv);
        g_address = addressArg.getValue();
        g_provider_id = providerArg.getValue();
        g_target = targetArg.getValue();
        g_log_level = logLevel.getValue();
        g_protocol = g_address.substr(0, g_address.find(":"));
    } catch(TCLAP::ArgException &e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
        exit(-1);
    }
}
