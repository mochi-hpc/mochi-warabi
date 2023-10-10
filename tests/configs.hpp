#include <string>
#include <fmt/format.h>

static inline std::string makeConfigForBackend(const std::string& type) {
    if(type == "memory") {
        return R"({
            "transfer_manager": "tm"
        })";
    }
    if(type == "pmdk") {
        return R"({
            "transfer_manager": "tm",
            "path": "/tmp/warabi-pmdk-test-target.dat",
            "create_if_missing_with_size": 10485760,
            "override_if_exists": true
        })";
    }
    if(type == "abtio") {
        return R"({
            "transfer_manager": "tm",
            "path": "/tmp/warabi-abtio-test-target.dat",
            "create_if_missing": true,
            "override_if_exists": true
        })";
    }
    return "{}";
}

static inline std::string makeConfigForTransferManager(const std::string& type) {
    if(type == "__default__") {
        return "{}";
    }
    if(type == "pipeline") {
        return R"({
            "num_pools": 2,
            "num_buffers_per_pool": 8,
            "first_buffer_size": 1024,
            "buffer_size_multiple": 2
        })";
    }
    return "{}";
}
