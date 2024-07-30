#include <sstream>
#include <string>
#include <fmt/format.h>

static inline std::string makeConfigForBackend(const std::string& type) {
    if(type == "memory") {
        return R"({})";
    }
    if(type == "pmdk") {
        return R"({
            "path": "/tmp/warabi-pmdk-test-target.dat",
            "create_if_missing_with_size": 10485760,
            "override_if_exists": true
        })";
    }
    if(type == "abtio") {
        return R"({
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
            "buffer_size_multiplier": 2
        })";
    }
    return "{}";
}

static inline std::string makeConfigForProvider(const std::string& target_type, const std::string& tm_type) {
    std::stringstream ss;
    ss << "{\"target\":{"
       << "\"type\":\"" << target_type << "\",\"config\":"
       << makeConfigForBackend(target_type)
       << "},\"transfer_manager\":{"
       << "\"type\":\"" << tm_type << "\",\"config\":"
       << makeConfigForTransferManager(tm_type)
       << "}}";
    return ss.str();
}
