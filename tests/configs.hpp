#include <string>
#include <fmt/format.h>

static inline std::string makeConfigForBackend(const std::string& type) {
    if(type == "memory") {
        return "{}";
    }
    if(type == "pmdk") {
        return R"({
            "path": "/tmp/warabi-test-target.dat",
            "create_if_missing_with_size": 10485760,
            "override_if_exists": true
        })";
    }
    return "{}";
}
