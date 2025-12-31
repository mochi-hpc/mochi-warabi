/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <warabi/Provider.hpp>
#include <warabi/Exception.hpp>

#include <thallium.hpp>
#include <nlohmann/json.hpp>

namespace py = pybind11;
using namespace pybind11::literals;

// Helper function to convert Python dict to JSON string
static std::string dict_to_json(const py::dict& d) {
    py::module json = py::module::import("json");
    py::object dumps = json.attr("dumps");
    py::object res = dumps(d);
    return res.cast<std::string>();
}

// Helper function to convert JSON string to Python dict
static py::dict json_to_dict(const std::string& json_str) {
    py::module json = py::module::import("json");
    py::object loads = json.attr("loads");
    py::object res = loads(json_str);
    return res.cast<py::dict>();
}

PYBIND11_MODULE(_pywarabi_server, m) {
    m.doc() = "Python binding for the Warabi server library";

    // Register the Warabi exception
    py::register_exception<warabi::Exception>(m, "Exception");

    // Bind Provider
    py::class_<warabi::Provider>(m, "Provider")
        .def(py::init([](py::object pyMargoEngine,
                         uint16_t provider_id,
                         const py::dict& config) {
            // Extract margo_instance_id from PyMargo Engine
            py::capsule mid = pyMargoEngine.attr("get_internal_mid")();
            auto config_str = dict_to_json(config);
            return warabi::Provider{
                thallium::engine{mid},
                provider_id,
                config_str
            };
        }),
            R"(
            Provider constructor.

            Parameters
            ----------
            engine (pymargo.Engine): PyMargo Engine to use.
            provider_id (int): Provider ID.
            config (dict): Configuration dictionary with the following structure:
                {
                    "target": {
                        "type": "memory" | "abtio" | "pmdk",
                        "config": {
                            // Backend-specific configuration
                            // For abtio/pmdk: "path": "/path/to/file",
                            //                 "create_if_missing": true/false,
                            //                 "override_if_exists": true/false
                            // For pmdk:       "create_if_missing_with_size": <size>
                        }
                    },
                    "transfer_manager": {
                        "type": "__default__" | "pipeline",
                        "config": {
                            // Transfer manager specific config (for pipeline)
                            // "num_pools": <int>,
                            // "num_buffers_per_pool": <int>,
                            // "first_buffer_size": <int>,
                            // "buffer_size_multiplier": <int>
                        }
                    }
                }

            Examples
            --------
            >>> # Memory backend
            >>> provider = Provider(engine, 0, {
            ...     "target": {"type": "memory"}
            ... })
            >>>
            >>> # ABT-IO backend with file
            >>> provider = Provider(engine, 0, {
            ...     "target": {
            ...         "type": "abtio",
            ...         "config": {
            ...             "path": "/tmp/warabi_data.dat",
            ...             "create_if_missing": True
            ...         }
            ...     }
            ... })
            )",
            "engine"_a,
            "provider_id"_a,
            "config"_a,
            py::keep_alive<1, 2>())
        .def("get_config", [](const warabi::Provider& provider) {
            return json_to_dict(provider.getConfig());
        },
            R"(
            Get the provider configuration as a Python dictionary.

            Returns
            -------
            dict: Configuration dictionary.
            )")
        .def("__bool__", [](const warabi::Provider& provider) {
            return static_cast<bool>(provider);
        });
}
