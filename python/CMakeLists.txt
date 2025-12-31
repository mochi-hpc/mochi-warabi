# Find pybind11
find_package(pybind11 REQUIRED)

# Create client module
add_library(_pywarabi_client MODULE py-warabi-client.cpp)
target_link_libraries(_pywarabi_client PRIVATE
    pybind11::module
    warabi-client
    coverage_config)
pybind11_extension(_pywarabi_client)
pybind11_strip(_pywarabi_client)
set_target_properties(_pywarabi_client PROPERTIES
    CXX_VISIBILITY_PRESET "hidden"
    INTERPROCEDURAL_OPTIMIZATION TRUE)

# Create server module
add_library(_pywarabi_server MODULE py-warabi-server.cpp)
target_link_libraries(_pywarabi_server PRIVATE
    pybind11::module
    warabi-server
    coverage_config)
pybind11_extension(_pywarabi_server)
pybind11_strip(_pywarabi_server)
set_target_properties(_pywarabi_server PROPERTIES
    CXX_VISIBILITY_PRESET "hidden"
    INTERPROCEDURAL_OPTIMIZATION TRUE)

# Set installation paths
set (PY_VERSION ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR})

# Install compiled modules
install(TARGETS _pywarabi_client _pywarabi_server
        LIBRARY DESTINATION lib/python${PY_VERSION}/site-packages)

# Install Python package
install(DIRECTORY mochi/warabi
        DESTINATION lib/python${PY_VERSION}/site-packages/mochi)
