# set source files
set (server-src-files
     Provider.cpp
     Backend.cpp
     TransferManager.cpp
     DefaultTransferManager.cpp
     PipelineTransferManager.cpp
     MemoryBackend.cpp
     PmemBackend.cpp
     AbtIOBackend.cpp)

set (client-src-files
     Client.cpp
     TargetHandle.cpp
     AsyncRequest.cpp)

set (module-src-files
     BedrockModule.cpp)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (warabi-pkg "share/cmake/warabi")

# set version fo shared library
set (warabi-vers "${WARABI_VERSION_MAJOR}.${WARABI_VERSION_MINOR}")

# server library
if (${ENABLE_REMI})
    set (OPTIONAL_REMI remi)
else ()
    set (OPTIONAL_REMI)
endif ()
add_library (warabi-server ${server-src-files})
add_library (warabi::server ALIAS warabi-server)
target_link_libraries (warabi-server
    PUBLIC thallium nlohmann_json::nlohmann_json ${OPTIONAL_REMI}
    PRIVATE ${OPTIONAL_REMI} nlohmann_json_schema_validator::validator
            spdlog::spdlog fmt::fmt PkgConfig::libpmemobj
            PkgConfig::abt-io stdc++fs coverage_config)
target_include_directories (warabi-server PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (warabi-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (warabi-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (warabi-server
    PROPERTIES VERSION ${WARABI_VERSION}
    SOVERSION ${WARABI_VERSION_MAJOR})

# client library
add_library (warabi-client ${client-src-files})
add_library (warabi::client ALIAS warabi-client)
target_link_libraries (warabi-client
    PUBLIC thallium nlohmann_json::nlohmann_json
    PRIVATE spdlog::spdlog fmt::fmt coverage_config)
target_include_directories (warabi-client PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (warabi-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (warabi-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (warabi-client
    PROPERTIES VERSION ${WARABI_VERSION}
    SOVERSION ${WARABI_VERSION_MAJOR})

# C error library
add_library (warabi-c-error ${CMAKE_CURRENT_SOURCE_DIR}/c/error.cpp)
add_library (warabi::c-error ALIAS warabi-c-error)
target_link_libraries (warabi-c-error PRIVATE warabi-client coverage_config)
target_include_directories (warabi-c-error PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (warabi-c-error BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (warabi-c-error BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (warabi-c-error
    PROPERTIES VERSION ${WARABI_VERSION}
    SOVERSION ${WARABI_VERSION_MAJOR})

# C client library
add_library (warabi-c-client ${CMAKE_CURRENT_SOURCE_DIR}/c/client.cpp)
add_library (warabi::c-client ALIAS warabi-c-client)
target_link_libraries (warabi-c-client PUBLIC warabi-c-error PRIVATE warabi-client coverage_config)
target_include_directories (warabi-c-client PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (warabi-c-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (warabi-c-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (warabi-c-client
    PROPERTIES VERSION ${WARABI_VERSION}
    SOVERSION ${WARABI_VERSION_MAJOR})

# C server library
add_library (warabi-c-server ${CMAKE_CURRENT_SOURCE_DIR}/c/server.cpp)
add_library (warabi::c-server ALIAS warabi-c-server)
target_link_libraries (warabi-c-server PUBLIC warabi-c-error PRIVATE warabi-server coverage_config)
target_include_directories (warabi-c-server PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (warabi-c-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (warabi-c-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (warabi-c-server
    PROPERTIES VERSION ${WARABI_VERSION}
    SOVERSION ${WARABI_VERSION_MAJOR})

if (${ENABLE_BEDROCK})
# bedrock module library
add_library (warabi-bedrock-module ${module-src-files})
target_link_libraries (warabi-bedrock-module warabi-server warabi-client bedrock::module-api coverage_config)
target_include_directories (warabi-bedrock-module PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (warabi-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (warabi-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (warabi-bedrock-module
    PROPERTIES VERSION ${WARABI_VERSION}
    SOVERSION ${WARABI_VERSION_MAJOR})
endif ()

# installation stuff (packaging and install commands)
write_basic_package_version_file (
    "warabi-config-version.cmake"
    VERSION ${WARABI_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
set (INCLUDE_INSTALL_DIR include/)
set (LIB_INSTALL_DIR lib/)
configure_package_config_file (warabi-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/warabi-config.cmake
  INSTALL_DESTINATION ${LIB_INSTALL_DIR}/warabi/cmake
  PATH_VARS INCLUDE_INSTALL_DIR)

# some bits for the pkg-config file
set (DEST_DIR "${CMAKE_INSTALL_PREFIX}")
set (SERVER_PRIVATE_LIBS "-lwarabi-server")
set (CLIENT_PRIVATE_LIBS "-lwarabi-client")
configure_file ("warabi-server.pc.in" "warabi-server.pc" @ONLY)
configure_file ("warabi-client.pc.in" "warabi-client.pc" @ONLY)
configure_file ("warabi-c-server.pc.in" "warabi-c-server.pc" @ONLY)
configure_file ("warabi-c-client.pc.in" "warabi-c-client.pc" @ONLY)

# configure config.h
configure_file ("config.h.in" "config.h" @ONLY)

# "make install" rules
install (TARGETS warabi-server warabi-client
                 warabi-c-error warabi-c-client warabi-c-server
         EXPORT warabi-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
if (${ENABLE_BEDROCK})
    install (TARGETS warabi-bedrock-module
             ARCHIVE DESTINATION lib
             LIBRARY DESTINATION lib)
endif ()
install (EXPORT warabi-targets
         DESTINATION ${warabi-pkg}
         FILE "warabi-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/warabi-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/warabi-config-version.cmake"
         DESTINATION ${warabi-pkg} )
install (DIRECTORY ../include/warabi
         DESTINATION include
         FILES_MATCHING PATTERN "*.h")
install (DIRECTORY ../include/warabi
         DESTINATION include
         FILES_MATCHING PATTERN "*.hpp")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/warabi-server.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/warabi-client.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/warabi-c-server.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/warabi-c-client.pc"
         DESTINATION "lib/pkgconfig/")
