# set source files
set (server-src-files
     Provider.cpp
     Backend.cpp)

set (client-src-files
     Client.cpp
     TargetHandle.cpp
     AsyncRequest.cpp)

set (admin-src-files
     Admin.cpp)

set (dummy-src-files
     dummy/DummyBackend.cpp)

set (module-src-files
     BedrockModule.cpp)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (warabi-pkg "share/cmake/warabi")

# set version fo shared library
set (warabi-vers "${WARABI_VERSION_MAJOR}.${WARABI_VERSION_MINOR}")

# server library
add_library (warabi-server ${server-src-files} ${dummy-src-files})
target_link_libraries (warabi-server
    PUBLIC thallium PkgConfig::uuid nlohmann_json::nlohmann_json
    PRIVATE spdlog::spdlog fmt::fmt coverage_config)
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
target_link_libraries (warabi-client
    PUBLIC thallium PkgConfig::uuid nlohmann_json::nlohmann_json
    PRIVATE spdlog::spdlog fmt::fmt coverage_config)
target_include_directories (warabi-client PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (warabi-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (warabi-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (warabi-client
    PROPERTIES VERSION ${WARABI_VERSION}
    SOVERSION ${WARABI_VERSION_MAJOR})

# admin library
add_library (warabi-admin ${admin-src-files})
target_link_libraries (warabi-admin PUBLIC thallium PkgConfig::uuid nlohmann_json::nlohmann_json
    PRIVATE spdlog::spdlog fmt::fmt coverage_config)
target_include_directories (warabi-admin PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (warabi-admin BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (warabi-admin BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (warabi-admin
    PROPERTIES VERSION ${WARABI_VERSION}
    SOVERSION ${WARABI_VERSION_MAJOR})

if (${ENABLE_BEDROCK})
# bedrock module library
add_library (warabi-bedrock-module ${module-src-files})
target_link_libraries (warabi-bedrock-module warabi-server warabi-admin warabi-client bedrock-server coverage_config)
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
set (ADMIN_PRIVATE_LIBS  "-lwarabi-admin")
configure_file ("warabi-server.pc.in" "warabi-server.pc" @ONLY)
configure_file ("warabi-client.pc.in" "warabi-client.pc" @ONLY)
configure_file ("warabi-admin.pc.in"  "warabi-admin.pc"  @ONLY)

# configure config.h
configure_file ("config.h.in" "config.h" @ONLY)

# "make install" rules
install (TARGETS warabi-admin warabi-server warabi-client
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
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/warabi-server.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/warabi-client.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/warabi-admin.pc"
         DESTINATION "lib/pkgconfig/")
