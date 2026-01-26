if (ENABLE_REMI)
  add_executable (08_warabi_migration migration_example.cpp)
  target_link_libraries (08_warabi_migration
    warabi::server
    warabi::client
    remi)
endif ()
