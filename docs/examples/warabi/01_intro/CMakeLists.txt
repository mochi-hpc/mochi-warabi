add_executable (01_warabi_server server.cpp)
target_link_libraries (01_warabi_server warabi::server warabi::client)

add_executable (01_warabi_client client.cpp)
target_link_libraries (01_warabi_client warabi::server warabi::client)
