add_executable (AdminTest AdminTest.cpp)
target_link_libraries (AdminTest PRIVATE Catch2::Catch2WithMain warabi-server warabi-admin)
add_test (NAME AdminTest COMMAND ./AdminTest)

add_executable (ClientTest ClientTest.cpp)
target_link_libraries (ClientTest PRIVATE Catch2::Catch2WithMain warabi-server warabi-client warabi-admin)
add_test (NAME ClientTest COMMAND ./ClientTest)

add_executable (TargetTest TargetTest.cpp)
target_link_libraries (TargetTest PRIVATE Catch2::Catch2WithMain warabi-server warabi-client warabi-admin)
add_test (NAME TargetTest COMMAND ./TargetTest)
