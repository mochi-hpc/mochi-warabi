add_executable (AdminTest AdminTest.cpp)
target_link_libraries (AdminTest PRIVATE Catch2::Catch2WithMain warabi-server warabi-admin fmt::fmt)
add_test (NAME AdminTest COMMAND timeout 60s ./AdminTest)

add_executable (ClientTest ClientTest.cpp)
target_link_libraries (ClientTest PRIVATE Catch2::Catch2WithMain warabi-server warabi-client warabi-admin fmt::fmt)
add_test (NAME ClientTest COMMAND timeout 60s ./ClientTest)

add_executable (TargetTest TargetTest.cpp)
target_link_libraries (TargetTest PRIVATE Catch2::Catch2WithMain warabi-server warabi-client warabi-admin fmt::fmt)
add_test (NAME TargetTest COMMAND timeout 60s ./TargetTest)

add_executable (ProviderTest ProviderTest.cpp)
target_link_libraries (ProviderTest PRIVATE Catch2::Catch2WithMain warabi-server warabi-admin fmt::fmt)
add_test (NAME ProviderTest COMMAND timeout 60s ./ProviderTest)

