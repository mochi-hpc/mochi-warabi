
file (GLOB test-sources ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
foreach (test-source ${test-sources})
    get_filename_component (test-target ${test-source} NAME_WE)
    add_executable (${test-target} ${test-source})
    target_link_libraries (${test-target} PRIVATE
        Catch2::Catch2WithMain warabi-server warabi-client warabi-admin
        warabi-c-server warabi-c-client warabi-c-admin fmt::fmt)
    add_test (NAME ${test-target} COMMAND timeout 60s ./${test-target})
endforeach ()
