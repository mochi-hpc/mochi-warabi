
file (GLOB test-sources ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
foreach (test-source ${test-sources})
    get_filename_component (test-target ${test-source} NAME_WE)
    # Skip Migration tests if REMI is not enabled
    if (${test-target} MATCHES "Migration" AND NOT ${ENABLE_REMI})
        message (STATUS "Skipping ${test-target} (ENABLE_REMI is OFF)")
        continue()
    endif ()
    add_executable (${test-target} ${test-source})
    target_link_libraries (${test-target} PRIVATE
        Catch2::Catch2WithMain warabi-server warabi-client
        warabi-c-server warabi-c-client fmt::fmt)
    add_test (NAME ${test-target} COMMAND timeout 60s ./${test-target})
endforeach ()

# Set the path to the python directory
set (PYTHON_MODULE_DIR ${CMAKE_SOURCE_DIR}/python)
# Use file(GLOB_RECURSE ...) to find all files matching the test_*.py pattern
file (GLOB_RECURSE PYTHON_TEST_FILES "${PYTHON_MODULE_DIR}/test_*.py")

foreach (PYTHON_TEST_FILE ${PYTHON_TEST_FILES})
    # Remove the directory part
    file (RELATIVE_PATH PYTHON_TEST_FILE_REL ${PYTHON_MODULE_DIR} ${PYTHON_TEST_FILE})
    # Remove the file extension
    string (REPLACE ".py" "" PYTHON_TEST_FILE_BASE ${PYTHON_TEST_FILE_REL})
    # Replace slashes with dots
    string (REPLACE "/" "." PYTHON_TEST_NAME ${PYTHON_TEST_FILE_BASE})
    # Add the test
    if (${ENABLE_COVERAGE})
        message (STATUS "${PYTHON_TEST_NAME} test will run with code coverage")
        add_test (NAME ${PYTHON_TEST_NAME} COMMAND coverage run -a -m unittest ${PYTHON_TEST_NAME})
    else ()
        add_test (NAME ${PYTHON_TEST_NAME} COMMAND python -m unittest ${PYTHON_TEST_NAME})
    endif ()
    set_property (TEST ${PYTHON_TEST_NAME} PROPERTY ENVIRONMENT
                  PYTHONPATH=${CMAKE_SOURCE_DIR}/python/:${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH})
endforeach()
