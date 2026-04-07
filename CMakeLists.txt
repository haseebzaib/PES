cmake_minimum_required(VERSION 3.20)

project(
    PES
    VERSION 0.1.0
    DESCRIPTION "Processing Engine Software"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(PES_BUILD_TESTS "Build PES tests" ON)

add_subdirectory(src/core)
add_subdirectory(apps/pes_main)
add_subdirectory(third_party/spdlog)

if(PES_BUILD_TESTS AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/tests/CMakeLists.txt)
    enable_testing()
    add_subdirectory(tests)
endif()