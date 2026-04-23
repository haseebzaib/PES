

add_library(pes_utils
  file/file_reader.cpp
  file/json_file.cpp
)

target_include_directories(pes_utils
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_compile_features(pes_utils PUBLIC cxx_std_20)

target_compile_options(pes_utils
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

target_link_libraries(pes_utils
    PUBLIC
        spdlog::spdlog_header_only
)
