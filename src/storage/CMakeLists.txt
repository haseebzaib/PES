find_package(SQLite3 REQUIRED)

add_library(pes_storage
    ping_storage.cpp
    redis_storage.cpp
    sqlite_storage.cpp
)

target_include_directories(pes_storage
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_compile_features(pes_storage PUBLIC cxx_std_20)

target_compile_options(pes_storage
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

target_link_libraries(pes_storage
    PUBLIC
        SQLite::SQLite3
        spdlog::spdlog_header_only
)
