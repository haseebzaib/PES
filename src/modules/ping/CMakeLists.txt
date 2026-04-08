find_package(Boost REQUIRED COMPONENTS system)

add_library(pes_ping
    ping.cpp
)

target_include_directories(pes_ping
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_compile_features(pes_ping PUBLIC cxx_std_20)

target_link_libraries(pes_ping
    PUBLIC
        Boost::system
        spdlog::spdlog_header_only
)