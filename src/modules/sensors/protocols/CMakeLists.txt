find_package(Boost REQUIRED COMPONENTS system)

add_library(pes_protocols
    serial/serial_port.cpp
)

target_include_directories(pes_protocols
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_compile_features(pes_protocols PUBLIC cxx_std_20)

target_link_libraries(pes_protocols
    PUBLIC
        Boost::system
        spdlog::spdlog_header_only
)