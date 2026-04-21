add_library(pes_drivers
    dustrak/drx_85xx.cpp
)

target_include_directories(pes_drivers
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_compile_features(pes_drivers PUBLIC cxx_std_20)

target_link_libraries(pes_drivers
    PUBLIC
        pes_protocols
        spdlog::spdlog_header_only
)
