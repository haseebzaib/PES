add_library(pes_core
    application.cpp
)

target_include_directories(pes_core
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_compile_features(pes_core PUBLIC cxx_std_20)

# target_compile_options(pes_core
#     PRIVATE
#         -Wall
#         -Wextra
#         -Wpedantic
# )


target_link_libraries(pes_core
    PUBLIC
        spdlog::spdlog_header_only
)