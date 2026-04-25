add_library(pes_protocols
    modbus/modbus_client.cpp
    serial/serial_port.cpp
    third_party/libserial/SerialPort.cpp
)

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(LIBMODBUS QUIET IMPORTED_TARGET libmodbus)
endif()

if(NOT TARGET PkgConfig::LIBMODBUS)
    find_path(LIBMODBUS_INCLUDE_DIR
        NAMES modbus/modbus.h modbus.h
    )
    find_library(LIBMODBUS_LIBRARY
        NAMES modbus
    )

    if(NOT LIBMODBUS_INCLUDE_DIR OR NOT LIBMODBUS_LIBRARY)
        message(FATAL_ERROR "libmodbus was not found. Install libmodbus-dev or provide LIBMODBUS_INCLUDE_DIR and LIBMODBUS_LIBRARY.")
    endif()
endif()

target_include_directories(pes_protocols
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

if(NOT TARGET PkgConfig::LIBMODBUS)
    target_include_directories(pes_protocols
        PUBLIC
            ${LIBMODBUS_INCLUDE_DIR}
    )
endif()

target_compile_features(pes_protocols PUBLIC cxx_std_20)

target_link_libraries(pes_protocols
    PUBLIC
        spdlog::spdlog_header_only
)

if(TARGET PkgConfig::LIBMODBUS)
    target_link_libraries(pes_protocols
        PUBLIC
            PkgConfig::LIBMODBUS
    )
else()
    target_link_libraries(pes_protocols
        PUBLIC
            ${LIBMODBUS_LIBRARY}
    )
endif()
