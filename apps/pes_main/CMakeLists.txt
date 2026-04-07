add_executable(pes_main
    main.cpp
)

target_link_libraries(pes_main
    PRIVATE
        pes_core
)