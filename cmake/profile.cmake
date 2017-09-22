set(ENABLE_GCOV_DEFAULT OFF)
option(ENABLE_GCOV "Enable integration with gcov, a code coverage program" ${ENABLE_GCOV_DEFAULT})

if (ENABLE_GCOV)
    add_compile_flags("C;CXX"
        "-fprofile-arcs"
        "-ftest-coverage"
    )

    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -ftest-coverage")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fprofile-arcs")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -ftest-coverage")
endif()

if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(ENABLE_GPROF_DEFAULT ON)
else()
    set(ENABLE_GPROF_DEFAULT OFF)
endif()
option(ENABLE_GPROF "Enable integration with gprof, a performance analyzing tool" ${GPROF_DEFAULT})

if (ENABLE_GPROF)
    add_compile_flags("C;CXX" "-pg")
endif()

option(ENABLE_VALGRIND "Enable integration with valgrind, a memory analyzing tool" OFF)
if (ENABLE_VALGRIND)
    add_definitions(-UNVALGRIND)
else()
    add_definitions(-DNVALGRIND=1)
endif()

option(ENABLE_ASAN "Enable AddressSanitizer, a fast memory error detector based on compiler instrumentation" OFF)
if (ENABLE_ASAN)
    add_compile_flags("C;CXX" -fsanitize=address)
endif()
