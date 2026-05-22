cmake_minimum_required(VERSION 3.16)
project(kvdb VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

option(KVDB_BUILD_TESTS "Build tests" ON)
option(KVDB_BUILD_FUZZ "Build fuzz test (not run automatically)" OFF)

add_library(kvdb_lib STATIC
    src/memtable.cpp
    src/sstable.cpp
    src/engine.cpp
    src/wal.cpp
    src/manifest.cpp
    src/protocol.cpp
    src/server.cpp
    src/client.cpp
    src/iterator.cpp
)

target_include_directories(kvdb_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(WIN32)
    target_link_libraries(kvdb_lib PUBLIC ws2_32)
endif()

if(KVDB_BUILD_TESTS)
    enable_testing()

    function(add_kvdb_test TEST_NAME TEST_SOURCE)
        add_executable(${TEST_NAME} ${TEST_SOURCE})
        target_link_libraries(${TEST_NAME} PRIVATE kvdb_lib)
        target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
        add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
    endfunction()

    add_kvdb_test(test_memtable tests/test_memtable.cpp)
    add_kvdb_test(test_sstable tests/test_sstable.cpp)
    add_kvdb_test(test_engine tests/test_engine.cpp)
    add_kvdb_test(test_wal tests/test_wal.cpp)
    add_kvdb_test(test_manifest tests/test_manifest.cpp)
    add_kvdb_test(test_server tests/test_server.cpp)
    add_kvdb_test(test_wfw tests/test_wfw.cpp)
    add_kvdb_test(test_small tests/test_small.cpp)
    add_kvdb_test(test_restart tests/test_restart.cpp)
endif()

if(KVDB_BUILD_FUZZ)
    add_executable(test_fuzz tests/test_fuzz.cpp)
    target_link_libraries(test_fuzz PRIVATE kvdb_lib)
    target_include_directories(test_fuzz PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    set_target_properties(test_fuzz PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
endif()
