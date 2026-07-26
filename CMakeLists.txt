# CMakeLists.txt
cmake_minimum_required(VERSION 3.16)
project(NanoDB CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Release build = full optimizations
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS_RELEASE "-O2")
set(CMAKE_CXX_FLAGS_DEBUG "-g -fsanitize=address,thread")

# ─────────────────────────────────────────────
# nanodb_cli  — original interactive CLI
# ─────────────────────────────────────────────
add_executable(nanodb_cli
    src/main.cpp
)
target_include_directories(nanodb_cli PRIVATE src)

# ─────────────────────────────────────────────
# nanodb_server  — TCP server (new)
# ─────────────────────────────────────────────
add_executable(nanodb_server
    src/server.cpp
)
target_include_directories(nanodb_server PRIVATE src)

# Link pthread for both targets (needed for sharded mutexes)
find_package(Threads REQUIRED)
target_link_libraries(nanodb_cli Threads::Threads)
target_link_libraries(nanodb_server Threads::Threads)

# ─────────────────────────────────────────────
# benchmark  — ops/sec + latency measurement
# ─────────────────────────────────────────────
add_executable(nanodb_bench
    src/bench.cpp
)
target_include_directories(nanodb_bench PRIVATE src)
target_link_libraries(nanodb_bench Threads::Threads)

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Targets: nanodb_cli, nanodb_server, nanodb_bench")