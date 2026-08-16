cmake_minimum_required(VERSION 3.16)
project(dfs CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
add_compile_options(-O2 -Wall -Wextra)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(XXHASH REQUIRED IMPORTED_TARGET libxxhash)

add_library(dfs_common STATIC common/net.cpp)
target_include_directories(dfs_common PUBLIC ${CMAKE_SOURCE_DIR})

add_executable(dfs_coordinator coordinator/main.cpp coordinator/coordinator.cpp)
target_link_libraries(dfs_coordinator dfs_common Threads::Threads PkgConfig::XXHASH)

add_executable(dfs_node node/main.cpp node/node.cpp)
target_link_libraries(dfs_node dfs_common Threads::Threads PkgConfig::XXHASH)

add_executable(dfs_client client/main.cpp client/client.cpp)
target_link_libraries(dfs_client dfs_common Threads::Threads PkgConfig::XXHASH)
