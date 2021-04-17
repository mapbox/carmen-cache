project (carmen-cache-vng)
cmake_minimum_required (VERSION 2.8)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(ROOT_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

file(GLOB_RECURSE SOURCES "src/*.cpp")
file(GLOB_RECURSE HEADERS "src/*.hpp")

include_directories(
    "${ROOT_DIR}/mason_packages/.link/include/"
    "${ROOT_DIR}/node_modules/nan/"
    "/Users/user/.node-gyp/8.10.0/include/node/"
)

link_directories("${ROOT_DIR}/mason_packages/.link/lib/")

add_library(carmen-cache-vng SHARED ${SOURCES} ${HEADERS})

target_link_libraries(carmen-cache-vng bz2 rocksdb tbb)
