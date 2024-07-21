cmake_minimum_required(VERSION 3.27)
project(aslam)

find_package(Threads REQUIRED)
find_package(spdlog REQUIRED)

set(CMAKE_CXX_FLAGS "-O3")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(aslam src/main.cpp
        src/MemTable.cpp
        src/SkipList.cpp
        src/LSMStore.cpp
        src/LSMStore.h
        src/LSMCommon.h
        src/MergeIterator.h
)

target_link_libraries(aslam PRIVATE Threads::Threads spdlog::spdlog)
