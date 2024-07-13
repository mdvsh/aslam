cmake_minimum_required(VERSION 3.27)
project(aslam)

set(CMAKE_CXX_FLAGS "-O3")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(aslam src/main.cpp
        src/MemTable.cpp
        src/SkipList.cpp
)
