cmake_minimum_required(VERSION 3.26)
project(nuke_core)
find_package(Boost)
set(CMAKE_CXX_STANDARD 14)

add_executable(nuke_core
        tester.cpp)
target_link_libraries(nuke_core ${Boost_LIBRARIES})
