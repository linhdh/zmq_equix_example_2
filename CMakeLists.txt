cmake_minimum_required(VERSION 4.0)

project(zmq_equix_example_2 VERSION 1.0 DESCRIPTION "Equix example 2" LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(libs)

add_executable(clonesrv6
        src/clonesrv6.cpp)
target_link_libraries(clonesrv6 zmq zmqpp bstar clone kvmsg)
target_include_directories(clonesrv6 PRIVATE include)

add_executable(pubcli6
        src/pubcli6.cpp)
target_include_directories(pubcli6 PRIVATE include)

add_executable(subcli6
        src/subcli6.cpp)
target_include_directories(subcli6 PRIVATE include)
