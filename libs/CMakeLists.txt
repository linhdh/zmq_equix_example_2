add_library(kvmsg
        src/kvmsg.cpp)
target_include_directories(kvmsg PUBLIC include)

add_library(bstar
        src/bstar.cpp)
target_include_directories(bstar PUBLIC include)

add_library(clone
        src/clone.cpp)
target_include_directories(clone PUBLIC include)
target_link_libraries(clone zmq zmqpp)