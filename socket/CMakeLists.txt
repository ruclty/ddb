cmake_minimum_required(VERSION 3.15)
add_definitions(-std=c++14)
set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-pthread ")
project(socket)


set(CUSTOM_INC /home/dyj/CLionProjects/socket)


include_directories(${CUSTOM_INC})
set(CMAKE_CXX_STANDARD 14)

add_executable(socket main.cpp rpc_receive.cpp rpc_receive.h rpc_sent.cpp rpc_sent.h)


