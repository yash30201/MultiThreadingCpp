cmake_minimum_required(VERSION 3.24)
project(MultiThreadingCpp)

set(CMAKE_CXX_STANDARD 17)

add_executable(MultiThreadingCpp main.cpp producer_consumer.h rate_limiter.h)
