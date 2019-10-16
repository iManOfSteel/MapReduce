cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_COMPILER mpic++)
set(CMAKE_C_COMPILER mpicc)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_FLAGS "-std=c++11")

project(MapReduce)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${MapReduce_SOURCE_DIR}/bin)

add_executable(MapReduce main.cxx Reader.h Reducer.h Mapper.h MapReduce.h Shuffler.h)
