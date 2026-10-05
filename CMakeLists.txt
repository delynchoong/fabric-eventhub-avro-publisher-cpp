cmake_minimum_required(VERSION 3.20)
project(eventhub_avro_publisher LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(WIN32 AND NOT TARGET avro-cpp::avrocpp_s)
    add_library(avro-cpp::avrocpp_s INTERFACE IMPORTED)
endif()

find_package(avro-cpp CONFIG REQUIRED)
find_package(azure-identity-cpp CONFIG REQUIRED)
find_package(azure-messaging-eventhubs-cpp CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)

add_executable(eventhub_avro_publisher eventhub_avro_publisher.cpp)
add_executable(
    eventhub_avro_map_publisher
    eventhub_avro_map_publisher.cpp
)

foreach(target IN ITEMS
    eventhub_avro_publisher
    eventhub_avro_map_publisher)
    if(MSVC)
        target_compile_options(${target} PRIVATE /utf-8 /W4 /permissive-)
    else()
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
    endif()
    target_link_libraries(
        ${target}
        PRIVATE
            avro-cpp::avrocpp
            Azure::azure-identity
            Azure::azure-messaging-eventhubs
            fmt::fmt)
endforeach()
