cmake_minimum_required(VERSION 3.16)
project(sum_grid LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Путь к Asio
set(ASIO_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libs/asio/asio/include)
if(NOT EXISTS ${ASIO_INCLUDE_DIR})
    # Запасной путь
    set(ASIO_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libs/asio/include)
endif()

add_subdirectory(common)
add_subdirectory(server)
add_subdirectory(client)