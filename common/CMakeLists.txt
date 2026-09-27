add_library(common INTERFACE)

target_include_directories(common INTERFACE 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${ASIO_INCLUDE_DIR}
)

target_compile_definitions(common INTERFACE ASIO_STANDALONE)

find_package(Threads REQUIRED)
target_link_libraries(common INTERFACE Threads::Threads)