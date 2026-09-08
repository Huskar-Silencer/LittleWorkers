cmake_minimum_required(VERSION 4.4)
project(LittleWorkers)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(LittleWorkers STATIC little_workers.cpp)
target_include_directories(LittleWorkers PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

enable_testing()

add_executable(test_orders tests/test_orders.cpp)
target_link_libraries(test_orders PRIVATE LittleWorkers)

add_executable(test_http_server tests/test_http_server.cpp)
target_link_libraries(test_http_server PRIVATE LittleWorkers)

add_executable(test_pipeline tests/test_pipeline.cpp)
target_link_libraries(test_pipeline PRIVATE LittleWorkers)

add_executable(test_scaling tests/test_scaling.cpp)
target_link_libraries(test_scaling PRIVATE LittleWorkers)

add_executable(test_post tests/test_post.cpp)
target_link_libraries(test_post PRIVATE LittleWorkers)

add_executable(test_group tests/test_group.cpp)
target_link_libraries(test_group PRIVATE LittleWorkers)

add_executable(test_group_business tests/test_group_business.cpp)
target_link_libraries(test_group_business PRIVATE LittleWorkers)

add_executable(test_lifecycle tests/test_lifecycle.cpp)
target_link_libraries(test_lifecycle PRIVATE LittleWorkers)

add_test(NAME orders COMMAND test_orders)
add_test(NAME http_server COMMAND test_http_server)
add_test(NAME pipeline COMMAND test_pipeline)
add_test(NAME scaling COMMAND test_scaling)
add_test(NAME post COMMAND test_post)
add_test(NAME group COMMAND test_group)
add_test(NAME group_business COMMAND test_group_business)
add_test(NAME lifecycle COMMAND test_lifecycle)
