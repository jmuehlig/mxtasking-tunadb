add_executable(hello_world src/application/hello_world/main.cpp)
target_link_libraries(hello_world pthread numa atomic mxtasking mxbenchmarking)