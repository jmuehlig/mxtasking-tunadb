add_executable(print_core_set
    src/application/print_core_set/main.cpp
)
add_dependencies(print_core_set argparse-external)
target_link_libraries(print_core_set pthread numa atomic mxtasking)