set(SOURCES execution_manager.cpp executor_explain.cpp)
add_library(execution STATIC ${SOURCES})

target_link_libraries(execution system record transaction planner)