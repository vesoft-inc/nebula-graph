nebula_add_library(
    planner_obj OBJECT
    ExecutionPlan.cpp
    Query.cpp
    IdGenerator.cpp
)

add_subdirectory(test)
