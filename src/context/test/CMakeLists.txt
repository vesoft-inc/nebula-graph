nebula_add_test(
    NAME query_context_test
    SOURCES QueryContextTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:context_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
    LIBRARIES gtest
)

