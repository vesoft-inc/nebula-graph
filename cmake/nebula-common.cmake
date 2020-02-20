include(ExternalProject)
ExternalProject_Add(
    common
    PREFIX modules
    SOURCE_DIR modules/common
#GIT_REPOSITORY git@github.com:vesoft-inc-private/nebula-common.git
    GIT_REPOSITORY https://github.com/CPWstatic/nebula-common.git
    GIT_SHALLOW true
    GIT_PROGRESS true
    GIT_TAG master
    CMAKE_ARGS
        -DNEBULA_THIRDPARTY_ROOT=${NEBULA_THIRDPARTY_ROOT}
        -DNEBULA_OTHER_ROOT=${NEBULA_OTHER_ROOT}
        -DENABLE_JEMALLOC=${ENABLE_JEMALLOC}
        -DENABLE_NATIVE=${ENABLE_NATIVE}
        -DENABLE_CCACHE=${ENABLE_CCACHE}
        -DENABLE_TESTING=false
    INSTALL_COMMAND ""
    BUILD_IN_SOURCE true
)

add_custom_target(nebula-common DEPENDS "modules/common")
