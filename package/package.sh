#!/usr/bin/env bash
#
#  Package nebula as deb/rpm package
#
# introduce the args
#   -v: The version of package, the version should be match tag name, default value is the `commitId`
#   -n: Package to one or multi packages, `ON` means one package, `OFF` means multi packages, default value is `ON`
#   -s: Whether to strip the package, default value is `FALSE`
#   -g: Whether build storage, default is ON
#   -b: The branch of nebula-common and nebula-storage
#   -t: The build type, values `Debug, Release, RelWithDebInfo, MinSizeRel`, default value is Release
#   -f: Whether to package to tar file or rpm/deb file, default is `auto`, it means package to rpm/deb, `tar` means package to tar, `all` means package to tar and  rpm/deb
#
# usage: ./package.sh -v <version> -n <ON/OFF> -s <TRUE/FALSE> -b <BRANCH> -g <ON/OFF> -t <Debug/Release/RelWithDebInfo/MinSizeRel> -f <auto/tar/all>
#

set -e

version=""
build_storage=ON
package_one=ON
strip_enable="FALSE"
usage="Usage: ${0} -v <version> -n <ON/OFF> -s <TRUE/FALSE> -b <BRANCH> -g <ON/OFF> -t <Debug/Release/RelWithDebInfo/MinSizeRel> -f <auto/tar>"
project_dir="$(cd "$(dirname "$0")" && pwd)/.."
build_dir=${project_dir}/pkg-build
package_dir=${build_dir}/package/
modules_dir=${project_dir}/modules
storage_dir=${modules_dir}/storage
storage_build_dir=${build_dir}/modules/storage
enablesanitizer="OFF"
static_sanitizer="OFF"
build_type="Release"
branch="master"
jobs=$(nproc)
jobs=20
package_type="auto"
install_dir="/usr/local/nebula"

while getopts v:n:s:b:d:t:g:f: opt;
do
    case $opt in
        v)
            version=$OPTARG
            ;;
        n)
            package_one=$OPTARG
            ;;
        s)
            strip_enable=$OPTARG
            ;;
        b)
            branch=$OPTARG
            ;;
        d)
            enablesanitizer="ON"
            if [ "$OPTARG" == "static" ]; then
                static_sanitizer="ON"
            fi
            build_type="RelWithDebInfo"
            ;;
        t)
            build_type=$OPTARG
            ;;
        g)
            build_storage=$OPTARG
            ;;
        f)
            package_type=$OPTARG
            ;;
        ?)
            echo "Invalid option, use default arguments"
            ;;
    esac
done

# version is null, get from tag name
[[ -z $version ]] && version=$(git describe --exact-match --abbrev=0 --tags | sed 's/^v//')
# version is null, use UTC date as version
[[ -z $version ]] && version=$(date -u +%Y.%m.%d)-nightly

if [[ -z $version ]]; then
    echo "version is null, exit"
    echo ${usage}
    exit 1
fi


if [[ $strip_enable != TRUE ]] && [[ $strip_enable != FALSE ]]; then
    echo "strip enable is wrong, exit"
    echo ${usage}
    exit 1
fi

if [[ $package_type != "auto" ]] && [[ $package_type != "tar" ]] && [[ $package_type != "all" ]]; then
    echo "package type[$package_type] is wrong, should be [auto/tar/all]. exit"
    echo ${usage}
    exit -1
fi

echo ">>>>>>>> option <<<<<<<<
version          : [$version]
strip_enable     : [$strip_enable]
enablesanitizer  : [$enablesanitizer]
static_sanitizer : [$static_sanitizer]
build_type       : [$build_type]
branch           : [$branch]
package_one      : [$package_one]
build_storage    : [$build_storage]
package_type   : [$package_type]
>>>>>>>> option <<<<<<<<"


function _build_storage {
    if [[ ! -d ${storage_dir} && ! -L ${storage_dir} ]]; then
        git clone --single-branch --branch ${branch} https://github.com/vesoft-inc/nebula-storage.git ${storage_dir}
    fi

    pushd ${storage_build_dir}
    cmake -DCMAKE_BUILD_TYPE=${build_type} \
          -DNEBULA_BUILD_VERSION=${version} \
          -DENABLE_ASAN=${enablesanitizer} \
          -DENABLE_UBSAN=${enablesanitizer} \
          -DENABLE_STATIC_ASAN=${static_sanitizer} \
          -DENABLE_STATIC_UBSAN=${static_sanitizer} \
          -DCMAKE_INSTALL_PREFIX=${install_dir} \
          -DNEBULA_COMMON_REPO_TAG=${branch} \
          -DENABLE_TESTING=OFF \
          -DENABLE_PACK_ONE=${package_one} \
          ${storage_dir}

    if ! ( make -j ${jobs} ); then
        echo ">>> build nebula storage failed <<<"
        exit 1
    fi
    popd
    echo ">>> build nebula storage successfully <<<"
}

function _build_graph {
    pushd ${build_dir}
    cmake -DCMAKE_BUILD_TYPE=${build_type} \
          -DNEBULA_BUILD_VERSION=${version} \
          -DENABLE_ASAN=${enablesanitizer} \
          -DENABLE_UBSAN=${enablesanitizer} \
          -DENABLE_STATIC_ASAN=${static_sanitizer} \
          -DENABLE_STATIC_UBSAN=${static_sanitizer} \
          -DCMAKE_INSTALL_PREFIX=${install_dir} \
          -DNEBULA_COMMON_REPO_TAG=${branch} \
          -DENABLE_TESTING=OFF \
          -DENABLE_BUILD_STORAGE=OFF \
          -DENABLE_PACK_ONE=${package_one} \
          ${project_dir}

    if ! ( make -j ${jobs} ); then
        echo ">>> build nebula graph failed <<<"
        exit 1
    fi
    popd
    echo ">>> build nebula graph successfully <<<"
}

# args: <version>
function build {
    rm -rf ${build_dir} && mkdir -p ${build_dir}

    if [[ "$build_storage" == "ON" ]]; then
        mkdir -p ${storage_build_dir}
        _build_storage
    fi
    _build_graph
}

function package_cmake {
    package_to_tar="OFF"
    if [[ $package_type == "tar" ]] || [[ $package_type == "all" ]]; then
        package_to_tar="ON"
    fi
    cmake \
        -DNEBULA_BUILD_VERSION=${version} \
        -DENABLE_PACK_ONE=${package_one} \
        -DCMAKE_INSTALL_PREFIX=${install_dir} \
        -DENABLE_PACKAGE_STORAGE=${build_storage} \
        -DNEBULA_STORAGE_SOURCE_DIR=${storage_dir} \
        -DNEBULA_STORAGE_BINARY_DIR=${storage_build_dir} \
        -DNEBULA_TARGET_FILE_DIR=${package_dir} \
        -DENABLE_TO_PACKAGE_SH=${package_to_tar} \
        ${project_dir}/package/
}

function package {
    package_cmake
    args=""
    [[ $strip_enable == TRUE ]] && args="-D CPACK_STRIP_FILES=TRUE -D CPACK_RPM_SPEC_MORE_DEFINE="

    if ! ( cpack --verbose $args ); then
        echo ">>> package nebula failed <<<"
        exit 1
    fi
}

function package_tar {
    install_dir=${build_dir}/install
    package_cmake
    pushd ${package_dir}
        make install -j$(nproc)
    popd
    if ! ( make package-tar ); then
        echo ">>> package nebula to sh failed <<<"
        exit 1
    fi
}

function move_file {
    # rename package file
    outputDir=$build_dir/cpack_output
    mkdir -p ${outputDir}
    for pkg_name in $(ls ./*nebula*-${version}*); do
        mv ${pkg_name} ${outputDir}/
        echo "####### taget package file is ${outputDir}/${pkg_name}"
    done
}

# The main
build

# The package CMakeLists.txt in ${project_dir}/package/build
if [[ -d $package_dir ]]; then
    rm -rf ${package_dir:?}/*
else
    mkdir ${package_dir}
fi

pushd ${package_dir}
if [[ "$package_type" == "tar" ]]; then
    package_tar
elif [[ "$package_type" == "auto" ]]; then
    package
else
    package
    package_tar
fi
move_file
popd
