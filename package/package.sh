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
#   -f: Whether to package to tar file or rpm/deb file, default is `auto`, it means package to rpm/deb, `tar` means package to tar
#
# usage: ./package.sh -v <version> -n <ON/OFF> -s <TRUE/FALSE> -b <BRANCH> -g <ON/OFF> -t <Debug/Release/RelWithDebInfo/MinSizeRel> -f <auto/tar>
#

set -e

version=""
build_storage=ON
package_one=ON
strip_enable="FALSE"
usage="Usage: ${0} -v <version> -n <ON/OFF> -s <TRUE/FALSE> -b <BRANCH> -g <ON/OFF> -t <Debug/Release/RelWithDebInfo/MinSizeRel> -f <auto/tar>"
project_dir="$(cd "$(dirname "$0")" && pwd)/.."
build_dir=${project_dir}/pkg-build
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

if [[ $package_type != "auto" ]] && [[ $package_type != "tar" ]]; then
    echo "package type[$package_type] is wrong, exit"
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
package_type     : [$package_type]
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
    version=$1
    san=$2
    ssan=$3
    build_type=$4
    branch=$5

    rm -rf ${build_dir} && mkdir -p ${build_dir}

    if [[ "$build_storage" == "ON" ]]; then
        mkdir -p ${storage_build_dir}
        _build_storage
    fi
    _build_graph
}

function package {
    # The package CMakeLists.txt in ${project_dir}/package/build
    package_dir=${build_dir}/package/
    if [[ -d $package_dir ]]; then
        rm -rf ${package_dir:?}/*
    else
        mkdir ${package_dir}
    fi
    pushd ${package_dir}
    cmake \
        -DNEBULA_BUILD_VERSION=${version} \
        -DENABLE_PACK_ONE=${package_one} \
        -DCMAKE_INSTALL_PREFIX=/usr/local/nebula \
        -DENABLE_PACKAGE_STORAGE=${build_storage} \
        -DNEBULA_STORAGE_SOURCE_DIR=${storage_dir} \
        -DNEBULA_STORAGE_BINARY_DIR=${storage_build_dir} \
        ${project_dir}/package/

    args=""
    [[ $strip_enable == TRUE ]] && args="-D CPACK_STRIP_FILES=TRUE -D CPACK_RPM_SPEC_MORE_DEFINE="

    if ! ( cpack --verbose $args ); then
        echo ">>> package nebula failed <<<"
        exit 1
    else
        # rename package file
        outputDir=$build_dir/cpack_output
        mkdir -p ${outputDir}
        for pkg_name in $(ls ./*nebula*-${version}*); do
            mv ${pkg_name} ${outputDir}/
            echo "####### taget package file is ${outputDir}/${pkg_name}"
        done
    fi

    popd
}

function gen_package_name {
    if [[ -f "/etc/redhat-release" ]]; then
        sys_name=`cat /etc/redhat-release | cut -d ' ' -f1`
        if [[ ${sys_name} == "CentOS" ]]; then
            sys_ver=`cat /etc/redhat-release | tr -dc '0-9.' | cut -d \. -f1`
            if [[ ${sys_ver} == 7 ]] || [[ ${sys_ver} == 6 ]]; then
                package_name=.el${sys_ver}.$(uname -m)
            else
                package_name=.el${sys_ver}.$(uname -m)
            fi
        elif [[ ${sys_name} == "Fedora" ]]; then
            sys_ver=`cat /etc/redhat-release | cut -d ' ' -f3`
            package_name=.fc${sys_ver}.$(uname -m)
        fi
    elif [[ -f "/etc/lsb-release" ]]; then
        sys_name=`cat /etc/lsb-release | grep DISTRIB_RELEASE | cut -d "=" -f 2 | sed 's/\.//'`
        package_name=.ubuntu${sys_name}.$(uname -m)
    elif [[ -f "/etc/issue" ]]; then
        sys_name=`cat /etc/issue | cut -d " " -f 3`
        package_name=.debian${sys_name}.$(uname -m)
    fi
}

function package_tar_sh {
    gen_package_name
    exec_file=$build_dir/nebula-$version$package_name.sh

    echo "Creating self-extractable package $exec_file"
    cat > $exec_file <<EOF
#! /usr/bin/env bash
set -e
hash xz &> /dev/null || { echo "xz: Command not found"; exit 1; }
[[ \$# -ne 0 ]] && prefix=\$(echo "\$@" | sed 's;.*--prefix=(\S*).*;\1;p' -rn)
prefix=\${prefix:-/usr/local/nebula}
mkdir -p \$prefix
[[ -w \$prefix ]] || { echo "\$prefix: No permission to write"; exit 1; }
archive_offset=\$(awk '/^__start_of_archive__$/{print NR+1; exit 0;}' \$0)
tail -n+\$archive_offset \$0 | tar --no-same-owner --numeric-owner -xJf - -C \$prefix
daemons=(metad graphd storaged)
for daemon in \${daemons[@]}
do
    if [[ ! -f \$prefix/etc/nebula-\$daemon.conf ]] && [[ -f \$prefix/etc/nebula-\$daemon.conf.default ]]; then
        cp \$prefix/etc/nebula-\$daemon.conf.default \$prefix/etc/nebula-\$daemon.conf
        chmod 644 \$prefix/etc/nebula-\$daemon.conf
    fi
done
echo "Nebula Graph has been installed to \$prefix"
exit 0
__start_of_archive__
EOF
    pushd $install_dir
    tar -cJf - * >> $exec_file
    chmod 0755 $exec_file
    echo "####### target package file is $exec_file"
    popd
}

# The main
if [[ $package_type == "auto" ]]; then
    build
    package
else
    install_dir=${build_dir}/install
    build $version $enablesanitizer $static_sanitizer $buildtype
    pushd ${build_dir}
        make install-all -j$(nproc)
    popd
    package_tar_sh
fi

