#! /usr/bin/env bash
set -e
hash xz &> /dev/null || { echo "xz: Command not found"; exit 1; }
[[ $# -ne 0 ]] && prefix=$(echo "$@" | sed 's;.*--prefix=(\S*).*;\1;p' -rn)
prefix=${prefix:-/usr/local/nebula}
mkdir -p $prefix
[[ -w $prefix ]] || { echo "$prefix: No permission to write"; exit 1; }
archive_offset=$(awk '/^__start_of_archive__$/{print NR+1; exit 0;}' $0)
tail -n+$archive_offset $0 | tar --no-same-owner --numeric-owner -xJf - -C $prefix
daemons=(metad graphd storaged)
for daemon in ${daemons[@]}
do
    if [[ ! -f $prefix/etc/nebula-$daemon.conf ]] && [[ -f $prefix/etc/nebula-$daemon.conf.default ]]; then
        cp $prefix/etc/nebula-$daemon.conf.default $prefix/etc/nebula-$daemon.conf
        chmod 644 $prefix/etc/nebula-$daemon.conf
    fi
done
echo "Nebula Graph has been installed to $prefix"
exit 0
__start_of_archive__
