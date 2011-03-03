#!/bin/sh

tests=testsuite

while [ -n "$1" ]; do
    if [ "$1" = "-t" ]; then
        shift
        tests="$1"
    else
        cass_dir="$1"
    fi
    shift
done

[ -n "$cass_dir" ] && [ -e "$cass_dir/test/system" ] || {
    echo "Run with the cassandra tree toplevel dir as an argument." >&2
    exit 1
}

PYTHONPATH="$cass_dir/test/system" nosetests --tests="$tests"
