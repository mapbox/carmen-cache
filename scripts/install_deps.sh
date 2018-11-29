#!/bin/bash

set -eu
set -o pipefail

# gyp will put "MAKEFLAGS=r -- BUILDTYPE=Release" into the makefiles
# which breaks the rocksdb build
unset MAKEFLAGS

# setup mason
./scripts/setup.sh --config local.env
source local.env

# avoid mis-reporting of CPU due to docker
# from resulting in OOM killer knocking out g++
export MASON_CONCURRENCY=2

mason install clang++ 3.9.1
mason install bzip2 1.0.6
mason install rocksdb 5.4.6
mason install protozero 1.6.2

mason link bzip2 1.0.6
mason link protozero 1.6.2
mason link rocksdb 5.4.6
