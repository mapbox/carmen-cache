#!/bin/bash

set -eu
set -o pipefail

function install() {
  mason install $1 $2
  mason link $1 $2
}

# setup mason
./scripts/setup.sh --config local.env
source local.env

install bzip2 1.0.6
install rocksdb 5.3.6
install clang++ 3.9.1
install protozero 1.5.1