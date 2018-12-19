#!/bin/bash

set -eu
set -o pipefail

# setup mason
./scripts/setup.sh --config local.env
source local.env

mason install bzip2 1.0.6
mason install rocksdb 5.4.6
mason install protozero 1.6.2

mason link bzip2 1.0.6
mason link protozero 1.6.2
mason link rocksdb 5.4.6
