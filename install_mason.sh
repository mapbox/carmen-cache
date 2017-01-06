#!/bin/bash

if [ ! -d ./mason ]; then
    # TODO: after https://github.com/mapbox/mason/pull/305 merges point at official mason release
    git clone --branch master --single-branch https://github.com/mapbox/mason.git
    ./mason/mason install bzip2 1.0.6
    ./mason/mason link bzip2 1.0.6
    ./mason/mason install rocksdb 4.13
    ./mason/mason link rocksdb 4.13
fi
