#!/bin/bash

if [ ! -d ./mason ]; then
    git clone --branch v0.5.0 --single-branch https://github.com/mapbox/mason.git
    ./mason/mason install bzip2 1.0.6
    ./mason/mason link bzip2 1.0.6
    ./mason/mason install rocksdb 4.13
    ./mason/mason link rocksdb 4.13

    ./mason/mason install clang++ 3.9.1
    ./mason/mason link clang++ 3.9.1
fi
