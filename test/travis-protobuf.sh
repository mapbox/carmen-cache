#!/bin/bash

set -e

build_dir="$(pwd)"

export PATH="/usr/local/bin:$PATH"
export CXXFLAGS="$CXXFLAGS -fPIC"

PROTOBUF_VERSION=2.6.1
wget https://github.com/google/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-${PROTOBUF_VERSION}.tar.bz2 -O /tmp/protobuf-${PROTOBUF_VERSION}.tar.bz2
tar xf /tmp/protobuf-${PROTOBUF_VERSION}.tar.bz2 -C /tmp
cd /tmp/protobuf-${PROTOBUF_VERSION}
./configure --enable-static --disable-shared --disable-dependency-tracking
make -j8
make install
cd $build_dir
