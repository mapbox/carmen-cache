[![Build Status](https://travis-ci.org/mapbox/carmen-cache.svg)](https://travis-ci.org/mapbox/carmen-cache)
[![Coverity](https://scan.coverity.com/projects/5667/badge.svg)](https://scan.coverity.com/projects/5667)
[![codecov](https://codecov.io/gh/mapbox/carmen-cache/branch/master/graph/badge.svg)](https://codecov.io/gh/mapbox/carmen-cache)

carmen-cache
------------
Protobuf-based cache. Written originally for use in [carmen](https://github.com/mapbox/carmen) geocoder.

### Install

To install `carmen-cache` run:

```
npm install
```

By default, binaries are provided for `64 bit OS X >= 10.8` and `64 bit Linux (>= Ubuntu Trusty)`. On those platforms no external dependencies are needed.

Other platforms will fall back to a source compile: see [Source Build](#Source Build) for details

### Source Build

To build from source run:

```
make
```

This will automatically:

  - Install [mason](https://github.com/mapbox/mason/) locally
  - Install several C/C++ dependencies locally (in `mason_packages`) via mason: bzip, rocksdb, protozero, and the clang++ compiler
  
To do a full rebuild run: `make clean`

### Publishing

See [CONTRIBUTING](CONTRIBUTING.md) for how to release a new carmen-cache version.

### Coalesce docs (incomplete)

![coalescemulti](https://cloud.githubusercontent.com/assets/83384/21327650/3588be54-c5fe-11e6-894e-cdaa68ecfa5f.jpg)
