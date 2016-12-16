[![Build Status](https://travis-ci.org/mapbox/carmen-cache.svg)](https://travis-ci.org/mapbox/carmen-cache)
[![Coverity](https://scan.coverity.com/projects/5667/badge.svg)](https://scan.coverity.com/projects/5667)
[![Coverage Status](https://coveralls.io/repos/mapbox/carmen-cache/badge.svg)](https://coveralls.io/r/mapbox/carmen-cache)

carmen-cache
------------
Protobuf-based cache. Written originally for use in [carmen](https://github.com/mapbox/carmen) geocoder.

### Installing from source

First install mason and then (from inside the carmen-cache) directory, install rocksdb:

```
mason install bzip2 1.0.6
mason link bzip2 1.0.6
mason install rocksdb 4.13
mason link rocksdb 4.13
```

Then you can build carmen-cache from source like:

```
npm install --build-from-source
```