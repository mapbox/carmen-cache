[![Build Status](https://travis-ci.org/mapbox/carmen-cache.svg?branch=master)](https://travis-ci.org/mapbox/carmen-cache)
[![Coverity](https://scan.coverity.com/projects/5667/badge.svg)](https://scan.coverity.com/projects/5667)
[![codecov](https://codecov.io/gh/mapbox/carmen-cache/branch/master/graph/badge.svg)](https://codecov.io/gh/mapbox/carmen-cache)

carmen-cache
------------

`carmen-cache` is a low-level storage layer used in the [carmen](https://github.com/mapbox/carmen) geocoder.

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

### What `carmen-cache` does

When doing a forward (text -> coordinates) geocode, carmen goes through a few steps. The [carmen README](https://github.com/mapbox/carmen/blob/master/README.md) has a full rundown, but abbreviated for purposes of this module, `carmen` does approximately as follows:
* break the query up into constituent substrings
* check to see which substrings are in which indexes
* **look up the specific the tile coordinates of each occurrence of each substring in each matching index**
* **determine which occurrences of which substrings approximately spatially align**
* for any plausible stackings of features, retrieve full feature geometry and verify the estimated spatial alignment
* format any results for response to the user

The steps in bold are implemented in this module in C++ for disk/memory compactness and speed; the rest are implemented in `carmen` itself in Javascript, or in other `carmen` dependencies.

As a concrete example, if a user were searching for "paris france," carmen might determine that the `country` index contained the string "france" and the `city` index contained multiple occurrences of "paris" (one for the real one in France, one for the one in Texas, etc.). carmen-cache would be responsible for retriving the tile coordinates of all the tiles covering the country France, and the tile coordinates covering each Paris, and seeing which aligned with which; it would discover that the French Paris's tiles overlapped with some of the tiles in the France feature, making that combination a plausible stacking, while the Texas Paris's tiles would not align with France. It would return these results to `carmen` for further verification.

### Detailed architecture

`carmen-cache` exposes two implementations of the same interface, one read-write version called `MemoryCache` and one disk-based read-only version called `RocksDBCache` build on [Facebook's RocksDB](https://github.com/facebook/rocksdb). The read-write version is used during `carmen`'s index-building process, at the end of which it's serialized into the read-only version for storage. At query time, the read-only version is used instead, as it's both faster and more memory-efficient.

Carmen-cache knows about the following kinds of data:
* **keys**: these are strings that might occur in a feature or a user query
* **grids**: each key is mapped to one or more *grid values*, each of which is a 64-bit representation of a tuple of score, textual relevance, grid coordinates (x and y), and feature ID representing the metadata about one occurrence of the given key within the index. The same key might map to more than one grid if, for example, more than one feature has the same name, or a single feature spans more than one tile. Grids are exposed to and expected from carmen as integers; see `test/grid.js` for an example of how the these 5-tuples should be packed into an integer representation.
* **languages**: keys are optionally annotated with the language or languages for which the key is valid for the given feature. For example, a feature called "London" in English but "Londres" in French can be stored under both keys but with different language annotations. This way, `carmen-cache` can apply a penalty for results that match a given query in a language other than the one the user requested. `carmen-cache` represents each language as a number, and has no internal concept of which real-world language each number maps to; it's `carmen`'s responsibility to perform that mapping.

The `MemoryCache` supports setting of a key (and optional list of language numbers) to a list of grid numbers. It also supports a `pack` operation, which writes out the read-write form into a henceforth-read-only version encoded on disk as a RocksDB database.

Both versions support a `list` operation to retrieve all keys, a `get` operation to retrieve a grid list for a given key and language set, and a `getMatching` operation that can do either or both of:
* retrieve grids for all occurrences of a key with optional penalties applied for non-matching languages
* retrieve grids for all keys starting with a given prefix (useful for autocomplete queries)

### `RocksDBCache` format

The RocksDB representation of the cache condenses the data for on-disk storage as a RocksDB database. Each key in the RocksDB database is a string, followed by a `|` delimiter, followed by as many as 128 bits of data representing the language annotation. If this bit set is of zero length, it is interpreted as `2^128 - 1` (i.e., all `1`). If it's any other length less than 128, it's interpreted as the least significant bits of a 128-bit integer (in other words, it's padded with `0` on the most significant side).

The value is a compact representation of the set of grid integers for a given set of grids. This value is obtained by sorting the integer representations of the grids in descending order, delta-encoding them (that is, storing all values after the first as the difference between it and its predecessor), and packing them as variable-length integers into a `protobuf` buffer. Reading from this structure operates in reverse, expending out all values after the first additively, and can be done lazily.

The `RocksDB` representation contains an additional optimization to assist in autocomplete queries: it precomputes combined sorted lists of grids automatically for fixed-length prefixes of length 3 and length 6, so as to reduce the number of seeks and reads necessary to calculate autocomplete results for very short autocomplete queries. These precomputed versions are stored with a key that begins with `=1` or `=2` (for shorter and longer prefixes, respectively), followed by the prefix string, followed by the `|` delimiter and language bitmask as per usual. This process is transparent to carmen: these keys are calculated and populated automatically at `pack` time, read automatically instead of reading the full `grid` lists at `getMatching` time if the requested key is sufficiently short, and hidden from, e.g., `carmen`'s `list` operation.

### Coalesce (incomplete)

`carmen-cache`'s `coalesce` operation is what computes the possible stacking of combinations of substrings and returns the results to carmen. It can take advantage of the C++ threadpool to consider multiple possible stackings in parallel, and contains two implementations: `coalesceSingle` and `coalesceMulti`. The former handles cases where a given query could be satisfied in its entirety by a single index, whereas the latter considers multi-index interactions. `coalesce` expects a set of `phrasematch` objects (see `carmen`'s source for what they contain), and returns a set of coalesce results via callback to `carmen`.

A brief diagrammatic overview of how `coalesceMulti` works follows:

![coalescemulti](https://cloud.githubusercontent.com/assets/83384/21327650/3588be54-c5fe-11e6-894e-cdaa68ecfa5f.jpg)
