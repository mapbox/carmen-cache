#ifndef __CARMEN_BINDING_HPP__
#define __CARMEN_BINDING_HPP__

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunknown-pragmas"
#pragma clang diagnostic ignored "-Wconversion"
#pragma clang diagnostic ignored "-Wshadow"
#pragma clang diagnostic ignored "-Wsign-compare"
#pragma clang diagnostic ignored "-Wunused-local-typedef"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wpadded"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wsign-conversion"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#include <nan.h>
#include <exception>
#include <string>
#include <map>
#include <list>
#include <vector>
#include <deque>
#include <limits>
#include <algorithm>
#include "radix_max_heap.h"
#pragma clang diagnostic pop
#include <iostream>
#include <fstream>
#include <tuple>

#include <cassert>
#include <cstring>

#include "rocksdb/db.h"

namespace carmen {

class noncopyable
{
protected:
    constexpr noncopyable() = default;
    ~noncopyable() = default;
    noncopyable( noncopyable const& ) = delete;
    noncopyable& operator=(noncopyable const& ) = delete;
};

typedef std::string key_type;
typedef uint64_t value_type;
typedef unsigned __int128 langfield_type;
// fully cached item
typedef std::vector<value_type> intarray;
typedef std::vector<key_type> keyarray;
typedef std::map<key_type,intarray> arraycache;

class MemoryCache: public node::ObjectWrap {
public:
    ~MemoryCache();
    static Nan::Persistent<v8::FunctionTemplate> constructor;
    static void Initialize(v8::Handle<v8::Object> target);
    static NAN_METHOD(New);
    static NAN_METHOD(pack);
    static NAN_METHOD(list);
    static NAN_METHOD(_get);
    static NAN_METHOD(_getmatching);
    static NAN_METHOD(_set);
    static NAN_METHOD(coalesce);
    explicit MemoryCache();
    void _ref() { Ref(); }
    void _unref() { Unref(); }
    arraycache cache_;
};

class RocksDBCache: public node::ObjectWrap {
public:
    ~RocksDBCache();
    static Nan::Persistent<v8::FunctionTemplate> constructor;
    static void Initialize(v8::Handle<v8::Object> target);
    static NAN_METHOD(New);
    static NAN_METHOD(pack);
    static NAN_METHOD(merge);
    static NAN_METHOD(list);
    static NAN_METHOD(_get);
    static NAN_METHOD(_getmatching);
    static NAN_METHOD(_set);
    static NAN_METHOD(coalesce);
    explicit RocksDBCache();
    void _ref() { Ref(); }
    void _unref() { Unref(); }
    std::shared_ptr<rocksdb::DB> db;
};

#define CACHE_MESSAGE 1
#define CACHE_ITEM 1

#define MEMO_PREFIX_LENGTH_T1 4
#define MEMO_PREFIX_LENGTH_T2 7
#define PREFIX_MAX_GRID_LENGTH 500000

#define ALL_LANGUAGES std::numeric_limits<unsigned __int128>::max()
#define LANGFIELD_SEPARATOR '|'

#define TYPE_MEMORY 1
#define TYPE_ROCKSDB 2
}

#endif // __CARMEN_BINDING_HPP__
