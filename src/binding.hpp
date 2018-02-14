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

#include "node_util.hpp"
#include "coalesce.hpp"
#include "memorycache.hpp"

#include "radix_max_heap.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include <algorithm>
#include <deque>
#include <exception>
#include <limits>
#include <list>
#include <map>
#include <nan.h>
#include <string>
#include <vector>
#pragma clang diagnostic pop
#include <cassert>
#include <cstring>
#include <fstream>
#include <iostream>
#include <tuple>

namespace carmen {

using namespace v8;


class RocksDBCache : public node::ObjectWrap {
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

class NormalizationCache : public node::ObjectWrap {
  public:
    ~NormalizationCache();
    static Nan::Persistent<v8::FunctionTemplate> constructor;
    static void Initialize(v8::Handle<v8::Object> target);
    static NAN_METHOD(New);
    static NAN_METHOD(get);
    static NAN_METHOD(getprefixrange);
    static NAN_METHOD(getall);
    static NAN_METHOD(writebatch);
    explicit NormalizationCache();
    void _ref() { Ref(); }
    void _unref() { Unref(); }
    std::shared_ptr<rocksdb::DB> db;
};

#define CACHE_MESSAGE 1
#define CACHE_ITEM 1

#define MEMO_PREFIX_LENGTH_T1 3
#define MEMO_PREFIX_LENGTH_T2 6
#define PREFIX_MAX_GRID_LENGTH 500000

intarray __getmatching(RocksDBCache const* c, std::string phrase, bool match_prefixes, langfield_type langfield);
intarray __get(RocksDBCache const* c, std::string phrase, langfield_type langfield);

} // namespace carmen

#endif // __CARMEN_BINDING_HPP__
