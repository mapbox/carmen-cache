#ifndef __CARMEN_NORMALIZATIONCACHE_HPP__
#define __CARMEN_NORMALIZATIONCACHE_HPP__

#include "cpp_util.hpp"

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
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"

#pragma clang diagnostic pop

namespace carmen {

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

} // namespace carmen

#endif // __CARMEN_NORMALIZATIONCACHE_HPP__
