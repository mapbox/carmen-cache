#ifndef __CARMEN_NORMALIZATIONCACHE_HPP__
#define __CARMEN_NORMALIZATIONCACHE_HPP__

#include <nan.h>
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include "cpp_util.hpp"

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
