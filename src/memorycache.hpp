#ifndef __CARMEN_MEMORYCACHE_HPP__
#define __CARMEN_MEMORYCACHE_HPP__

#include "node_util.hpp"
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

#pragma clang diagnostic pop


namespace carmen {

class MemoryCache : public node::ObjectWrap {
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

intarray __get(MemoryCache const* c, std::string phrase, langfield_type langfield);
intarray __getmatching(MemoryCache const* c, std::string phrase, bool match_prefixes, langfield_type langfield);

} // namespace carmen

#endif // __CARMEN_MEMORYCACHE_HPP__
