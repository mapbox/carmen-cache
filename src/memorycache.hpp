#ifndef __CARMEN_MEMORYCACHE_HPP__
#define __CARMEN_MEMORYCACHE_HPP__

#include "node_util.hpp"
#include "cpp_util.hpp"
#include <nan.h>

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
