#ifndef __CARMEN_BINDING_HPP__
#define __CARMEN_BINDING_HPP__

#include "coalesce.hpp"
#include "memorycache.hpp"
#include "node_util.hpp"
#include "rocksdbcache.hpp"

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

using namespace v8;

template <class T>
class JSCache : public node::ObjectWrap {
  public:
    ~JSCache<T>();
    static Nan::Persistent<v8::FunctionTemplate> constructor;
    static void Initialize(v8::Handle<v8::Object> target);
    static NAN_METHOD(New);
    static NAN_METHOD(pack);
    static NAN_METHOD(list);
    static NAN_METHOD(_get);
    static NAN_METHOD(_getmatching);
    static NAN_METHOD(_set);
    explicit JSCache();
    void _ref() { Ref(); }
    void _unref() { Unref(); }

    T cache;
};

template <class T>
Nan::Persistent<v8::FunctionTemplate> JSCache<T>::constructor;

template <>
NAN_METHOD(JSCache<carmen::RocksDBCache>::New);
template <>
NAN_METHOD(JSCache<carmen::MemoryCache>::New);

template <>
NAN_METHOD(JSCache<carmen::MemoryCache>::_set);

using JSRocksDBCache = JSCache<carmen::RocksDBCache>;
using JSMemoryCache = JSCache<carmen::MemoryCache>;

template <class T>
intarray __get(JSCache<T>* c, std::string& phrase, langfield_type langfield);
template <class T>
intarray __getmatching(JSCache<T>* c, std::string& phrase, bool match_prefixes, langfield_type langfield);

struct CoalesceBaton : carmen::noncopyable {
    uv_work_t request;
    // params
    std::vector<PhrasematchSubq> stack;
    std::vector<uint64_t> centerzxy;
    std::vector<uint64_t> bboxzxy;
    double radius;
    Nan::Persistent<v8::Function> callback;
    // ref tracking
    std::vector<std::pair<char, void*>> refs;
    // return
    std::vector<Context> features;
    // error
    std::string error;
};

NAN_METHOD(JSCoalesce);
void jsCoalesceTask(uv_work_t* req);
void jsCoalesceAfter(uv_work_t* req, int status);

} // namespace carmen

#endif // __CARMEN_BINDING_HPP__
