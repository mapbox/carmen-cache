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
#include <vector>
#include "index.pb.h"
#include <sparsehash/sparse_hash_map>
#pragma clang diagnostic pop

namespace carmen {

class noncopyable
{
protected:
    constexpr noncopyable() = default;
    ~noncopyable() = default;
    noncopyable( noncopyable const& ) = delete;
    noncopyable& operator=(noncopyable const& ) = delete;
};

class Cache: public node::ObjectWrap {
public:
    ~Cache();
    typedef uint64_t int_type;
    // lazy ref item
    typedef uint64_t offset_type;
    typedef google::sparse_hash_map<int_type,offset_type> larraycache;
    typedef std::map<std::string,larraycache> lazycache;
    typedef std::map<std::string,std::string> message_cache;
    // fully cached item
    typedef std::vector<int_type> intarray;
    typedef std::map<uint32_t,intarray> arraycache;
    typedef std::map<std::string,arraycache> memcache;
    static v8::Persistent<v8::FunctionTemplate> constructor;
    static void Initialize(v8::Handle<v8::Object> target);
    static NAN_METHOD(New);
    static NAN_METHOD(has);
    static NAN_METHOD(loadSync);
    static NAN_METHOD(load);
    static void AsyncLoad(uv_work_t* req);
    static void AfterLoad(uv_work_t* req);
    static NAN_METHOD(pack);
    static NAN_METHOD(list);
    static NAN_METHOD(_get);
    static NAN_METHOD(_set);
    static NAN_METHOD(_exists);
    static NAN_METHOD(unload);
    static NAN_METHOD(coalesce);
    static void AsyncRun(uv_work_t* req);
    static void AfterRun(uv_work_t* req);
    Cache(std::string const& id, unsigned shardlevel);
    void _ref() { Ref(); }
    void _unref() { Unref(); }
    std::string id_;
    unsigned shardlevel_;
    memcache cache_;
    lazycache lazy_;
    message_cache msg_;
};

}

#endif // __CARMEN_BINDING_HPP__
