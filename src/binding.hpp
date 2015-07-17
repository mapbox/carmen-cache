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
#include <sparsehash/sparse_hash_set>
#pragma clang diagnostic pop
#include <iostream>

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
    typedef uint32_t key_type;
    typedef uint64_t value_type;
    // pbf message cache
    typedef google::sparse_hash_set<uint32_t> ldictcache;
    typedef std::map<std::string,std::string> message_cache;
    // fully cached item
    typedef std::vector<value_type> intarray;
    typedef std::map<key_type,intarray> arraycache;
    typedef std::map<std::string,arraycache> memcache;
    typedef std::map<std::string,ldictcache> dictcache;
    static v8::Persistent<v8::FunctionTemplate> constructor;
    static void Initialize(v8::Handle<v8::Object> target);
    static NAN_METHOD(New);
    static NAN_METHOD(has);
    static NAN_METHOD(hasDict);
    static NAN_METHOD(loadSync);
    static NAN_METHOD(loadAsDict);
    static NAN_METHOD(pack);
    static NAN_METHOD(list);
    static NAN_METHOD(_get);
    static NAN_METHOD(_set);
    static NAN_METHOD(_dict);
    static NAN_METHOD(unload);
    static NAN_METHOD(coalesce);
    explicit Cache();
    void _ref() { Ref(); }
    void _unref() { Unref(); }
    memcache cache_;
    dictcache dict_;
    message_cache msg_;
};

}

#endif // __CARMEN_BINDING_HPP__
