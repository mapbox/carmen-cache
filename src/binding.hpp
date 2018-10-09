#ifndef __CARMEN_BINDING_HPP__
#define __CARMEN_BINDING_HPP__

#include "rocksdbcache.hpp"
#include "coalesce.hpp"
#include "memorycache.hpp"
#include "node_util.hpp"
#include "normalizationcache.hpp"

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
        static NAN_METHOD(coalesce);
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
intarray __get(JSCache<T>* c, std::string phrase, langfield_type langfield);
template <class T>
intarray __getmatching(JSCache<T>* c, std::string phrase, bool match_prefixes, langfield_type langfield);

} // namespace carmen

#endif // __CARMEN_BINDING_HPP__
