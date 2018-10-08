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
        static NAN_METHOD(merge);
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

template <>
NAN_METHOD(JSCache<carmen::RocksDBCache>::merge);

using JSRocksDBCache = JSCache<carmen::RocksDBCache>;

intarray __get(JSRocksDBCache* c, std::string phrase, langfield_type langfield);
intarray __getmatching(JSRocksDBCache* c, std::string phrase, bool match_prefixes, langfield_type langfield);

} // namespace carmen

#endif // __CARMEN_BINDING_HPP__
