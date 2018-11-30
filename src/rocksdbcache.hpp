#ifndef __CARMEN_ROCKSDBCACHE_HPP__
#define __CARMEN_ROCKSDBCACHE_HPP__

#include "cpp_util.hpp"
#include "node_util.hpp"

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

#define CACHE_MESSAGE 1
#define CACHE_ITEM 1

#define MEMO_PREFIX_LENGTH_T1 3
#define MEMO_PREFIX_LENGTH_T2 6
#define PREFIX_MAX_GRID_LENGTH 500000

struct sortableGrid {
    sortableGrid(protozero::const_varint_iterator<uint64_t> _it,
                 protozero::const_varint_iterator<uint64_t> _end,
                 value_type _unadjusted_lastval,
                 bool _matches_language)
        : it(_it),
          end(_end),
          unadjusted_lastval(_unadjusted_lastval),
          matches_language(_matches_language) {
    }
    protozero::const_varint_iterator<uint64_t> it;
    protozero::const_varint_iterator<uint64_t> end;
    value_type unadjusted_lastval;
    bool matches_language;
    sortableGrid() = delete;
    sortableGrid(sortableGrid const& c) = delete;
    sortableGrid& operator=(sortableGrid const& c) = delete;
    sortableGrid& operator=(sortableGrid&& c) = default;
    sortableGrid(sortableGrid&& c) = default;
};

struct MergeBaton : carmen::noncopyable {
    uv_work_t request;
    std::string filename1;
    std::string filename2;
    std::string filename3;
    std::string method;
    std::string error;
    Nan::Persistent<v8::Function> callback;
};

inline void decodeMessage(std::string const& message, intarray& array) {
    protozero::pbf_reader item(message);
    item.next(CACHE_ITEM);
    auto vals = item.get_packed_uint64();
    uint64_t lastval = 0;
    // delta decode values.
    for (auto it = vals.first; it != vals.second; ++it) {
        if (lastval == 0) {
            lastval = *it;
            array.emplace_back(lastval);
        } else {
            lastval = lastval - *it;
            array.emplace_back(lastval);
        }
    }
}

inline void decodeAndBoostMessage(std::string const& message, intarray& array) {
    protozero::pbf_reader item(message);
    item.next(CACHE_ITEM);
    auto vals = item.get_packed_uint64();
    uint64_t lastval = 0;
    // delta decode values.
    for (auto it = vals.first; it != vals.second; ++it) {
        if (lastval == 0) {
            lastval = *it;
            array.emplace_back(lastval | LANGUAGE_MATCH_BOOST);
        } else {
            lastval = lastval - *it;
            array.emplace_back(lastval | LANGUAGE_MATCH_BOOST);
        }
    }
}

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

void mergeQueue(uv_work_t* req);
void mergeAfter(uv_work_t* req, int status);

intarray __get(RocksDBCache const* c, std::string phrase, langfield_type langfield);
intarray __getmatching(RocksDBCache const* c, std::string phrase, PrefixMatch match_prefixes, langfield_type langfield);

} // namespace carmen

#endif // __CARMEN_ROCKSDBCACHE_HPP__
