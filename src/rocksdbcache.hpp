#ifndef __CARMEN_ROCKSDBCACHE_HPP__
#define __CARMEN_ROCKSDBCACHE_HPP__

#include "cpp_util.hpp"

// this is an external library, so squash this warning
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-conversion"
#include "radix_max_heap.h"
#pragma clang diagnostic pop

namespace carmen {

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

class RocksDBCache {
  public:
    RocksDBCache(const std::string& filename);
    RocksDBCache();
    ~RocksDBCache();

    bool pack(const std::string& filename);
    std::vector<std::pair<std::string, langfield_type>> list();

    std::vector<uint64_t> __get(const std::string& phrase, langfield_type langfield);
    std::vector<uint64_t> __getmatching(const std::string& phrase_ref, bool match_prefixes, langfield_type langfield);

    std::shared_ptr<rocksdb::DB> db;
};

} // namespace carmen

#endif // __CARMEN_ROCKSDBCACHE_HPP__
