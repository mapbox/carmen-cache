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

// this is a basic decoding operation that unpacks a whole protobuff message
inline void decodeMessage(std::string const& message, intarray& array, size_t limit) {
    protozero::pbf_reader item(message);
    item.next(CACHE_ITEM);
    auto vals = item.get_packed_uint64();
    uint64_t lastval = 0;
    // delta decode values.
    for (auto it = vals.first; it != vals.second && array.size() < limit; ++it) {
        if (lastval == 0) {
            lastval = *it;
            array.emplace_back(lastval);
        } else {
            lastval = lastval - *it;
            array.emplace_back(lastval);
        }
    }
}

// this function is as above, but also modifies the output of the protobuf message
// to set the language-match bit to true, effectively boosting its sort order
inline void decodeAndBoostMessage(std::string const& message, intarray& array, size_t limit) {
    protozero::pbf_reader item(message);
    item.next(CACHE_ITEM);
    auto vals = item.get_packed_uint64();
    uint64_t lastval = 0;
    // delta decode values.
    for (auto it = vals.first; it != vals.second && array.size() < limit; ++it) {
        if (lastval == 0) {
            lastval = *it;
            array.emplace_back(lastval | LANGUAGE_MATCH_BOOST);
        } else {
            lastval = lastval - *it;
            array.emplace_back(lastval | LANGUAGE_MATCH_BOOST);
        }
    }
}

inline bool inplaceBboxCheck(uint64_t val, const uint64_t box[4]) {
    uint64_t inplaceX = val & X_MASK;
    uint64_t inplaceY = val & Y_MASK;
    return (inplaceX >= box[0] && inplaceX <= box[2] && inplaceY >= box[1] && inplaceY <= box[3]);
}

// This is a modified decode operation used in RocksDBCache::__getmatchingBboxFiltered.
// it takes the boost-y-ness as an argument (which we could likely do above as well
// if we wanted, but would need to evaluate performance) and also takes a bounding box
// parameter to allow for pre-filtering results by bounding box before they're later
// sorted inside getmatching; this makes sense to do in this order in circumstances
// where we expect the bounding box filter to filter out lots of things, as it does
// more work at O(n) for a potential big savings on an O(n log n) operation if the
// second n can be significantly reduced by the linear filter.
//
// The format of the box is in [minX, minY, maxX, maxY] tile coordinate order,
// except that the X's and Y's need to have already been shifted into same positions
// as they occupy in encoded grids (20 bits left and 34 bits left, respectively)
// so that we can efficiently compare them to the X and Y coordinates within each
// grid without shifting, to keep this whole operation as fast as possible.
inline void decodeAndBboxFilter(std::string const& message, intarray& array, uint64_t boost, const uint64_t box[4]) {
    protozero::pbf_reader item(message);
    item.next(CACHE_ITEM);
    auto vals = item.get_packed_uint64();
    // delta decode values.
    auto it = vals.first;
    if (vals.first != vals.second) {
        uint64_t lastval = *it;
        if (inplaceBboxCheck(lastval, box)) array.emplace_back(lastval | boost);
        it++;
        for (; it != vals.second; ++it) {
            lastval = lastval - *it;
            if (inplaceBboxCheck(lastval, box)) array.emplace_back(lastval | boost);
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
    std::vector<uint64_t> __getmatching(const std::string& phrase_ref, PrefixMatch match_prefixes, langfield_type langfield, size_t max_results);
    std::vector<uint64_t> __getmatchingBboxFiltered(const std::string& phrase_ref, PrefixMatch match_prefixes, langfield_type langfield, size_t max_results, const uint64_t box[4]);

    std::shared_ptr<rocksdb::DB> db;

private:
    template <class FnT>
    void fetch_messages(const std::string& phrase_ref, PrefixMatch match_prefixes,
                        langfield_type langfield, FnT && fn);
};

} // namespace carmen

#endif // __CARMEN_ROCKSDBCACHE_HPP__
