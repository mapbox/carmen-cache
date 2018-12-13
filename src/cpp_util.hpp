#ifndef __CARMEN_CPP_UTIL_HPP__
#define __CARMEN_CPP_UTIL_HPP__

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

#include "rocksdb/db.h"
#include <cassert>
#include <cmath>
#include <cstdint>
#include <map>
#include <protozero/pbf_reader.hpp>
#include <protozero/pbf_writer.hpp>
#include <string>
#include <vector>

#pragma clang diagnostic pop

namespace carmen {

typedef std::string key_type;
typedef uint64_t value_type;
typedef unsigned __int128 langfield_type;
// fully cached item
typedef std::vector<value_type> intarray;
typedef std::vector<key_type> keyarray;
typedef std::map<key_type, intarray> arraycache;

class noncopyable {
  protected:
    constexpr noncopyable() = default;
    ~noncopyable() = default;
    noncopyable(noncopyable const&) = delete;
    noncopyable& operator=(noncopyable const&) = delete;
};

typedef enum {
    disabled,
    enabled,
    word_boundary
} PrefixMatch;

typedef unsigned __int128 langfield_type;
constexpr uint64_t LANGUAGE_MATCH_BOOST = static_cast<const uint64_t>(1) << 63;

//relev = 5 bits
//count = 3 bits
//reason = 12 bits
//* 1 bit gap
//id = 32 bits
constexpr double _pow(double x, int y) {
    return y == 0 ? 1.0 : x * _pow(x, y - 1);
}

constexpr uint64_t POW2_51 = static_cast<uint64_t>(_pow(2.0, 51));
constexpr uint64_t POW2_48 = static_cast<uint64_t>(_pow(2.0, 48));
constexpr uint64_t POW2_34 = static_cast<uint64_t>(_pow(2.0, 34));
constexpr uint64_t POW2_28 = static_cast<uint64_t>(_pow(2.0, 28));
constexpr uint64_t POW2_25 = static_cast<uint64_t>(_pow(2.0, 25));
constexpr uint64_t POW2_20 = static_cast<uint64_t>(_pow(2.0, 20));
constexpr uint64_t POW2_14 = static_cast<uint64_t>(_pow(2.0, 14));
constexpr uint64_t POW2_3 = static_cast<uint64_t>(_pow(2.0, 3));
constexpr uint64_t POW2_2 = static_cast<uint64_t>(_pow(2.0, 2));

struct PhrasematchSubq {
    PhrasematchSubq(void* c,
                    char t,
                    double w,
                    std::string p,
                    PrefixMatch pf,
                    unsigned short i,
                    unsigned short z,
                    uint32_t m,
                    langfield_type l) : cache(c),
                                        type(t),
                                        weight(w),
                                        phrase(p),
                                        prefix(pf),
                                        idx(i),
                                        zoom(z),
                                        mask(m),
                                        langfield(l) {}
    void* cache;
    char type;
    double weight;
    std::string phrase;
    PrefixMatch prefix;
    unsigned short idx;
    unsigned short zoom;
    uint32_t mask;
    langfield_type langfield;
    PhrasematchSubq& operator=(PhrasematchSubq&& c) = default;
    PhrasematchSubq(PhrasematchSubq&& c) = default;
};

struct Cover {
    double relev;
    uint32_t id;
    uint32_t tmpid;
    unsigned short x;
    unsigned short y;
    unsigned short score;
    unsigned short idx;
    uint32_t mask;
    double distance;
    double scoredist;
    bool matches_language;

    Cover() = default;
    Cover(Cover const& c) = default;
    Cover& operator=(Cover const& c) = delete;
    Cover& operator=(Cover&& c) = default;
    Cover(Cover&& c) = default;
};

struct Context {
    std::vector<Cover> coverList;
    uint32_t mask;
    double relev;

    Context() = delete;
    Context(Context const& c) = delete;
    Context& operator=(Context const& c) = delete;
    Context(Cover&& cov,
            uint32_t mask,
            double relev)
        : coverList(),
          mask(mask),
          relev(relev) {
        coverList.emplace_back(std::move(cov));
    }
    Context& operator=(Context&& c) {
        coverList = std::move(c.coverList);
        mask = std::move(c.mask);
        relev = std::move(c.relev);
        return *this;
    }
    Context(std::vector<Cover>&& cl,
            uint32_t mask,
            double relev)
        : coverList(std::move(cl)),
          mask(mask),
          relev(relev) {}

    Context(Context&& c)
        : coverList(std::move(c.coverList)),
          mask(std::move(c.mask)),
          relev(std::move(c.relev)) {}
};

struct ZXY {
    unsigned z;
    unsigned x;
    unsigned y;
};

Cover numToCover(uint64_t num);
ZXY pxy2zxy(unsigned z, unsigned x, unsigned y, unsigned target_z);
ZXY bxy2zxy(unsigned z, unsigned x, unsigned y, unsigned target_z, bool max = false);
double scoredist(unsigned zoom, double distance, unsigned short score, double radius);

inline bool coverSortByRelev(Cover const& a, Cover const& b) noexcept {
    if (b.relev > a.relev)
        return false;
    else if (b.relev < a.relev)
        return true;
    else if (b.scoredist > a.scoredist)
        return false;
    else if (b.scoredist < a.scoredist)
        return true;
    else if (b.idx < a.idx)
        return false;
    else if (b.idx > a.idx)
        return true;
    else if (b.id < a.id)
        return false;
    else if (b.id > a.id)
        return true;
    // sorting by x and y is arbitrary but provides a more deterministic output order
    else if (b.x < a.x)
        return false;
    else if (b.x > a.x)
        return true;
    else
        return (b.y > a.y);
}

inline bool subqSortByZoom(PhrasematchSubq const& a, PhrasematchSubq const& b) noexcept {
    if (a.zoom < b.zoom) return true;
    if (a.zoom > b.zoom) return false;
    return (a.idx < b.idx);
}

inline bool contextSortByRelev(Context const& a, Context const& b) noexcept {
    if (b.relev > a.relev)
        return false;
    else if (b.relev < a.relev)
        return true;
    else if (b.coverList[0].scoredist > a.coverList[0].scoredist)
        return false;
    else if (b.coverList[0].scoredist < a.coverList[0].scoredist)
        return true;
    else if (b.coverList[0].idx < a.coverList[0].idx)
        return false;
    else if (b.coverList[0].idx > a.coverList[0].idx)
        return true;
    return (b.coverList[0].id > a.coverList[0].id);
}

inline double tileDist(unsigned px, unsigned py, unsigned tileX, unsigned tileY) {
    const double dx = static_cast<double>(px) - static_cast<double>(tileX);
    const double dy = static_cast<double>(py) - static_cast<double>(tileY);
    const double distance = sqrt((dx * dx) + (dy * dy));

    return distance;
}

constexpr langfield_type ALL_LANGUAGES = ~static_cast<langfield_type>(0);
#define LANGFIELD_SEPARATOR '|'

inline void add_langfield(std::string& s, langfield_type langfield) {
    if (langfield != ALL_LANGUAGES) {
        char* lf_as_char = reinterpret_cast<char*>(&langfield);

        // we only want to copy over as many bytes as we're using
        // so find the last byte that's not zero, and copy until there
        // NOTE: this assumes little-endianness, where the bytes
        // in use will be first rather than last
        size_t highest(0);
        for (size_t i = 0; i < sizeof(langfield_type); i++) {
            if (lf_as_char[i] != 0) highest = i;
        }
        size_t field_length = highest + 1;

        s.reserve(sizeof(LANGFIELD_SEPARATOR) + field_length);
        s.push_back(LANGFIELD_SEPARATOR);
        s.append(lf_as_char, field_length);
    } else {
        s.push_back(LANGFIELD_SEPARATOR);
    }
}

// in general, the key format including language field is <key_text><separator character><8-byte langfield>
// but as a size optimization for language-less indexes, we omit the langfield
// if it would otherwise have been ALL_LANGUAGES (all 1's), so if the first occurence
// of the separator character is also the last character in the string, we just retur ALL_LANGUAGES
// otherwise we extract it from the string
//
// we centralize both the adding of the field and extracting of the field here to keep from having
// to handle that optimization everywhere
inline langfield_type extract_langfield(std::string const& s) {
    size_t length = s.length();
    size_t langfield_start = s.find(LANGFIELD_SEPARATOR) + 1;
    size_t distance_from_end = length - langfield_start;

    if (distance_from_end == 0) {
        return ALL_LANGUAGES;
    } else {
        langfield_type result(0);
        memcpy(&result, s.data() + langfield_start, distance_from_end);
        return result;
    }
}

inline void packVec(intarray const& varr, std::unique_ptr<rocksdb::DB> const& db, std::string const& key) {
    std::string message;

    protozero::pbf_writer item_writer(message);

    {
        // Using new (in protozero 1.3.0) packed writing API
        // https://github.com/mapbox/protozero/commit/4e7e32ac5350ea6d3dcf78ff5e74faeee513a6e1
        protozero::packed_field_uint64 field{item_writer, 1};
        uint64_t lastval = 0;
        for (auto const& vitem : varr) {
            if (lastval == 0) {
                field.add_element(static_cast<uint64_t>(vitem));
            } else {
                field.add_element(static_cast<uint64_t>(lastval - vitem));
            }
            lastval = vitem;
        }
    }

    db->Put(rocksdb::WriteOptions(), key, message);
}

// rocksdb is also used in memorycache
rocksdb::Status OpenDB(const rocksdb::Options& options, const std::string& name, std::unique_ptr<rocksdb::DB>& dbptr);
rocksdb::Status OpenForReadOnlyDB(const rocksdb::Options& options, const std::string& name, std::unique_ptr<rocksdb::DB>& dbptr);

#define TYPE_MEMORY 1
#define TYPE_ROCKSDB 2

#define CACHE_MESSAGE 1
#define CACHE_ITEM 1

#define MEMO_PREFIX_LENGTH_T1 3
#define MEMO_PREFIX_LENGTH_T2 6
#define PREFIX_MAX_GRID_LENGTH 500000

} // namespace carmen

#endif // __CARMEN_CPP_UTIL_HPP__
