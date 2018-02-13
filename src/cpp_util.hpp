#ifndef __CARMEN_CPP_UTIL_HPP__
#define __CARMEN_CPP_UTIL_HPP__

#include <cstdint>
#include <string>
#include <vector>
#include <cassert>
#include <cmath>

namespace carmen {

typedef unsigned __int128 langfield_type;
constexpr uint64_t LANGUAGE_MATCH_BOOST = (const uint64_t)(1) << 63;

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
                    bool pf,
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
    bool prefix;
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
};

struct Context {
    std::vector<Cover> coverList;
    uint32_t mask;
    double relev;

    Context(Context const& c) = default;
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
double scoredist(unsigned zoom, double distance, double score, double radius);

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

} // namespace carmen

#endif // __CARMEN_CPP_UTIL_HPP__