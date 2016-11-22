#include "binding.hpp"
#include "cache.hpp"

#include <cmath>

namespace carmen {

inline std::string shard(uint64_t level, uint64_t id) {
    if (level == 0) return "0";
    unsigned int bits = 52 - (static_cast<unsigned int>(level) * 4);
    unsigned int shard_id = static_cast<unsigned int>(std::floor(id / std::pow(2, bits)));
    return std::to_string(shard_id);
}

//relev = 5 bits
//count = 3 bits
//reason = 12 bits
//* 1 bit gap
//id = 32 bits
constexpr double _pow(double x, int y)
{
    return y == 0 ? 1.0 : x * _pow(x, y-1);
}

constexpr uint64_t POW2_51 = static_cast<uint64_t>(_pow(2.0,51));
constexpr uint64_t POW2_48 = static_cast<uint64_t>(_pow(2.0,48));
constexpr uint64_t POW2_34 = static_cast<uint64_t>(_pow(2.0,34));
constexpr uint64_t POW2_28 = static_cast<uint64_t>(_pow(2.0,28));
constexpr uint64_t POW2_25 = static_cast<uint64_t>(_pow(2.0,25));
constexpr uint64_t POW2_20 = static_cast<uint64_t>(_pow(2.0,20));
constexpr uint64_t POW2_14 = static_cast<uint64_t>(_pow(2.0,14));
constexpr uint64_t POW2_3 = static_cast<uint64_t>(_pow(2.0,3));
constexpr uint64_t POW2_2 = static_cast<uint64_t>(_pow(2.0,2));

Cover numToCover(uint64_t num) {
    Cover cover;
    assert(((num >> 34) % POW2_14) <= static_cast<double>(std::numeric_limits<unsigned short>::max()));
    assert(((num >> 34) % POW2_14) >= static_cast<double>(std::numeric_limits<unsigned short>::min()));
    unsigned short y = static_cast<unsigned short>((num >> 34) % POW2_14);
    assert(((num >> 20) % POW2_14) <= static_cast<double>(std::numeric_limits<unsigned short>::max()));
    assert(((num >> 20) % POW2_14) >= static_cast<double>(std::numeric_limits<unsigned short>::min()));
    unsigned short x = static_cast<unsigned short>((num >> 20) % POW2_14);
    assert(((num >> 48) % POW2_3) <= static_cast<double>(std::numeric_limits<unsigned short>::max()));
    assert(((num >> 48) % POW2_3) >= static_cast<double>(std::numeric_limits<unsigned short>::min()));
    unsigned short score = static_cast<unsigned short>((num >> 48) % POW2_3);
    uint32_t id = static_cast<uint32_t>(num % POW2_20);
    cover.x = x;
    cover.y = y;
    double relev = 0.4 + (0.2 * static_cast<double>((num >> 51) % POW2_2));
    cover.relev = relev;
    cover.score = score;
    cover.id = id;

    // These are not derived from decoding the input num but by
    // external values after initialization.
    cover.idx = 0;
    cover.subq = 0;
    cover.tmpid = 0;
    cover.distance = 0;

    return cover;
};

struct ZXY {
    unsigned z;
    unsigned x;
    unsigned y;
};

ZXY pxy2zxy(unsigned z, unsigned x, unsigned y, unsigned target_z) {
    ZXY zxy;
    zxy.z = target_z;

    // Interval between parent and target zoom level
    unsigned zDist = target_z - z;
    unsigned zMult = zDist - 1;
    if (zDist == 0) {
        zxy.x = x;
        zxy.y = y;
        return zxy;
    }

    // Midpoint length @ z for a tile at parent zoom level
    unsigned pMid_d = static_cast<unsigned>(std::pow(2,zDist) / 2);
    assert(pMid_d <= static_cast<double>(std::numeric_limits<unsigned>::max()));
    assert(pMid_d >= static_cast<double>(std::numeric_limits<unsigned>::min()));
    unsigned pMid = static_cast<unsigned>(pMid_d);
    zxy.x = (x * zMult) + pMid;
    zxy.y = (y * zMult) + pMid;
    return zxy;
}

ZXY bxy2zxy(unsigned z, unsigned x, unsigned y, unsigned target_z, bool max=false) {
    ZXY zxy;
    zxy.z = target_z;

    // Interval between parent and target zoom level
    signed zDist = target_z - z;
    if (zDist == 0) {
        zxy.x = x;
        zxy.y = y;
        return zxy;
    }

    // zoom conversion multiplier
    float mult = static_cast<float>(std::pow(2,zDist));

    // zoom in min
    if (zDist > 0 && !max) {
        zxy.x = static_cast<unsigned>(static_cast<float>(x) * mult);
        zxy.y = static_cast<unsigned>(static_cast<float>(y) * mult);
        return zxy;
    }
    // zoom in max
    else if (zDist > 0 && max) {
        zxy.x = static_cast<unsigned>(static_cast<float>(x) * mult + (mult - 1));
        zxy.y = static_cast<unsigned>(static_cast<float>(y) * mult + (mult - 1));
        return zxy;
    }
    // zoom out
    else {
        unsigned mod = static_cast<unsigned>(std::pow(2,target_z));
        unsigned xDiff = x % mod;
        unsigned yDiff = y % mod;
        unsigned newX = x - xDiff;
        unsigned newY = y - yDiff;

        zxy.x = static_cast<unsigned>(static_cast<float>(newX) * mult);
        zxy.y = static_cast<unsigned>(static_cast<float>(newY) * mult);
        return zxy;
    }
}

inline bool coverSortByRelev(Cover const& a, Cover const& b) noexcept {
    if (b.relev > a.relev) return false;
    else if (b.relev < a.relev) return true;
    else if (b.scoredist > a.scoredist) return false;
    else if (b.scoredist < a.scoredist) return true;
    else if (b.idx < a.idx) return false;
    else if (b.idx > a.idx) return true;
    else if (b.id < a.id) return false;
    else if (b.id > a.id) return true;
    // sorting by x and y is arbitrary but provides a more deterministic output order
    else if (b.x < a.x) return false;
    else if (b.x > a.x) return true;
    else return (b.y > a.y);
}

inline bool contextSortByRelev(Context const& a, Context const& b) noexcept {
    if (b.relev > a.relev) return false;
    else if (b.relev < a.relev) return true;
    else if (b.coverList[0].scoredist > a.coverList[0].scoredist) return false;
    else if (b.coverList[0].scoredist < a.coverList[0].scoredist) return true;
    else if (b.coverList[0].idx < a.coverList[0].idx) return false;
    else if (b.coverList[0].idx > a.coverList[0].idx) return true;
    return (b.coverList[0].id > a.coverList[0].id);
}

// 32 tiles is about 40 miles at z14.
// Simulates 40 mile cutoff in carmen.
double scoredist(unsigned zoom, double distance, double score) {
    if (distance == 0.0) distance = 0.01;
    double scoredist = 0;
    if (zoom >= 14) scoredist = 32.0 / distance;
    if (zoom == 13) scoredist = 16.0 / distance;
    if (zoom == 12) scoredist = 8.0 / distance;
    if (zoom == 11) scoredist = 4.0 / distance;
    if (zoom == 10) scoredist = 2.0 / distance;
    if (zoom <= 9)  scoredist = 1.0 / distance;
    return score > scoredist ? score : scoredist;
}

inline unsigned tileDist(unsigned ax, unsigned bx, unsigned ay, unsigned by) noexcept {
    return (ax > bx ? ax - bx : bx - ax) + (ay > by ? ay - by : by - ay);
}

void coalesceFinalize(CoalesceBaton* baton, std::vector<Context> const& contexts) {
    if (contexts.size() > 0) {
        // Coalesce stack, generate relevs.
        double relevMax = contexts[0].relev;
        unsigned short total = 0;
        std::map<uint64_t,bool> sets;
        std::map<uint64_t,bool>::iterator sit;

        for (unsigned short i = 0; i < contexts.size(); i++) {
            // Maximum allowance of coalesced features: 40.
            if (total >= 40) break;

            Context const& feature = contexts[i];

            // Since `coalesced` is sorted by relev desc at first
            // threshold miss we can break the loop.
            if (relevMax - feature.relev >= 0.25) break;

            // Only collect each feature once.
            sit = sets.find(feature.coverList[0].tmpid);
            if (sit != sets.end()) continue;

            sets.emplace(feature.coverList[0].tmpid, true);
            baton->features.emplace_back(feature);
            total++;
        }
    }
}

void coalesceSingle(uv_work_t* req) {
    CoalesceBaton *baton = static_cast<CoalesceBaton *>(req->data);

    try {
        std::vector<PhrasematchSubq> const& stack = baton->stack;
        PhrasematchSubq const& subq = stack[0];
        std::string type = "grid";
        std::string shardId = shard(4, subq.phrase);

        // proximity (optional)
        bool proximity = !baton->centerzxy.empty();
        unsigned cz;
        unsigned cx;
        unsigned cy;
        if (proximity) {
            cz = baton->centerzxy[0];
            cx = baton->centerzxy[1];
            cy = baton->centerzxy[2];
        } else {
            cz = 0;
            cx = 0;
            cy = 0;
        }

        // bbox (optional)
        bool bbox = !baton->bboxzxy.empty();
        unsigned minx;
        unsigned miny;
        unsigned maxx;
        unsigned maxy;
        if (bbox) {
            minx = baton->bboxzxy[1];
            miny = baton->bboxzxy[2];
            maxx = baton->bboxzxy[3];
            maxy = baton->bboxzxy[4];
        } else {
            minx = 0;
            miny = 0;
            maxx = 0;
            maxy = 0;
        }

        // sort grids by distance to proximity point
        Cache::intarray grids = __get(subq.cache, type, shardId, subq.phrase);

        unsigned long m = grids.size();
        double relevMax = 0;
        std::vector<Cover> covers;
        covers.reserve(m);

        for (unsigned long j = 0; j < m; j++) {
            Cover cover = numToCover(grids[j]);
            cover.idx = subq.idx;
            cover.tmpid = static_cast<uint32_t>(cover.idx * POW2_25 + cover.id);
            cover.relev = cover.relev * subq.weight;
            cover.distance = proximity ? tileDist(cx, cover.x, cy, cover.y) : 0;
            cover.scoredist = proximity ? scoredist(cz, cover.distance, cover.score) : cover.score;

            // short circuit based on relevMax thres
            if (relevMax - cover.relev >= 0.25) continue;
            if (cover.relev > relevMax) relevMax = cover.relev;

            if (bbox) {
                if (cover.x < minx || cover.y < miny || cover.x > maxx || cover.y > maxy) continue;
            }

            covers.emplace_back(cover);
        }

        std::sort(covers.begin(), covers.end(), coverSortByRelev);

        uint32_t lastid = 0;
        unsigned short added = 0;
        std::vector<Context> contexts;
        m = covers.size();
        contexts.reserve(m);
        for (unsigned long j = 0; j < m; j++) {
            // Stop at 40 contexts
            if (added == 40) break;

            // Attempt not to add the same feature but by diff cover twice
            if (lastid == covers[j].id) continue;

            lastid = covers[j].id;
            added++;

            Context context;
            context.coverList.emplace_back(covers[j]);
            context.relev = covers[j].relev;
            contexts.emplace_back(context);
        }

        coalesceFinalize(baton, contexts);
    } catch (std::exception const& ex) {
        baton->error = ex.what();
    }
}

void coalesceMulti(uv_work_t* req) {
    CoalesceBaton *baton = static_cast<CoalesceBaton *>(req->data);

    try {
        std::vector<PhrasematchSubq> const& stack = baton->stack;

        size_t size;

        // Cache zoom levels to iterate over as coalesce occurs.
        Cache::intarray zoom;
        std::vector<bool> zoomUniq(22);
        std::vector<Cache::intarray> zoomCache(22);
        size = stack.size();
        for (unsigned short i = 0; i < size; i++) {
            if (zoomUniq[stack[i].zoom]) continue;
            zoomUniq[stack[i].zoom] = true;
            zoom.emplace_back(stack[i].zoom);
        }

        size = zoom.size();
        for (unsigned short i = 0; i < size; i++) {
            Cache::intarray sliced;
            sliced.reserve(i);
            for (unsigned short j = 0; j < i; j++) {
                sliced.emplace_back(zoom[j]);
            }
            std::reverse(sliced.begin(), sliced.end());
            zoomCache[zoom[i]] = sliced;
        }

        // Coalesce relevs into higher zooms, e.g.
        // z5 inherits relev of overlapping tiles at z4.
        // @TODO assumes sources are in zoom ascending order.
        std::string type = "grid";
        std::map<uint64_t,std::vector<Cover>> coalesced;
        std::map<uint64_t,std::vector<Cover>>::iterator cit;
        std::map<uint64_t,std::vector<Cover>>::iterator pit;
        std::map<uint64_t,bool> done;
        std::map<uint64_t,bool>::iterator dit;

        size = stack.size();

        // proximity (optional)
        bool proximity = baton->centerzxy.size() > 0;
        unsigned cz;
        unsigned cx;
        unsigned cy;
        if (proximity) {
            cz = baton->centerzxy[0];
            cx = baton->centerzxy[1];
            cy = baton->centerzxy[2];
        } else {
            cz = 0;
            cx = 0;
            cy = 0;
        }

        // bbox (optional)
        bool bbox = !baton->bboxzxy.empty();
        unsigned bboxz;
        unsigned minx;
        unsigned miny;
        unsigned maxx;
        unsigned maxy;
        if (bbox) {
            bboxz = baton->bboxzxy[0];
            minx = baton->bboxzxy[1];
            miny = baton->bboxzxy[2];
            maxx = baton->bboxzxy[3];
            maxy = baton->bboxzxy[4];
        } else {
            bboxz = 0;
            minx = 0;
            miny = 0;
            maxx = 0;
            maxy = 0;
        }

        for (unsigned short i = 0; i < size; i++) {
            PhrasematchSubq const& subq = stack[i];

            std::string shardId = shard(4, subq.phrase);

            Cache::intarray grids = __get(subq.cache, type, shardId, subq.phrase);

            unsigned short z = subq.zoom;
            auto const& zCache = zoomCache[z];
            std::size_t zCacheSize = zCache.size();

            unsigned long m = grids.size();

            for (unsigned long j = 0; j < m; j++) {
                Cover cover = numToCover(grids[j]);
                cover.idx = subq.idx;
                cover.subq = i;
                cover.tmpid = static_cast<uint32_t>(cover.idx * POW2_25 + cover.id);
                cover.relev = cover.relev * subq.weight;
                if (proximity) {
                    ZXY dxy = pxy2zxy(z, cover.x, cover.y, cz);
                    cover.distance = tileDist(cx, dxy.x, cy, dxy.y);
                    cover.scoredist = scoredist(cz, cover.distance, cover.score);
                } else {
                    cover.distance = 0;
                    cover.scoredist = cover.score;
                }

                if (bbox) {
                    ZXY min = bxy2zxy(bboxz, minx, miny, z, false);
                    ZXY max = bxy2zxy(bboxz, maxx, maxy, z, true);
                    if (cover.x < min.x || cover.y < min.y || cover.x > max.x || cover.y > max.y) continue;
                }

                uint64_t zxy = (z * POW2_28) + (cover.x * POW2_14) + (cover.y);

                cit = coalesced.find(zxy);
                if (cit == coalesced.end()) {
                    std::vector<Cover> coverList;
                    coverList.push_back(cover);
                    coalesced.emplace(zxy, coverList);
                } else {
                    cit->second.push_back(cover);
                }

                dit = done.find(zxy);
                if (dit == done.end()) {
                    for (unsigned a = 0; a < zCacheSize; a++) {
                        uint64_t p = zCache[a];
                        double s = static_cast<double>(1 << (z-p));
                        uint64_t pxy = static_cast<uint64_t>(p * POW2_28) +
                            static_cast<uint64_t>(std::floor(cover.x/s) * POW2_14) +
                            static_cast<uint64_t>(std::floor(cover.y/s));
                        // Set a flag to ensure coalesce occurs only once per zxy.
                        pit = coalesced.find(pxy);
                        if (pit != coalesced.end()) {
                            cit = coalesced.find(zxy);
                            for (auto const& pArray : pit->second) {
                                cit->second.emplace_back(pArray);
                            }
                            done.emplace(zxy, true);
                            break;
                        }
                    }
                }
            }
        }

        std::vector<Context> contexts;
        for (auto const& matched : coalesced) {
            std::vector<Cover> const& coverList = matched.second;
            size_t coverSize = coverList.size();
            for (unsigned i = 0; i < coverSize; i++) {
                unsigned used = 1 << coverList[i].subq;
                unsigned lastMask = 0;
                double lastRelev = 0.0;
                double stacky = 0.0;

                unsigned coverPos = 0;

                Context context;
                context.coverList.emplace_back(coverList[i]);
                context.relev = coverList[i].relev;
                for (unsigned j = i+1; j < coverSize; j++) {
                    unsigned mask = 1 << coverList[j].subq;

                    // this cover is functionally identical with previous and
                    // is more relevant, replace the previous.
                    if (mask == lastMask && coverList[j].relev > lastRelev) {
                        context.relev -= lastRelev;
                        context.relev += coverList[j].relev;
                        context.coverList[coverPos] = coverList[j];

                        stacky = 1.0;
                        used = used | mask;
                        lastMask = mask;
                        lastRelev = coverList[j].relev;
                        coverPos++;
                    // this cover doesn't overlap with used mask.
                    } else if (!(used & mask)) {
                        context.coverList.emplace_back(coverList[j]);
                        context.relev += coverList[j].relev;

                        stacky = 1.0;
                        used = used | mask;
                        lastMask = mask;
                        lastRelev = coverList[j].relev;
                        coverPos++;
                    }
                    // all other cases conflict with existing mask. skip.
                }
                context.relev -= 0.01;
                context.relev += 0.01 * stacky;
                contexts.emplace_back(context);
            }
        }
        std::sort(contexts.begin(), contexts.end(), contextSortByRelev);
        coalesceFinalize(baton, contexts);
    } catch (std::exception const& ex) {
       baton->error = ex.what();
    }
}

}
