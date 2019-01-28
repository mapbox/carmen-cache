
#include "coalesce.hpp"
#include "memorycache.hpp"
#include "rocksdbcache.hpp"

namespace carmen {

std::vector<Context> coalesce(std::vector<PhrasematchSubq>& stack, const std::vector<uint64_t>& centerzxy, const std::vector<uint64_t>& bboxzxy, double radius) {
    std::vector<Context> contexts;
    if (stack.size() == 1) {
        contexts = coalesceSingle(stack, centerzxy, bboxzxy, radius);
    } else {
        contexts = coalesceMulti(stack, centerzxy, bboxzxy, radius);
    }

    std::vector<Context> out;
    if (!contexts.empty()) {
        // Coalesce stack, generate relevs.
        double relevMax = contexts[0].relev;
        std::size_t total = 0;
        std::map<uint64_t, bool> sets;
        std::map<uint64_t, bool>::iterator sit;
        std::size_t max_contexts = 40;
        out.reserve(max_contexts);
        for (auto&& context : contexts) {
            // Maximum allowance of coalesced features: 40.
            if (total >= max_contexts) break;

            // Since `coalesced` is sorted by relev desc at first
            // threshold miss we can break the loop.
            if (relevMax - context.relev >= 0.25) break;

            // Only collect each feature once.
            uint32_t id = context.coverList[0].tmpid;
            sit = sets.find(id);
            if (sit != sets.end()) continue;

            sets.emplace(id, true);
            out.emplace_back(std::move(context));
            total++;
        }
    }
    return out;
}

// behind the scenes, coalesce has two different strategies, depending on whether
// it's actually trying to stack multiple matches or whether it's considering a
// single match that consumes the entire query; this function handles the latter case
// and takes as a parameter the libuv task that contains info about the job it's supposed to do
inline std::vector<Context> coalesceSingle(std::vector<PhrasematchSubq>& stack, const std::vector<uint64_t>& centerzxy, const std::vector<uint64_t>& bboxzxy, double radius) {
    PhrasematchSubq const& subq = stack[0];

    // proximity (optional)
    bool proximity = !centerzxy.empty();
    unsigned cz;
    unsigned cx;
    unsigned cy;
    if (proximity) {
        cz = static_cast<unsigned>(centerzxy[0]);
        cx = static_cast<unsigned>(centerzxy[1]);
        cy = static_cast<unsigned>(centerzxy[2]);
    } else {
        cz = 0;
        cx = 0;
        cy = 0;
    }

    // bbox (optional)
    bool bbox = !bboxzxy.empty();
    unsigned minx;
    unsigned miny;
    unsigned maxx;
    unsigned maxy;
    if (bbox) {
        minx = static_cast<unsigned>(bboxzxy[1]);
        miny = static_cast<unsigned>(bboxzxy[2]);
        maxx = static_cast<unsigned>(bboxzxy[3]);
        maxy = static_cast<unsigned>(bboxzxy[4]);
    } else {
        minx = 0;
        miny = 0;
        maxx = 0;
        maxy = 0;
    }

    // Load and concatenate grids for all ids in `phrases`
    intarray grids;
    size_t max_results = subq.extended_scan ? std::numeric_limits<size_t>::max() : PREFIX_MAX_GRID_LENGTH;
    grids = subq.type == TYPE_MEMORY ? reinterpret_cast<MemoryCache*>(subq.cache)->__getmatching(subq.phrase, subq.prefix, subq.langfield, max_results) : reinterpret_cast<RocksDBCache*>(subq.cache)->__getmatching(subq.phrase, subq.prefix, subq.langfield, max_results);

    unsigned long m = grids.size();
    double relevMax = 0;
    std::vector<Cover> covers;

    uint32_t length = 0;
    uint32_t lastId = 0;
    double lastRelev = 0;
    double lastScoredist = 0;
    double lastDistance = 0;
    double minScoredist = std::numeric_limits<double>::max();
    for (unsigned long j = 0; j < m; j++) {
        Cover cover = numToCover(grids[j]);

        if (bbox) {
            if (cover.x < minx || cover.y < miny || cover.x > maxx || cover.y > maxy) continue;
        }

        cover.idx = subq.idx;
        cover.tmpid = static_cast<uint32_t>(cover.idx * POW2_25 + cover.id);
        cover.relev = cover.relev * subq.weight;
        if (proximity) {
            auto last = covers.empty() ? NULL : &covers.back();
            if (
                last != NULL &&
                last->x == cover.x &&
                last->y == cover.y &&
                last->score == cover.score
            ) {
                cover.distance = last->distance;
                cover.scoredist = last->scoredist;
            } else {
                cover.distance = tileDist(cx, cy, cover.x, cover.y);
                cover.scoredist = scoredist(cz, cover.distance, cover.score, radius);
            }
            if (!cover.matches_language && cover.distance > proximityRadius(cz, radius)) {
                cover.relev *= .96;
            }
        } else {
            cover.distance = 0;
            cover.scoredist = cover.score;
            if (!cover.matches_language) cover.relev *= .96;
        }

        // only add cover id if it's got a higer scoredist
        if (lastId == cover.id && cover.scoredist <= lastScoredist) continue;

        // short circuit based on relevMax thres
        if (length > 40) {
            if (cover.scoredist < minScoredist) continue;
            if (cover.relev < lastRelev) break;
        }
        if (relevMax - cover.relev >= 0.25) break;
        if (cover.relev > relevMax) relevMax = cover.relev;

        covers.emplace_back(cover);
        if (lastId != cover.id) length++;
        if (!proximity && length > 40) break;
        if (cover.scoredist < minScoredist) minScoredist = cover.scoredist;
        lastId = cover.id;
        lastRelev = cover.relev;
        lastScoredist = cover.scoredist;
        lastDistance = cover.distance;
    }

    // sort grids by distance to proximity point
    std::sort(covers.begin(), covers.end(), coverSortByRelev);

    uint32_t lastid = 0;
    std::size_t added = 0;
    std::vector<Context> contexts;
    std::size_t max_contexts = 40;
    for (auto&& cover : covers) {
        // Stop at 40 contexts
        if (added == max_contexts) break;

        // Attempt not to add the same feature but by diff cover twice
        if (lastid == cover.id) continue;

        lastid = cover.id;
        added++;

        double relev = cover.relev;
        uint32_t mask = 0;
        // have clang-tidy ignore the following line: it insists on stripping
        // the move, but that makes compilation fail
        contexts.emplace_back(std::move(cover), mask, relev); // NOLINT
    }
    return contexts;
}

// this function handles the case where stacking is occurring between multiple subqueries
// again, it takes a libuv task as a parameter
inline std::vector<Context> coalesceMulti(std::vector<PhrasematchSubq>& stack, const std::vector<uint64_t>& centerzxy, const std::vector<uint64_t>& bboxzxy, double radius) {
    std::sort(stack.begin(), stack.end(), subqSortByZoom);
    std::size_t stackSize = stack.size();

    // Cache zoom levels to iterate over as coalesce occurs.
    std::vector<intarray> zoomCache;
    zoomCache.reserve(stackSize);
    double maxrelev = 0;
    for (auto const& subq : stack) {
        zoomCache.emplace_back();
        auto& zooms = zoomCache.back();
        std::vector<bool> zoomUniq(22, false);
        for (auto const& subqB : stack) {
            if (subq.idx == subqB.idx) continue;
            if (zoomUniq[subqB.zoom]) continue;
            if (subq.zoom < subqB.zoom) continue;
            zoomUniq[subqB.zoom] = true;
            zooms.emplace_back(subqB.zoom);
        }
    }

    // Coalesce relevs into higher zooms, e.g.
    // z5 inherits relev of overlapping tiles at z4.
    // @TODO assumes sources are in zoom ascending order.
    std::map<uint64_t, std::vector<Context>> coalesced;
    std::map<uint64_t, std::vector<Context>>::iterator cit;
    std::map<uint64_t, std::vector<Context>>::iterator pit;
    std::map<uint64_t, bool> done;
    std::map<uint64_t, bool>::iterator dit;

    // proximity (optional)
    bool proximity = !centerzxy.empty();
    unsigned cz;
    unsigned cx;
    unsigned cy;
    if (proximity) {
        cz = static_cast<unsigned>(centerzxy[0]);
        cx = static_cast<unsigned>(centerzxy[1]);
        cy = static_cast<unsigned>(centerzxy[2]);
    } else {
        cz = 0;
        cx = 0;
        cy = 0;
    }

    // bbox (optional)
    bool bbox = !bboxzxy.empty();
    unsigned bboxz;
    unsigned minx;
    unsigned miny;
    unsigned maxx;
    unsigned maxy;
    if (bbox) {
        bboxz = static_cast<unsigned>(bboxzxy[0]);
        minx = static_cast<unsigned>(bboxzxy[1]);
        miny = static_cast<unsigned>(bboxzxy[2]);
        maxx = static_cast<unsigned>(bboxzxy[3]);
        maxy = static_cast<unsigned>(bboxzxy[4]);
    } else {
        bboxz = 0;
        minx = 0;
        miny = 0;
        maxx = 0;
        maxy = 0;
    }

    std::vector<Context> contexts;
    std::size_t i = 0;
    for (auto const& subq : stack) {
        // Load and concatenate grids for all ids in `phrases`
        intarray grids;
        grids = subq.type == TYPE_MEMORY ? reinterpret_cast<MemoryCache*>(subq.cache)->__getmatching(subq.phrase, subq.prefix, subq.langfield, PREFIX_MAX_GRID_LENGTH) : reinterpret_cast<RocksDBCache*>(subq.cache)->__getmatching(subq.phrase, subq.prefix, subq.langfield, PREFIX_MAX_GRID_LENGTH);

        bool first = i == 0;
        bool last = i == (stack.size() - 1);
        unsigned short z = subq.zoom;
        auto const& zCache = zoomCache[i];
        std::size_t zCacheSize = zCache.size();

        unsigned long m = grids.size();

        for (unsigned long j = 0; j < m; j++) {
            Cover cover = numToCover(grids[j]);
            cover.idx = subq.idx;
            cover.mask = subq.mask;
            cover.tmpid = static_cast<uint32_t>(cover.idx * POW2_25 + cover.id);
            cover.relev = cover.relev * subq.weight;
            if (proximity) {
                ZXY dxy = pxy2zxy(z, cover.x, cover.y, cz);
                cover.distance = tileDist(cx, cy, dxy.x, dxy.y);
                cover.scoredist = scoredist(cz, cover.distance, cover.score, radius);
                if (!cover.matches_language && cover.distance > proximityRadius(cz, radius)) {
                    cover.relev *= .96;
                }
            } else {
                cover.distance = 0;
                cover.scoredist = cover.score;
                if (!cover.matches_language) cover.relev *= .96;
            }

            if (bbox) {
                ZXY min = bxy2zxy(bboxz, minx, miny, z, false);
                ZXY max = bxy2zxy(bboxz, maxx, maxy, z, true);
                if (cover.x < min.x || cover.y < min.y || cover.x > max.x || cover.y > max.y) continue;
            }

            uint64_t zxy = (z * POW2_28) + (cover.x * POW2_14) + (cover.y);

            std::vector<Cover> covers;
            covers.push_back(cover);
            uint32_t context_mask = cover.mask;
            double context_relev = cover.relev;

            for (unsigned a = 0; a < zCacheSize; a++) {
                uint64_t p = zCache[a];
                auto s = static_cast<double>(1 << (z - p));
                uint64_t pxy = static_cast<uint64_t>(p * POW2_28) +
                               static_cast<uint64_t>(std::floor(cover.x / s) * POW2_14) +
                               static_cast<uint64_t>(std::floor(cover.y / s));
                pit = coalesced.find(pxy);
                if (pit != coalesced.end()) {
                    uint32_t lastMask = 0;
                    double lastRelev = 0.0;
                    for (auto const& parents : pit->second) {
                        for (auto const& parent : parents.coverList) {
                            // this cover is functionally identical with previous and
                            // is more relevant, replace the previous.
                            if (parent.mask == lastMask && parent.relev > lastRelev) {
                                covers.pop_back();
                                covers.emplace_back(parent);
                                context_relev -= lastRelev;
                                context_relev += parent.relev;
                                lastMask = parent.mask;
                                lastRelev = parent.relev;
                                // this cover doesn't overlap with used mask.
                            } else if ((context_mask & parent.mask) == 0u) {
                                covers.emplace_back(parent);
                                context_relev += parent.relev;
                                context_mask = context_mask | parent.mask;
                                lastMask = parent.mask;
                                lastRelev = parent.relev;
                            }
                        }
                    }
                }
            }
            maxrelev = std::max(maxrelev, context_relev);
            if (last) {
                // Slightly penalize contexts that have no stacking
                if (covers.size() == 1) {
                    context_relev -= 0.01;
                    // Slightly penalize contexts in ascending order
                } else if (covers[0].mask > covers[1].mask) {
                    context_relev -= 0.01;
                }
                if (maxrelev - context_relev < .25) {
                    contexts.emplace_back(std::move(covers), context_mask, context_relev);
                }
            } else if (first || covers.size() > 1) {
                cit = coalesced.find(zxy);
                if (cit == coalesced.end()) {
                    std::vector<Context> local_contexts;
                    local_contexts.emplace_back(std::move(covers), context_mask, context_relev);
                    coalesced.emplace(zxy, std::move(local_contexts));
                } else {
                    cit->second.emplace_back(std::move(covers), context_mask, context_relev);
                }
            }
        }

        i++;
    }

    // append coalesced to contexts by moving memory
    for (auto&& matched : coalesced) {
        for (auto&& context : matched.second) {
            if (maxrelev - context.relev < .25) {
                contexts.emplace_back(std::move(context));
            }
        }
    }

    std::sort(contexts.begin(), contexts.end(), contextSortByRelev);
    return contexts;
}

} // namespace carmen
