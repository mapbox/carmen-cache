
#include "coalesce.hpp"
#include "binding.hpp"

namespace carmen {

using namespace v8;

/**
 * The PhrasematchSubqObject type describes the metadata known about possible matches to be assessed for stacking by
 * coalesce as seen from Javascript. Note: it is of similar purpose to the PhrasematchSubq C++ struct type, but differs
 * slightly in specific field names and types.
 *
 * @typedef PhrasematchSubqObject
 * @name PhrasematchSubqObject
 * @type {Object}
 * @property {String} phrase - The matched string
 * @property {Number} weight - A float between 0 and 1 representing how much of the query this string covers
 * @property {Boolean} prefix - whether or not to do a prefix scan (as opposed to an exact match scan); used for autocomplete
 * @property {Number} idx - an identifier of the index the match came from; opaque to carmen-cache but returned in results
 * @property {Number} zoom - the configured tile zoom level for the index
 * @property {Number} mask - a bitmask representing which tokens in the original query the subquery covers
 * @property {Number[]} languages - a list of the language IDs to be considered matching
 * @property {Object} cache - the carmen-cache from the index in which the match was found
 */

/**
  * @callback coalesceCallback
  * @param err - error if any, or null if not
  * @param {CoalesceResult[]} results - the results of the coalesce operation
  */

/**
 * A member of the result set from a coalesce operation.
 *
 * @typedef CoalesceResult
 * @name CoalesceResult
 * @type {Object}
 * @property {Number} x - the X tile coordinate of the result
 * @property {Number} y - the Y tile coordinate of the result
 * @property {Number} relev - the computed relevance of the result
 * @property {Number} score - the computed score of the result
 * @property {Number} id - the feature ID of the result
 * @property {Number} idx - the index ID (preserved from the inbound subquery) of the index the result came from
 * @property {Number} tmpid - a composite ID used for uniquely identifying features across indexes that incorporates the ID and IDX
 * @property {Number} distance - the distance metric computed using the feature and proximity, if supplied; 0 otherwise
 * @property {Number} scoredist - the composite score incorporating the feature's score with the distance (or the score if distance is 0)
 * @property {Boolean} matches_language - whether or not the match is valid for one of the languages in the inbound languages array
 */

/**
 * The coalesce function determines whether or not phrase matches in different
 * carmen indexes align spatially, and computes information about successful matches
 * such as combined relevance and score. The computation is done on the thread pool,
 * and exposed asynchronously to JS via a callback argument.
 *
 * @name coalesce
 * @param {PhrasematchSubqObject[]} phrasematches - an array of PhrasematchSubqObject objects, each of which describes a match candidate
 * @param {Object} options - options for how to perform the coalesce operation that aren't specific to a particular subquery
 * @param {Number} [options.radius] - the fall-off radius for determining how wide-reaching the effect of proximity bias is
 * @param {Number[]} [options.centerzxy] - a 3-number array representing the ZXY of the tile on which the proximity point can be found
 * @param {Number[]} [options.bboxzxy] - a 5-number array representing the zoom, minX, minY, maxX, and maxY values of the tile cover of the requested bbox, if any
 * @param {coalesceCallback} callback - the callback function
 */
NAN_METHOD(coalesce) {
    // PhrasematchStack (js => cpp)
    if (info.Length() < 3) {
        return Nan::ThrowTypeError("Expects 3 arguments: an array of PhrasematchSubqObjects, an option object, and a callback");
    }

    if (!info[0]->IsArray()) {
        return Nan::ThrowTypeError("Arg 1 must be a PhrasematchSubqObject array");
    }

    Local<Array> array = Local<Array>::Cast(info[0]);
    auto array_length = array->Length();
    if (array_length < 1) {
        return Nan::ThrowTypeError("Arg 1 must be an array with one or more PhrasematchSubqObjects");
    }

    // Options object (js => cpp)
    Local<Value> options_val = info[1];
    if (!options_val->IsObject()) {
        return Nan::ThrowTypeError("Arg 2 must be an options object");
    }
    Local<Object> options = options_val->ToObject();

    // callback
    Local<Value> callback = info[2];
    if (!callback->IsFunction()) {
        return Nan::ThrowTypeError("Arg 3 must be a callback function");
    }

    // We use unique_ptr here to manage the heap allocated CoalesceBaton
    // If an error is thrown the unique_ptr will go out of scope and delete
    // its underlying baton.
    // If no error is throw we release the underlying baton pointer before
    // heading into the threadpool since we assume it will be deleted manually in coalesceAfter
    std::unique_ptr<CoalesceBaton> baton_ptr = std::make_unique<CoalesceBaton>();
    CoalesceBaton* baton = baton_ptr.get();
    try {
        for (uint32_t i = 0; i < array_length; i++) {
            Local<Value> val = array->Get(i);
            if (!val->IsObject()) {
                return Nan::ThrowTypeError("All items in array must be valid PhrasematchSubqObjects");
            }
            Local<Object> jsStack = val->ToObject();
            if (jsStack->IsNull() || jsStack->IsUndefined()) {
                return Nan::ThrowTypeError("All items in array must be valid PhrasematchSubqObjects");
            }

            double weight;
            std::string phrase;
            bool prefix;
            unsigned short idx;
            unsigned short zoom;
            uint32_t mask;
            langfield_type langfield;

            // TODO: this is verbose: we could write some generic functions to do this robust conversion per type
            if (!jsStack->Has(Nan::New("idx").ToLocalChecked())) {
                return Nan::ThrowTypeError("missing idx property");
            } else {
                Local<Value> prop_val = jsStack->Get(Nan::New("idx").ToLocalChecked());
                if (!prop_val->IsNumber()) {
                    return Nan::ThrowTypeError("idx value must be a number");
                }
                int64_t _idx = prop_val->IntegerValue();
                if (_idx < 0 || _idx > std::numeric_limits<unsigned short>::max()) {
                    return Nan::ThrowTypeError("encountered idx value too large to fit in unsigned short");
                }
                idx = static_cast<unsigned short>(_idx);
            }

            if (!jsStack->Has(Nan::New("zoom").ToLocalChecked())) {
                return Nan::ThrowTypeError("missing zoom property");
            } else {
                Local<Value> prop_val = jsStack->Get(Nan::New("zoom").ToLocalChecked());
                if (!prop_val->IsNumber()) {
                    return Nan::ThrowTypeError("zoom value must be a number");
                }
                int64_t _zoom = prop_val->IntegerValue();
                if (_zoom < 0 || _zoom > std::numeric_limits<unsigned short>::max()) {
                    return Nan::ThrowTypeError("encountered zoom value too large to fit in unsigned short");
                }
                zoom = static_cast<unsigned short>(_zoom);
            }

            if (!jsStack->Has(Nan::New("weight").ToLocalChecked())) {
                return Nan::ThrowTypeError("missing weight property");
            } else {
                Local<Value> prop_val = jsStack->Get(Nan::New("weight").ToLocalChecked());
                if (!prop_val->IsNumber()) {
                    return Nan::ThrowTypeError("weight value must be a number");
                }
                double _weight = prop_val->NumberValue();
                if (_weight < 0 || _weight > std::numeric_limits<double>::max()) {
                    return Nan::ThrowTypeError("encountered weight value too large to fit in double");
                }
                weight = _weight;
            }

            if (!jsStack->Has(Nan::New("phrase").ToLocalChecked())) {
                return Nan::ThrowTypeError("missing phrase property");
            } else {
                Local<Value> prop_val = jsStack->Get(Nan::New("phrase").ToLocalChecked());
                if (!prop_val->IsString()) {
                    return Nan::ThrowTypeError("phrase value must be a string");
                }
                Nan::Utf8String _phrase(prop_val);
                if (_phrase.length() < 1) {
                    return Nan::ThrowTypeError("encountered invalid phrase");
                }
                phrase = *_phrase;
            }

            if (!jsStack->Has(Nan::New("prefix").ToLocalChecked())) {
                return Nan::ThrowTypeError("missing prefix property");
            } else {
                Local<Value> prop_val = jsStack->Get(Nan::New("prefix").ToLocalChecked());
                if (!prop_val->IsBoolean()) {
                    return Nan::ThrowTypeError("prefix value must be a boolean");
                }
                prefix = prop_val->BooleanValue();
            }

            if (!jsStack->Has(Nan::New("mask").ToLocalChecked())) {
                return Nan::ThrowTypeError("missing mask property");
            } else {
                Local<Value> prop_val = jsStack->Get(Nan::New("mask").ToLocalChecked());
                if (!prop_val->IsNumber()) {
                    return Nan::ThrowTypeError("mask value must be a number");
                }
                int64_t _mask = prop_val->IntegerValue();
                if (_mask < 0 || _mask > std::numeric_limits<uint32_t>::max()) {
                    return Nan::ThrowTypeError("encountered mask value too large to fit in uint32_t");
                }
                mask = static_cast<uint32_t>(_mask);
            }

            langfield = ALL_LANGUAGES;
            if (jsStack->Has(Nan::New("languages").ToLocalChecked())) {
                Local<Value> c_array = jsStack->Get(Nan::New("languages").ToLocalChecked());
                if (!c_array->IsArray()) {
                    return Nan::ThrowTypeError("languages must be an array");
                }
                Local<Array> carray = Local<Array>::Cast(c_array);
                langfield = langarrayToLangfield(carray);
            }

            if (!jsStack->Has(Nan::New("cache").ToLocalChecked())) {
                return Nan::ThrowTypeError("missing cache property");
            } else {
                Local<Value> prop_val = jsStack->Get(Nan::New("cache").ToLocalChecked());
                if (!prop_val->IsObject()) {
                    return Nan::ThrowTypeError("cache value must be a Cache object");
                }
                Local<Object> _cache = prop_val->ToObject();
                if (_cache->IsNull() || _cache->IsUndefined()) {
                    return Nan::ThrowTypeError("cache value must be a Cache object");
                }
                bool isMemoryCache = Nan::New(MemoryCache::constructor)->HasInstance(prop_val);
                bool isRocksDBCache = Nan::New(RocksDBCache::constructor)->HasInstance(prop_val);
                if (!(isMemoryCache || isRocksDBCache)) {
                    return Nan::ThrowTypeError("cache value must be a MemoryCache or RocksDBCache object");
                }
                if (isMemoryCache) {
                    baton->stack.emplace_back(
                        static_cast<void*>(node::ObjectWrap::Unwrap<MemoryCache>(_cache)),
                        TYPE_MEMORY,
                        weight,
                        phrase,
                        prefix,
                        idx,
                        zoom,
                        mask,
                        langfield);
                } else {
                    baton->stack.emplace_back(
                        static_cast<void*>(node::ObjectWrap::Unwrap<RocksDBCache>(_cache)),
                        TYPE_ROCKSDB,
                        weight,
                        phrase,
                        prefix,
                        idx,
                        zoom,
                        mask,
                        langfield);
                }
            }
        }

        if (options->Has(Nan::New("radius").ToLocalChecked())) {
            Local<Value> prop_val = options->Get(Nan::New("radius").ToLocalChecked());
            if (!prop_val->IsNumber()) {
                return Nan::ThrowTypeError("radius must be a number");
            }
            int64_t _radius = prop_val->IntegerValue();
            if (_radius < 0 || _radius > std::numeric_limits<unsigned>::max()) {
                return Nan::ThrowTypeError("encountered radius too large to fit in unsigned");
            }
            baton->radius = static_cast<double>(_radius);
        } else {
            baton->radius = 40.0;
        }

        if (options->Has(Nan::New("centerzxy").ToLocalChecked())) {
            Local<Value> c_array = options->Get(Nan::New("centerzxy").ToLocalChecked());
            if (!c_array->IsArray()) {
                return Nan::ThrowTypeError("centerzxy must be an array");
            }
            Local<Array> carray = Local<Array>::Cast(c_array);
            if (carray->Length() != 3) {
                return Nan::ThrowTypeError("centerzxy must be an array of 3 numbers");
            }
            baton->centerzxy.reserve(carray->Length());
            for (uint32_t i = 0; i < carray->Length(); ++i) {
                Local<Value> item = carray->Get(i);
                if (!item->IsNumber()) {
                    return Nan::ThrowTypeError("centerzxy values must be number");
                }
                int64_t a_val = item->IntegerValue();
                if (a_val < 0 || a_val > std::numeric_limits<uint32_t>::max()) {
                    return Nan::ThrowTypeError("encountered centerzxy value too large to fit in uint32_t");
                }
                baton->centerzxy.emplace_back(static_cast<uint32_t>(a_val));
            }
        }

        if (options->Has(Nan::New("bboxzxy").ToLocalChecked())) {
            Local<Value> c_array = options->Get(Nan::New("bboxzxy").ToLocalChecked());
            if (!c_array->IsArray()) {
                return Nan::ThrowTypeError("bboxzxy must be an array");
            }
            Local<Array> carray = Local<Array>::Cast(c_array);
            if (carray->Length() != 5) {
                return Nan::ThrowTypeError("bboxzxy must be an array of 5 numbers");
            }
            baton->bboxzxy.reserve(carray->Length());
            for (uint32_t i = 0; i < carray->Length(); ++i) {
                Local<Value> item = carray->Get(i);
                if (!item->IsNumber()) {
                    return Nan::ThrowTypeError("bboxzxy values must be number");
                }
                int64_t a_val = item->IntegerValue();
                if (a_val < 0 || a_val > std::numeric_limits<uint32_t>::max()) {
                    return Nan::ThrowTypeError("encountered bboxzxy value too large to fit in uint32_t");
                }
                baton->bboxzxy.emplace_back(static_cast<uint32_t>(a_val));
            }
        }

        baton->callback.Reset(callback.As<Function>());

        // queue work
        baton->request.data = baton;
        // Release the managed baton
        baton_ptr.release();
        // Reference count the cache objects
        for (auto& subq : baton->stack) {
            if (subq.type == TYPE_MEMORY)
                reinterpret_cast<MemoryCache*>(subq.cache)->_ref();
            else
                reinterpret_cast<RocksDBCache*>(subq.cache)->_ref();
        }
        // optimization: for stacks of 1, use coalesceSingle
        if (baton->stack.size() == 1) {
            uv_queue_work(uv_default_loop(), &baton->request, coalesceSingle, static_cast<uv_after_work_cb>(coalesceAfter));
        } else {
            uv_queue_work(uv_default_loop(), &baton->request, coalesceMulti, static_cast<uv_after_work_cb>(coalesceAfter));
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }

    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

// behind the scenes, coalesce has two different strategies, depending on whether
// it's actually trying to stack multiple matches or whether it's considering a
// single match that consumes the entire query; this function handles the latter case
// and takes as a parameter the libuv task that contains info about the job it's supposed to do
void coalesceSingle(uv_work_t* req) {
    CoalesceBaton* baton = static_cast<CoalesceBaton*>(req->data);

    try {
        std::vector<PhrasematchSubq> const& stack = baton->stack;
        PhrasematchSubq const& subq = stack[0];

        // proximity (optional)
        bool proximity = !baton->centerzxy.empty();
        unsigned cz;
        unsigned cx;
        unsigned cy;
        if (proximity) {
            cz = static_cast<unsigned>(baton->centerzxy[0]);
            cx = static_cast<unsigned>(baton->centerzxy[1]);
            cy = static_cast<unsigned>(baton->centerzxy[2]);
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
            minx = static_cast<unsigned>(baton->bboxzxy[1]);
            miny = static_cast<unsigned>(baton->bboxzxy[2]);
            maxx = static_cast<unsigned>(baton->bboxzxy[3]);
            maxy = static_cast<unsigned>(baton->bboxzxy[4]);
        } else {
            minx = 0;
            miny = 0;
            maxx = 0;
            maxy = 0;
        }

        // Load and concatenate grids for all ids in `phrases`
        intarray grids;
        grids = subq.type == TYPE_MEMORY ? __getmatching(reinterpret_cast<MemoryCache*>(subq.cache), subq.phrase, subq.prefix, subq.langfield) : __getmatching(reinterpret_cast<RocksDBCache*>(subq.cache), subq.phrase, subq.prefix, subq.langfield);

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

            cover.idx = subq.idx;
            cover.tmpid = static_cast<uint32_t>(cover.idx * POW2_25 + cover.id);
            cover.relev = cover.relev * subq.weight;
            if (!cover.matches_language) cover.relev *= .96;
            cover.distance = proximity ? tileDist(cx, cy, cover.x, cover.y) : 0;
            cover.scoredist = proximity ? scoredist(cz, cover.distance, cover.score, baton->radius) : cover.score;

            // only add cover id if it's got a higer scoredist
            if (lastId == cover.id && cover.scoredist <= lastScoredist) continue;

            // short circuit based on relevMax thres
            if (length > 40) {
                if (cover.scoredist < minScoredist) continue;
                if (cover.relev < lastRelev) break;
            }
            if (relevMax - cover.relev >= 0.25) break;
            if (cover.relev > relevMax) relevMax = cover.relev;

            if (bbox) {
                if (cover.x < minx || cover.y < miny || cover.x > maxx || cover.y > maxy) continue;
            }

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
            contexts.emplace_back(std::move(cover), mask, relev);
        }
        coalesceFinalize(baton, std::move(contexts));
    } catch (std::exception const& ex) {
        baton->error = ex.what();
    }
}

// this function handles the case where stacking is occurring between multiple subqueries
// again, it takes a libuv task as a parameter
void coalesceMulti(uv_work_t* req) {
    CoalesceBaton* baton = static_cast<CoalesceBaton*>(req->data);

    try {
        std::vector<PhrasematchSubq>& stack = baton->stack;
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
        bool proximity = baton->centerzxy.size() > 0;
        unsigned cz;
        unsigned cx;
        unsigned cy;
        if (proximity) {
            cz = static_cast<unsigned>(baton->centerzxy[0]);
            cx = static_cast<unsigned>(baton->centerzxy[1]);
            cy = static_cast<unsigned>(baton->centerzxy[2]);
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
            bboxz = static_cast<unsigned>(baton->bboxzxy[0]);
            minx = static_cast<unsigned>(baton->bboxzxy[1]);
            miny = static_cast<unsigned>(baton->bboxzxy[2]);
            maxx = static_cast<unsigned>(baton->bboxzxy[3]);
            maxy = static_cast<unsigned>(baton->bboxzxy[4]);
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
            grids = subq.type == TYPE_MEMORY ? __getmatching(reinterpret_cast<MemoryCache*>(subq.cache), subq.phrase, subq.prefix, subq.langfield) : __getmatching(reinterpret_cast<RocksDBCache*>(subq.cache), subq.phrase, subq.prefix, subq.langfield);

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
                if (!cover.matches_language) cover.relev *= .96;
                if (proximity) {
                    ZXY dxy = pxy2zxy(z, cover.x, cover.y, cz);
                    cover.distance = tileDist(cx, cy, dxy.x, dxy.y);
                    cover.scoredist = scoredist(cz, cover.distance, cover.score, baton->radius);
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

                std::vector<Cover> covers;
                covers.push_back(std::move(cover));
                uint32_t context_mask = cover.mask;
                double context_relev = cover.relev;

                for (unsigned a = 0; a < zCacheSize; a++) {
                    uint64_t p = zCache[a];
                    double s = static_cast<double>(1 << (z - p));
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
                                } else if (!(context_mask & parent.mask)) {
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
                if (last) {
                    // Slightly penalize contexts that have no stacking
                    if (covers.size() == 1) {
                        context_relev -= 0.01;
                        // Slightly penalize contexts in ascending order
                    } else if (covers[0].mask > covers[1].mask) {
                        context_relev -= 0.01;
                    }
                    contexts.emplace_back(std::move(covers), context_mask, context_relev);
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
                contexts.emplace_back(std::move(context));
            }
        }

        std::sort(contexts.begin(), contexts.end(), contextSortByRelev);
        coalesceFinalize(baton, std::move(contexts));
    } catch (std::exception const& ex) {
        baton->error = ex.what();
    }
}

// we don't use the 'status' parameter, but it's required as part of the uv_after_work_cb
// function signature, so suppress the warning about it
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
// this function handles getting the results of either coalesceSingle or coalesceMulti
// ready to be passed back to JS land, and queues up a callback invokation on the main thread
void coalesceAfter(uv_work_t* req, int status) {
    Nan::HandleScope scope;
    CoalesceBaton* baton = static_cast<CoalesceBaton*>(req->data);

    // Reference count the cache objects
    for (auto& subq : baton->stack) {
        if (subq.type == TYPE_MEMORY)
            reinterpret_cast<MemoryCache*>(subq.cache)->_unref();
        else
            reinterpret_cast<RocksDBCache*>(subq.cache)->_unref();
    }

    if (!baton->error.empty()) {
        v8::Local<v8::Value> argv[1] = {Nan::Error(baton->error.c_str())};
        Nan::MakeCallback(Nan::GetCurrentContext()->Global(), Nan::New(baton->callback), 1, argv);
    } else {
        std::vector<Context> const& features = baton->features;

        Local<Array> jsFeatures = Nan::New<Array>(static_cast<int>(features.size()));
        for (uint32_t i = 0; i < features.size(); i++) {
            jsFeatures->Set(i, contextToArray(features[i]));
        }

        Local<Value> argv[2] = {Nan::Null(), jsFeatures};
        Nan::MakeCallback(Nan::GetCurrentContext()->Global(), Nan::New(baton->callback), 2, argv);
    }

    baton->callback.Reset();
    delete baton;
}
#pragma clang diagnostic pop

// this function tabulates the results of either coalesceSingle or coalesceMulti in
// a uniform way
void coalesceFinalize(CoalesceBaton* baton, std::vector<Context>&& contexts) {
    if (!contexts.empty()) {
        // Coalesce stack, generate relevs.
        double relevMax = contexts[0].relev;
        std::size_t total = 0;
        std::map<uint64_t, bool> sets;
        std::map<uint64_t, bool>::iterator sit;
        std::size_t max_contexts = 40;
        baton->features.reserve(max_contexts);
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
            baton->features.emplace_back(std::move(context));
            total++;
        }
    }
}

} // namespace carmen
