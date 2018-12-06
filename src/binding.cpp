#include "binding.hpp"

namespace carmen {

using namespace v8;

template <>
void JSCache<RocksDBCache>::Initialize(Handle<Object> target) {
    Nan::HandleScope scope;
    Local<FunctionTemplate> t = Nan::New<FunctionTemplate>(JSCache::New);
    t->InstanceTemplate()->SetInternalFieldCount(1);
    t->SetClassName(Nan::New("RocksDBCache").ToLocalChecked());
    Nan::SetPrototypeMethod(t, "pack", JSRocksDBCache::pack);
    Nan::SetPrototypeMethod(t, "list", JSRocksDBCache::list);
    Nan::SetPrototypeMethod(t, "_get", _get);
    Nan::SetPrototypeMethod(t, "_getMatching", _getmatching);
    target->Set(Nan::New("RocksDBCache").ToLocalChecked(), t->GetFunction());
    constructor.Reset(t);
}

template <>
void JSCache<MemoryCache>::Initialize(Handle<Object> target) {
    Nan::HandleScope scope;
    Local<FunctionTemplate> t = Nan::New<FunctionTemplate>(JSCache::New);
    t->InstanceTemplate()->SetInternalFieldCount(1);
    t->SetClassName(Nan::New("MemoryCache").ToLocalChecked());
    Nan::SetPrototypeMethod(t, "pack", JSMemoryCache::pack);
    Nan::SetPrototypeMethod(t, "list", JSMemoryCache::list);
    Nan::SetPrototypeMethod(t, "_set", _set);
    Nan::SetPrototypeMethod(t, "_get", _get);
    Nan::SetPrototypeMethod(t, "_getMatching", _getmatching);
    target->Set(Nan::New("MemoryCache").ToLocalChecked(), t->GetFunction());
    constructor.Reset(t);
}

template <class T>
JSCache<T>::JSCache()
    : ObjectWrap(),
      cache() {}

template <class T>
JSCache<T>::~JSCache() {}

/**
 * Writes an identical copy JSCache from another JSCache; not really used
 *
 * @name pack
 * @memberof JSCache
 * @param {String}, filename
 * @returns {Boolean}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const JSCache = new cache.JSCache('a');
 *
 * cache.pack('filename');
 *
 */

template <class T>
NAN_METHOD(JSCache<T>::pack) {
    if (info.Length() < 1) {
        return Nan::ThrowTypeError("expected one info: 'filename'");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first argument must be a String");
    }
    try {
        Nan::Utf8String utf8_filename(info[0]);
        if (utf8_filename.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        std::string filename(*utf8_filename);

        T* c = &(node::ObjectWrap::Unwrap<JSCache<T>>(info.This())->cache);

        try {
            c->pack(filename);
        } catch (std::exception const& ex) {
            return Nan::ThrowTypeError(ex.what());
        }
        info.GetReturnValue().Set(true);
        return;
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
}

/**
 * lists the keys in the JSCache object
 *
 * @name list
 * @memberof JSCache
 * @param {String} id
 * @returns {Array} Set of keys/ids
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const JSCache = new cache.JSCache('a');
 *
 * cache.list('a', (err, result) => {
 *    if (err) throw err;
 *    console.log(result);
 * });
 *
 */

template <class T>
NAN_METHOD(JSCache<T>::list) {
    try {
        T* c = &(node::ObjectWrap::Unwrap<JSCache<T>>(info.This())->cache);
        Local<Array> ids = Nan::New<Array>();

        std::vector<std::pair<std::string, langfield_type>> results = c->list();

        unsigned idx = 0;
        for (auto const& tuple : results) {
            Local<Array> out = Nan::New<Array>();
            out->Set(0, Nan::New(tuple.first).ToLocalChecked());

            langfield_type langfield = tuple.second;
            if (langfield == ALL_LANGUAGES) {
                out->Set(1, Nan::Null());
            } else {
                out->Set(1, langfieldToLangarray(langfield));
            }

            ids->Set(idx++, out);
        }

        info.GetReturnValue().Set(ids);
        return;
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
}

/**
* Creates an in-memory key-value store mapping phrases  and language IDs
* to lists of corresponding grids (grids ie are integer representations of occurrences of the phrase within an index)
*
 * @name JSCache
 * @memberof JSCache
 * @param {String} id
 * @param {String} filename
 * @returns {Object}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const JSCache = new cache.JSCache('a', 'filename');
 *
 */

template <>
NAN_METHOD(JSCache<RocksDBCache>::New) {
    if (!info.IsConstructCall()) {
        return Nan::ThrowTypeError("Cannot call constructor as function, you need to use 'new' keyword");
    }
    try {
        if (info.Length() < 2) {
            return Nan::ThrowTypeError("expected arguments 'id' and 'filename'");
        }
        if (!info[0]->IsString()) {
            return Nan::ThrowTypeError("first argument 'id' must be a String");
        }
        if (!info[1]->IsString()) {
            return Nan::ThrowTypeError("second argument 'filename' must be a String");
        }

        Nan::Utf8String utf8_filename(info[1]);
        if (utf8_filename.length() < 1) {
            return Nan::ThrowTypeError("second arg must be a String");
        }
        std::string filename(*utf8_filename);

        JSCache<RocksDBCache>* im = new JSCache<RocksDBCache>();
        im->cache = RocksDBCache(filename);
        im->Wrap(info.This());
        info.This()->Set(Nan::New("id").ToLocalChecked(), info[0]);
        info.GetReturnValue().Set(info.This());
        return;
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
}

/**
 * Creates an in-memory key-value store mapping phrases  and language IDs
 * to lists of corresponding grids (grids ie are integer representations of occurrences of the phrase within an index)
 *
 * @name MemoryCache
 * @memberof MemoryCache
 * @param {String} id
 * @returns {Array} grid of integers
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const MemoryCache = new cache.MemoryCache(id, languages);
 *
 */

template <>
NAN_METHOD(JSCache<MemoryCache>::New) {
    if (!info.IsConstructCall()) {
        return Nan::ThrowTypeError("Cannot call constructor as function, you need to use 'new' keyword");
    }
    try {
        if (info.Length() < 1) {
            return Nan::ThrowTypeError("expected 'id' argument");
        }
        if (!info[0]->IsString()) {
            return Nan::ThrowTypeError("first argument 'id' must be a String");
        }

        JSCache<MemoryCache>* im = new JSCache<MemoryCache>();
        im->cache = MemoryCache();
        im->Wrap(info.This());
        info.This()->Set(Nan::New("id").ToLocalChecked(), info[0]);
        info.GetReturnValue().Set(info.This());
        return;
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
}

/**
  * Retrieves data exactly matching phrase and language settings by id
  *
  * @name get
  * @memberof JSCache
  * @param {String} id
  * @param {Array} optional; array of languages
  * @returns {Array} integers referring to grids
  * @example
  * const cache = require('@mapbox/carmen-cache');
  * const JSCache = new cache.JSCache('a');
  *
  * JSCache.get(id, languages);
  *  // => [grid, grid, grid, grid... ]
  *
  */

template <class T>
NAN_METHOD(JSCache<T>::_get) {
    if (info.Length() < 1) {
        return Nan::ThrowTypeError("expected at least one info: id, [languages]");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first arg must be a String");
    }
    try {
        Nan::Utf8String utf8_id(info[0]);
        if (utf8_id.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        std::string id(*utf8_id);

        langfield_type langfield;
        if (info.Length() > 1 && !(info[1]->IsNull() || info[1]->IsUndefined())) {
            if (!info[1]->IsArray()) {
                return Nan::ThrowTypeError("second arg, if supplied must be an Array");
            }
            langfield = langarrayToLangfield(Local<Array>::Cast(info[1]));
        } else {
            langfield = ALL_LANGUAGES;
        }

        T* c = &(node::ObjectWrap::Unwrap<JSCache<T>>(info.This())->cache);
        intarray vector = c->__get(id, langfield);
        if (!vector.empty()) {
            std::size_t size = vector.size();
            Local<Array> array = Nan::New<Array>(static_cast<int>(size));
            for (uint32_t i = 0; i < size; ++i) {
                array->Set(i, Nan::New<Number>(vector[i]));
            }
            info.GetReturnValue().Set(array);
            return;
        } else {
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
}

/**
 * Retrieves grid that at least partially matches phrase and/or language inputs
 *
 * @name get
 * @memberof JSCache
 * @param {String} id
 * @param {Number} matches_prefix - whether or do an exact match (0), prefix scan(1), or word boundary scan(2); used for autocomplete 
 * @param {Array} optional; array of languages
 * @returns {Array} integers referring to grids
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const JSCache = new cache.JSCache('a');
 *
 * JSCache.get(id, languages);
 *  // => [grid, grid, grid, grid... ]
 *
 */

template <class T>
NAN_METHOD(JSCache<T>::_getmatching) {
    if (info.Length() < 2) {
        return Nan::ThrowTypeError("expected two or three info: id, match_prefixes, [languages]");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first arg must be a String");
    }
    if (!info[1]->IsNumber()) {
        return Nan::ThrowTypeError("second arg must be an integer between 0 - 2");
    }
    try {
        Nan::Utf8String utf8_id(info[0]);
        if (utf8_id.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        std::string id(*utf8_id);

        int32_t int32_prefix = info[1]->Int32Value();
        if (int32_prefix < 0 || int32_prefix > 2) {
            return Nan::ThrowTypeError("second arg must be an integer between 0 - 2");
        }
        PrefixMatch match_prefixes = static_cast<PrefixMatch>(int32_prefix);

        langfield_type langfield;
        if (info.Length() > 2 && !(info[2]->IsNull() || info[2]->IsUndefined())) {
            if (!info[2]->IsArray()) {
                return Nan::ThrowTypeError("third arg, if supplied, must be an Array");
            }
            langfield = langarrayToLangfield(Local<Array>::Cast(info[2]));
        } else {
            langfield = ALL_LANGUAGES;
        }

        T* c = &(node::ObjectWrap::Unwrap<JSCache<T>>(info.This())->cache);
        intarray vector = c->__getmatching(id, match_prefixes, langfield);
        if (!vector.empty()) {
            std::size_t size = vector.size();
            Local<Array> array = Nan::New<Array>(static_cast<int>(size));
            for (uint32_t i = 0; i < size; ++i) {
                auto obj = coverToObject(numToCover(vector[i]));

                // these values don't make any sense outside the context of coalesce, so delete them
                // it's a little clunky to set and then delete them, but this function as exposed
                // to node is only used in debugging/testing, so, meh
                obj->Delete(Nan::New("idx").ToLocalChecked());
                obj->Delete(Nan::New("tmpid").ToLocalChecked());
                obj->Delete(Nan::New("distance").ToLocalChecked());
                obj->Delete(Nan::New("scoredist").ToLocalChecked());
                array->Set(i, obj);
            }
            info.GetReturnValue().Set(array);
            return;
        } else {
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
}

template <>
NAN_METHOD(JSCache<MemoryCache>::_set) {
    if (info.Length() < 2) {
        return Nan::ThrowTypeError("expected at least two info: id, data, [languages], [append]");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first arg must be a String");
    }
    if (!info[1]->IsArray()) {
        return Nan::ThrowTypeError("second arg must be an Array");
    }
    Local<Array> data = Local<Array>::Cast(info[1]);
    if (data->IsNull() || data->IsUndefined()) {
        return Nan::ThrowTypeError("an array expected for second argument");
    }
    try {

        Nan::Utf8String utf8_id(info[0]);
        if (utf8_id.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        std::string id(*utf8_id);

        langfield_type langfield;
        if (info.Length() > 2 && !(info[2]->IsNull() || info[2]->IsUndefined())) {
            if (!info[2]->IsArray()) {
                return Nan::ThrowTypeError("third arg, if supplied must be an Array");
            }
            langfield = langarrayToLangfield(Local<Array>::Cast(info[2]));
        } else {
            langfield = ALL_LANGUAGES;
        }

        bool append = info.Length() > 3 && info[3]->IsBoolean() && info[3]->BooleanValue();

        unsigned array_size = data->Length();
        auto vec_data = intarray();
        vec_data.reserve(array_size);

        for (unsigned i = 0; i < array_size; ++i) {
            vec_data.emplace_back(static_cast<uint64_t>(data->Get(i)->NumberValue()));
        }

        MemoryCache* c = &(node::ObjectWrap::Unwrap<JSMemoryCache>(info.This())->cache);
        c->_set(id, vec_data, langfield, append);
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

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
 * @property {Number} prefix - whether or do an exact match (0), prefix scan(1), or word boundary scan(2); used for autocomplete
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
NAN_METHOD(JSCoalesce) {
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
            PrefixMatch prefix;
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
                if (!prop_val->IsNumber()) {
                    return Nan::ThrowTypeError("prefix value must be a integer between 0 - 2");
                }

                int32_t int32_prefix = prop_val->Int32Value();
                if (int32_prefix < 0 || int32_prefix > 2) {
                    return Nan::ThrowTypeError("prefix value must be a integer between 0 - 2");
                }
                prefix = static_cast<PrefixMatch>(int32_prefix);
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
                bool isMemoryCache = Nan::New(JSMemoryCache::constructor)->HasInstance(prop_val);
                bool isRocksDBCache = Nan::New(JSRocksDBCache::constructor)->HasInstance(prop_val);
                if (!(isMemoryCache || isRocksDBCache)) {
                    return Nan::ThrowTypeError("cache value must be a MemoryCache or RocksDBCache object");
                }
                if (isMemoryCache) {
                    auto unwrapped = node::ObjectWrap::Unwrap<JSMemoryCache>(_cache);
                    unwrapped->_ref();
                    baton->stack.emplace_back(
                        static_cast<void*>(&(unwrapped->cache)),
                        TYPE_MEMORY,
                        weight,
                        phrase,
                        prefix,
                        idx,
                        zoom,
                        mask,
                        langfield);
                    baton->refs.emplace_back(std::make_pair(TYPE_MEMORY, static_cast<void*>(unwrapped)));
                } else {
                    auto unwrapped = node::ObjectWrap::Unwrap<JSRocksDBCache>(_cache);
                    unwrapped->_ref();
                    baton->stack.emplace_back(
                        static_cast<void*>(&(unwrapped->cache)),
                        TYPE_ROCKSDB,
                        weight,
                        phrase,
                        prefix,
                        idx,
                        zoom,
                        mask,
                        langfield);
                    baton->refs.emplace_back(std::make_pair(TYPE_ROCKSDB, static_cast<void*>(unwrapped)));
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
        uv_queue_work(uv_default_loop(), &baton->request, jsCoalesceTask, static_cast<uv_after_work_cb>(jsCoalesceAfter));
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }

    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

void jsCoalesceTask(uv_work_t* req) {
    CoalesceBaton* baton = static_cast<CoalesceBaton*>(req->data);
    try {
        baton->features = coalesce(baton->stack, baton->centerzxy, baton->bboxzxy, baton->radius);
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
void jsCoalesceAfter(uv_work_t* req, int status) {
    Nan::HandleScope scope;
    CoalesceBaton* baton = static_cast<CoalesceBaton*>(req->data);

    // Reference count the cache objects
    for (auto& ref : baton->refs) {
        if (ref.first == TYPE_MEMORY)
            reinterpret_cast<JSMemoryCache*>(ref.second)->_unref();
        else
            reinterpret_cast<JSRocksDBCache*>(ref.second)->_unref();
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

extern "C" {
static void start(Handle<Object> target) {
    JSMemoryCache::Initialize(target);
    JSRocksDBCache::Initialize(target);
    Nan::SetMethod(target, "coalesce", JSCoalesce);
}
}

} // namespace carmen

// this macro expansion includes an old-style cast and is beyond our control
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wold-style-cast"
NODE_MODULE(carmen, carmen::start)
#pragma clang diagnostic pop
