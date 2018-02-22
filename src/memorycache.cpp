
#include "memorycache.hpp"
#include "cpp_util.hpp"

namespace carmen {

using namespace v8;

Nan::Persistent<FunctionTemplate> MemoryCache::constructor;

intarray __get(MemoryCache const* c, std::string phrase, langfield_type langfield) {
    arraycache const& cache = c->cache_;
    intarray array;

    add_langfield(phrase, langfield);
    arraycache::const_iterator aitr = cache.find(phrase);
    if (aitr != cache.end()) {
        array = aitr->second;
    }
    std::sort(array.begin(), array.end(), std::greater<uint64_t>());
    return array;
}

intarray __getmatching(MemoryCache const* c, std::string phrase, bool match_prefixes, langfield_type langfield) {
    intarray array;

    if (!match_prefixes) phrase.push_back(LANGFIELD_SEPARATOR);
    size_t phrase_length = phrase.length();
    const char* phrase_data = phrase.data();

    // Load values from memory cache

    for (auto const& item : c->cache_) {
        const char* item_data = item.first.data();
        size_t item_length = item.first.length();

        if (item_length < phrase_length) continue;

        if (memcmp(phrase_data, item_data, phrase_length) == 0) {
            langfield_type message_langfield = extract_langfield(item.first);

            if (message_langfield & langfield) {
                array.reserve(array.size() + item.second.size());
                for (auto const& grid : item.second) {
                    array.emplace_back(grid | LANGUAGE_MATCH_BOOST);
                }
            } else {
                array.insert(array.end(), item.second.begin(), item.second.end());
            }
        }
    }
    std::sort(array.begin(), array.end(), std::greater<uint64_t>());
    return array;
}

void MemoryCache::Initialize(Handle<Object> target) {
    Nan::HandleScope scope;
    Local<FunctionTemplate> t = Nan::New<FunctionTemplate>(MemoryCache::New);
    t->InstanceTemplate()->SetInternalFieldCount(1);
    t->SetClassName(Nan::New("MemoryCache").ToLocalChecked());
    Nan::SetPrototypeMethod(t, "pack", MemoryCache::pack);
    Nan::SetPrototypeMethod(t, "list", MemoryCache::list);
    Nan::SetPrototypeMethod(t, "_set", _set);
    Nan::SetPrototypeMethod(t, "_get", _get);
    Nan::SetPrototypeMethod(t, "_getMatching", _getmatching);
    target->Set(Nan::New("MemoryCache").ToLocalChecked(), t->GetFunction());
    constructor.Reset(t);
}

MemoryCache::MemoryCache()
    : ObjectWrap(),
      cache_() {}

MemoryCache::~MemoryCache() {}

/**
 * creates database from filename
 * optimize a memory cache and write to disc as rocksdbcache
 *
 * @name pack
 * @memberof MemoryCache
 * @param {String}, filename
 * @returns {String}, filename
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const MemoryCache = new cache.MemoryCache('a');
 *
 * cache.pack('filename');
 *
 */

NAN_METHOD(MemoryCache::pack) {
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

        MemoryCache* c = node::ObjectWrap::Unwrap<MemoryCache>(info.This());

        std::unique_ptr<rocksdb::DB> db;
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::Status status = OpenDB(options, filename, db);

        if (!status.ok()) {
            return Nan::ThrowTypeError("unable to open rocksdb file for packing");
        }

        std::map<key_type, std::deque<value_type>> memoized_prefixes;

        for (auto const& item : c->cache_) {
            std::size_t array_size = item.second.size();
            if (array_size > 0) {
                // make copy of intarray so we can sort without
                // modifying the original array
                intarray varr = item.second;

                // delta-encode values, sorted in descending order.
                std::sort(varr.begin(), varr.end(), std::greater<uint64_t>());

                packVec(varr, db, item.first);

                std::string prefix_t1 = "";
                std::string prefix_t2 = "";

                // add this to the memoized prefix array too, maybe
                auto phrase_length = item.first.find(LANGFIELD_SEPARATOR);
                // use the full string for things shorter than the limit
                // or the prefix otherwise
                if (phrase_length < MEMO_PREFIX_LENGTH_T1) {
                    prefix_t1 = "=1" + item.first;
                } else {
                    // get the prefix, then append the langfield back onto it again
                    langfield_type langfield = extract_langfield(item.first);

                    prefix_t1 = "=1" + item.first.substr(0, MEMO_PREFIX_LENGTH_T1);
                    add_langfield(prefix_t1, langfield);

                    if (phrase_length < MEMO_PREFIX_LENGTH_T2) {
                        prefix_t2 = "=2" + item.first;
                    } else {
                        prefix_t2 = "=2" + item.first.substr(0, MEMO_PREFIX_LENGTH_T2);
                        add_langfield(prefix_t2, langfield);
                    }
                }

                if (prefix_t1 != "") {
                    std::map<key_type, std::deque<value_type>>::const_iterator mitr = memoized_prefixes.find(prefix_t1);
                    if (mitr == memoized_prefixes.end()) {
                        memoized_prefixes.emplace(prefix_t1, std::deque<value_type>());
                    }
                    std::deque<value_type>& buf = memoized_prefixes[prefix_t1];

                    buf.insert(buf.end(), varr.begin(), varr.end());
                }
                if (prefix_t2 != "") {
                    std::map<key_type, std::deque<value_type>>::const_iterator mitr = memoized_prefixes.find(prefix_t2);
                    if (mitr == memoized_prefixes.end()) {
                        memoized_prefixes.emplace(prefix_t2, std::deque<value_type>());
                    }
                    std::deque<value_type>& buf = memoized_prefixes[prefix_t2];

                    buf.insert(buf.end(), varr.begin(), varr.end());
                }
            }
        }

        for (auto const& item : memoized_prefixes) {
            // copy the deque into a vector so we can sort without
            // modifying the original array
            intarray varr(item.second.begin(), item.second.end());

            // delta-encode values, sorted in descending order.
            std::sort(varr.begin(), varr.end(), std::greater<uint64_t>());

            if (varr.size() > PREFIX_MAX_GRID_LENGTH) {
                // for the prefix memos we're only going to ever use 500k max anyway
                varr.resize(PREFIX_MAX_GRID_LENGTH);
            }

            packVec(varr, db, item.first);
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

/**
 * lists the data in the memory cache object
 *
 * @name list
 * @memberof MemoryCache
 * @param {String} id
 * @returns {Array}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const MemoryCache = new cache.MemoryCache('a');
 *
 * cache.list('a', (err, result) => {
 *    if (err) throw err;
 *    console.log(result);
 * });
 *
 */

NAN_METHOD(MemoryCache::list) {
    try {
        Nan::Utf8String utf8_value(info[0]);
        if (utf8_value.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        MemoryCache* c = node::ObjectWrap::Unwrap<MemoryCache>(info.This());
        Local<Array> ids = Nan::New<Array>();

        unsigned idx = 0;
        for (auto const& item : c->cache_) {
            Local<Array> out = Nan::New<Array>();
            out->Set(0, Nan::New(item.first.substr(0, item.first.find(LANGFIELD_SEPARATOR))).ToLocalChecked());

            langfield_type langfield = extract_langfield(item.first);
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
 * replaces the data in the object
 *
 * @name set
 * @memberof MemoryCache
 * @param {String} id
 * @param {Array}, data
 * @returns {String}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const MemoryCache = new cache.MemoryCache('a');
 *
 * cache.set('a', [1,2,3], (err, result) => {
 *      if (err) throw err;
 *      console.log(result)
 *});
 *
 */

NAN_METHOD(MemoryCache::_set) {
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

        MemoryCache* c = node::ObjectWrap::Unwrap<MemoryCache>(info.This());
        arraycache& arrc = c->cache_;
        key_type key_id = static_cast<key_type>(id);
        add_langfield(key_id, langfield);

        arraycache::iterator itr2 = arrc.find(key_id);
        if (itr2 == arrc.end()) {
            arrc.emplace(key_id, intarray());
        }
        intarray& vv = arrc[key_id];

        unsigned array_size = data->Length();
        if (append) {
            vv.reserve(vv.size() + array_size);
        } else {
            if (itr2 != arrc.end()) vv.clear();
            vv.reserve(array_size);
        }

        for (unsigned i = 0; i < array_size; ++i) {
            vv.emplace_back(static_cast<uint64_t>(data->Get(i)->NumberValue()));
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

/**
 * retrieves data by id
 *
 * @name get
 * @memberof MemoryCache
 * @param {String} id
 * @returns {String}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const MemoryCache = new cache.MemoryCache('a');
 *
 * cache.get('a', (err, result) => {
 *      if (err) throw err;
 *      console.log(object)
 *});
 *
 */

NAN_METHOD(MemoryCache::_get) {
    return _genericget<MemoryCache>(info);
}

/**
 * Create MemoryCache object which keeps phrases in memory for indexing reference
 *
 * @name MemoryCache
 * @memberof MemoryCache
 * @param {String} id
 * @returns {Object}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const MemoryCache = new cache.MemoryCache('a');
 *
 */

NAN_METHOD(MemoryCache::New) {
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
        MemoryCache* im = new MemoryCache();

        im->Wrap(info.This());
        info.This()->Set(Nan::New("id").ToLocalChecked(), info[0]);
        info.GetReturnValue().Set(info.This());
        return;
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
}

/**
 * get something that's matching
 *
 * @name getmatching
 * @memberof MemoryCache
 * @param {String} id
 * @returns {String}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const MemoryCache = new cache.MemoryCache('a');
 *
 * cache.getMatching('a');
 *
 */

NAN_METHOD(MemoryCache::_getmatching) {
    return _genericgetmatching<MemoryCache>(info);
}

} // namespace carmen
