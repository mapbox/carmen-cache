#include "binding.hpp"

namespace carmen {

using namespace v8;

// temporary, to make the signatures of the rocksdb and memory cache versions match
template <class T>
intarray __get(JSCache<T>* jsc, std::string phrase, langfield_type langfield) {
    return jsc->cache.__get(phrase, langfield);
}

// temporary, to make the signatures of the rocksdb and memory cache versions match
template <class T>
intarray __getmatching(JSCache<T>* jsc, std::string phrase, bool match_prefixes, langfield_type langfield) {
    return jsc->cache.__getmatching(phrase, match_prefixes, langfield);
}

template intarray __get<MemoryCache>(JSCache<MemoryCache>*, std::string phrase, langfield_type langfield);
template intarray __get<RocksDBCache>(JSCache<RocksDBCache>*, std::string phrase, langfield_type langfield);
template intarray __getmatching<MemoryCache>(JSCache<MemoryCache>*, std::string phrase, bool match_prefixes, langfield_type langfield);
template intarray __getmatching<RocksDBCache>(JSCache<RocksDBCache>*, std::string phrase, bool match_prefixes, langfield_type langfield);

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
  * @param {Boolean} matches_prefixes: T if it matches exactly, F: if it does not
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
 * @param {Boolean} matches_prefixes: T if it matches exactly, F: if it does not
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
    if (!info[1]->IsBoolean()) {
        return Nan::ThrowTypeError("second arg must be a Bool");
    }
    try {
        Nan::Utf8String utf8_id(info[0]);
        if (utf8_id.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        std::string id(*utf8_id);

        bool match_prefixes = info[1]->BooleanValue();

        langfield_type langfield;
        if (info.Length() > 2 && !(info[2]->IsNull() || info[2]->IsUndefined())) {
            if (!info[2]->IsArray()) {
                return Nan::ThrowTypeError("third arg, if supplied must be an Array");
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

extern "C" {
static void start(Handle<Object> target) {
    JSMemoryCache::Initialize(target);
    JSRocksDBCache::Initialize(target);
    NormalizationCache::Initialize(target);
    Nan::SetMethod(target, "coalesce", coalesce);
}
}

} // namespace carmen

// this macro expansion includes an old-style cast and is beyond our control
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wold-style-cast"
NODE_MODULE(carmen, carmen::start)
#pragma clang diagnostic pop
