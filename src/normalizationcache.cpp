
#include "normalizationcache.hpp"

namespace carmen {

using namespace v8;

Nan::Persistent<FunctionTemplate> NormalizationCache::constructor;

/**
 * NormalizationCache represents an one-to-many integer-to-integer
 * mapping where each integer is assumed to be the position of a given term
 * in a lexicographically-sorted vocabulary. The purpose of the cache is to capture
 * equivalencies between different elements in the dictionary, such that further metadata
 * (e.g., in a RocksDBCache) can be stored only for the canonical form of a given name.
 * The structure is stored using a RocksDB database on disk.
 * @class NormalizationCache
 *
 */

void NormalizationCache::Initialize(Handle<Object> target) {
    Nan::HandleScope scope;
    Local<FunctionTemplate> t = Nan::New<FunctionTemplate>(NormalizationCache::New);
    t->InstanceTemplate()->SetInternalFieldCount(1);
    t->SetClassName(Nan::New("NormalizationCache").ToLocalChecked());
    Nan::SetPrototypeMethod(t, "get", get);
    Nan::SetPrototypeMethod(t, "getPrefixRange", getprefixrange);
    Nan::SetPrototypeMethod(t, "getAll", getall);
    Nan::SetPrototypeMethod(t, "writeBatch", writebatch);

    target->Set(Nan::New("NormalizationCache").ToLocalChecked(), t->GetFunction());
    constructor.Reset(t);
}

NormalizationCache::NormalizationCache()
    : ObjectWrap(),
      db() {}

NormalizationCache::~NormalizationCache() {}

class UInt32Comparator : public rocksdb::Comparator {
  public:
    UInt32Comparator(const UInt32Comparator&) = delete;
    UInt32Comparator& operator=(const UInt32Comparator&) = delete;
    UInt32Comparator() = default;

    int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
        uint32_t ia = 0, ib = 0;
        if (a.size() >= sizeof(uint32_t)) memcpy(&ia, a.data(), sizeof(uint32_t));
        if (b.size() >= sizeof(uint32_t)) memcpy(&ib, b.data(), sizeof(uint32_t));

        if (ia < ib) return -1;
        if (ia > ib) return +1;
        return 0;
    }

    const char* Name() const override { return "UInt32Comparator"; }

// these function signatures are mandated by rocksdb, so suppress warnings about not
// using all the parameters
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
    void FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const override {}
    void FindShortSuccessor(std::string* key) const override {}
#pragma clang diagnostic pop
};
UInt32Comparator UInt32ComparatorInstance;

/**
 * Constructor for NormalizationCache pointing to an on-disk RocksDB database
 * to be used for reading or writing.
 *
 * @name NormalizationCache
 * @memberof NormalizationCache
 * @param {String} filename
 * @param {String} read-only
 * @returns {Object}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const nc = new cache.NormalizationCache('file.norm.rocksdb', false);
 *
 */

NAN_METHOD(NormalizationCache::New) {
    if (!info.IsConstructCall()) {
        return Nan::ThrowTypeError("Cannot call constructor as function, you need to use 'new' keyword");
    }
    try {
        if (info.Length() < 2) {
            return Nan::ThrowTypeError("expected arguments 'filename' and 'read-only'");
        }
        if (!info[0]->IsString()) {
            return Nan::ThrowTypeError("first argument 'filename' must be a String");
        }
        if (!info[1]->IsBoolean()) {
            return Nan::ThrowTypeError("second argument 'read-only' must be a Boolean");
        }

        Nan::Utf8String utf8_filename(info[0]);
        if (utf8_filename.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        std::string filename(*utf8_filename);
        bool read_only = info[1]->BooleanValue();

        std::unique_ptr<rocksdb::DB> db;
        rocksdb::Options options;
        options.create_if_missing = true;
        options.comparator = &UInt32ComparatorInstance;

        rocksdb::Status status;
        if (read_only) {
            status = OpenForReadOnlyDB(options, filename, db);
        } else {
            status = OpenDB(options, filename, db);
        }

        if (!status.ok()) {
            return Nan::ThrowTypeError("unable to open rocksdb file for normalization cache");
        }
        NormalizationCache* im = new NormalizationCache();
        im->db = std::move(db);
        im->Wrap(info.This());
        info.This()->Set(Nan::New("id").ToLocalChecked(), info[0]);
        info.GetReturnValue().Set(info.This());
        return;
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
}

/**
 * retrieve the indices of the canonical labels for the index of a given non-canonical label
 *
 * @name get
 * @memberof NormalizationCache
 * @param {Number} id
 * @returns {Array}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const nc = new cache.NormalizationCache('file.norm.rocksdb', true);
 *
 * // for a normalization cache for the dictionary ['main st', 'main street']
 * // where 'main st' is canonical
 * const canonical = nc.get(1); // returns [0]
 */

NAN_METHOD(NormalizationCache::get) {
    if (info.Length() < 1) {
        return Nan::ThrowTypeError("expected one info: id");
    }
    if (!info[0]->IsNumber()) {
        return Nan::ThrowTypeError("first arg must be a Number");
    }

    uint32_t id = static_cast<uint32_t>(info[0]->IntegerValue());
    std::string sid(reinterpret_cast<const char*>(&id), sizeof(uint32_t));

    NormalizationCache* c = node::ObjectWrap::Unwrap<NormalizationCache>(info.This());
    std::shared_ptr<rocksdb::DB> db = c->db;

    std::string message;
    bool found;
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), sid, &message);
    found = s.ok();

    size_t message_length = message.size();
    if (found && message_length >= sizeof(uint32_t)) {
        Local<Array> out = Nan::New<Array>();
        uint32_t entry;
        for (uint32_t i = 0; i * sizeof(uint32_t) < message_length; i++) {
            memcpy(&entry, message.data() + (i * sizeof(uint32_t)), sizeof(uint32_t));
            out->Set(i, Nan::New(entry));
        }
        info.GetReturnValue().Set(out);
        return;
    } else {
        info.GetReturnValue().Set(Nan::Undefined());
        return;
    }
}

/**
 * given that in a lexicographically sorted list, all terms that share a prefix
 * are grouped together, this function retrieves the indices of all canonical forms
 * of all terms that share a given prefix as indicated by the index of the first term
 * in the shared prefix list and the number of terms that share a prefix, for which the canonical
 * form does not also share that same prefix
 *
 * @name getPrefixRange
 * @memberof NormalizationCache
 * @param {Number} start_id
 * @param {Number} count
 * @param {Number} [scan_max] - the maximum number of entries to scan
 * @param {Number} [return_max] - the maximum number of indices to return
 * @returns {Array}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const nc = new cache.NormalizationCache('file.norm.rocksdb', true);
 *
 * // for a normalization cache for the dictionary
 * // ['saint marks ave', 'saint peters ave', 'st marks ave', 'st peters ave']
 * // where the 'st ...' forms are canonical
 * const canonical = nc.getPrefixRange(0, 2); // looks up all the canonical
 *                                            // forms for things that begin with
 *                                            // 'saint'
 */

NAN_METHOD(NormalizationCache::getprefixrange) {
    if (info.Length() < 1) {
        return Nan::ThrowTypeError("expected at least two info: start_id, count, [scan_max], [return_max]");
    }
    if (!info[0]->IsNumber()) {
        return Nan::ThrowTypeError("first arg must be a Number");
    }
    if (!info[1]->IsNumber()) {
        return Nan::ThrowTypeError("second arg must be a Number");
    }

    uint32_t scan_max = 100;
    uint32_t return_max = 10;
    if (info.Length() > 2) {
        if (!info[2]->IsNumber()) {
            return Nan::ThrowTypeError("third arg, if supplied, must be a Number");
        } else {
            scan_max = static_cast<uint32_t>(info[2]->IntegerValue());
        }
    }
    if (info.Length() > 3) {
        if (!info[3]->IsNumber()) {
            return Nan::ThrowTypeError("third arg, if supplied, must be a Number");
        } else {
            return_max = static_cast<uint32_t>(info[3]->IntegerValue());
        }
    }

    uint32_t start_id = static_cast<uint32_t>(info[0]->IntegerValue());
    std::string sid(reinterpret_cast<const char*>(&start_id), sizeof(uint32_t));
    uint32_t count = static_cast<uint32_t>(info[1]->IntegerValue());
    uint32_t ceiling = start_id + count;

    uint32_t scan_count = 0, return_count = 0;

    Local<Array> out = Nan::New<Array>();
    unsigned out_idx = 0;

    NormalizationCache* c = node::ObjectWrap::Unwrap<NormalizationCache>(info.This());
    std::shared_ptr<rocksdb::DB> db = c->db;

    std::unique_ptr<rocksdb::Iterator> rit(db->NewIterator(rocksdb::ReadOptions()));
    for (rit->Seek(sid); rit->Valid(); rit->Next()) {
        std::string skey = rit->key().ToString();
        uint32_t key;
        memcpy(&key, skey.data(), sizeof(uint32_t));

        if (key >= ceiling) break;

        uint32_t val;
        std::string svalue = rit->value().ToString();
        for (uint32_t offset = 0; offset < svalue.length(); offset += sizeof(uint32_t)) {
            memcpy(&val, svalue.data() + offset, sizeof(uint32_t));
            if (val < start_id || val >= ceiling) {
                out->Set(out_idx++, Nan::New(val));

                return_count++;
                if (return_count >= return_max) break;
            }
        }

        scan_count++;
        if (scan_count >= scan_max) break;
    }

    info.GetReturnValue().Set(out);
    return;
}

/**
 * retrieve the entire contents of a NormalizationCache, as an array of arrays
 *
 * @name getAll
 * @memberof NormalizationCache
 * @returns {Array}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const nc = new cache.NormalizationCache('file.norm.rocksdb', true);
 *
 * // for a normalization cache for the dictionary
 * // ['saint marks ave', 'saint peters ave', 'st marks ave', 'st peters ave']
 * // where the 'st ...' forms are canonical
 * const canonical = nc.getAll() // returns [[0, [2]], [1, [3]]]
 */
NAN_METHOD(NormalizationCache::getall) {
    Local<Array> out = Nan::New<Array>();
    unsigned out_idx = 0;

    NormalizationCache* c = node::ObjectWrap::Unwrap<NormalizationCache>(info.This());
    std::shared_ptr<rocksdb::DB> db = c->db;

    std::unique_ptr<rocksdb::Iterator> rit(db->NewIterator(rocksdb::ReadOptions()));
    for (rit->SeekToFirst(); rit->Valid(); rit->Next()) {
        std::string skey = rit->key().ToString();
        uint32_t key = *reinterpret_cast<const uint32_t*>(skey.data());

        std::string svalue = rit->value().ToString();

        Local<Array> row = Nan::New<Array>();
        row->Set(0, Nan::New(key));

        Local<Array> vals = Nan::New<Array>();
        uint32_t entry;
        for (uint32_t i = 0; i * sizeof(uint32_t) < svalue.length(); i++) {
            memcpy(&entry, svalue.data() + (i * sizeof(uint32_t)), sizeof(uint32_t));
            vals->Set(i, Nan::New(entry));
        }

        row->Set(1, vals);

        out->Set(out_idx++, row);
    }

    info.GetReturnValue().Set(out);
    return;
}

/**
 * bulk-set the contents of a NormalizationCache to an array of arrays
 *
 * @name writeBatch
 * @memberof NormalizationCache
 * @param {Array} data - the values to be written to the cache, in the form [[from, [to, to, ...]], ...]
 * @returns {Array}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const nc = new cache.NormalizationCache('file.norm.rocksdb', true);
 *
 * // for a normalization cache for the dictionary
 * // ['saint marks ave', 'saint peters ave', 'st marks ave', 'st peters ave']
 * // where the 'st ...' forms are canonical
 * nc.writeBatch([[0, [2]], [1, [3]]]);
 */
NAN_METHOD(NormalizationCache::writebatch) {
    if (info.Length() < 1) {
        return Nan::ThrowTypeError("expected one info: data");
    }
    if (!info[0]->IsArray()) {
        return Nan::ThrowTypeError("first arg must be an Array");
    }
    Local<Array> data = Local<Array>::Cast(info[0]);
    if (data->IsNull() || data->IsUndefined()) {
        return Nan::ThrowTypeError("an array expected for first argument");
    }

    NormalizationCache* c = node::ObjectWrap::Unwrap<NormalizationCache>(info.This());
    std::shared_ptr<rocksdb::DB> db = c->db;

    rocksdb::WriteBatch batch;
    for (uint32_t i = 0; i < data->Length(); i++) {
        if (!data->Get(i)->IsArray()) return Nan::ThrowTypeError("second argument must be an array of arrays");
        Local<Array> row = Local<Array>::Cast(data->Get(i));

        if (row->Length() != 2) return Nan::ThrowTypeError("each element must have two values");

        uint32_t key = static_cast<uint32_t>(row->Get(0)->IntegerValue());
        std::string skey(reinterpret_cast<const char*>(&key), sizeof(uint32_t));

        std::string svalue("");

        Local<Value> nvalue = row->Get(1);
        uint32_t ivalue;
        if (nvalue->IsNumber()) {
            ivalue = static_cast<uint32_t>(nvalue->IntegerValue());
            svalue.append(reinterpret_cast<const char*>(&ivalue), sizeof(uint32_t));
        } else if (nvalue->IsArray()) {
            Local<Array> nvalue_arr = Local<Array>::Cast(nvalue);
            if (!nvalue_arr->IsNull() && !nvalue_arr->IsUndefined()) {
                for (uint32_t j = 0; j < nvalue_arr->Length(); j++) {
                    ivalue = static_cast<uint32_t>(nvalue_arr->Get(j)->IntegerValue());
                    svalue.append(reinterpret_cast<const char*>(&ivalue), sizeof(uint32_t));
                }
            } else {
                return Nan::ThrowTypeError("values should be either numbers or arrays of numbers");
            }
        } else {
            return Nan::ThrowTypeError("values should be either numbers or arrays of numbers");
        }

        batch.Put(skey, svalue);
    }
    db->Write(rocksdb::WriteOptions(), &batch);

    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

} // namespace carmen
