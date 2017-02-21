
#include "binding.hpp"

#include <sstream>
#include <cmath>
#include <cassert>
#include <cstring>
#include <limits>
#include <algorithm>
#include <memory>

#include <protozero/pbf_writer.hpp>
#include <protozero/pbf_reader.hpp>

#include <chrono>
typedef std::chrono::high_resolution_clock Clock;

namespace carmen {

using namespace v8;

Nan::Persistent<FunctionTemplate> MemoryCache::constructor;
Nan::Persistent<FunctionTemplate> RocksDBCache::constructor;

rocksdb::Status OpenDB(const rocksdb::Options& options, const std::string& name, std::unique_ptr<rocksdb::DB>& dbptr) {
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, name, &db);
    dbptr = std::move(std::unique_ptr<rocksdb::DB>(db));
    return status;
}

rocksdb::Status OpenForReadOnlyDB(const rocksdb::Options& options, const std::string& name, std::unique_ptr<rocksdb::DB>& dbptr) {
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::OpenForReadOnly(options, name, &db);
    dbptr = std::move(std::unique_ptr<rocksdb::DB>(db));
    return status;
}

intarray __get(MemoryCache const* c, std::string id, bool ignorePrefixFlag) {
    arraycache const& cache = c->cache_;
    intarray array;

    arraycache::const_iterator aitr = cache.find(id);
    if (aitr == cache.end()) {
        if (ignorePrefixFlag) {
            arraycache::const_iterator aitr2 = cache.find(id + ".");
            if (aitr2 != cache.end()) {
                // we only found . suffix
                return aitr2->second;
            }
        }
        // we didn't find the non-.-suffix version and didn't look for the .-suffix version
    } else {
        if (ignorePrefixFlag) {
            arraycache::const_iterator aitr2 = cache.find(id + ".");
            if (aitr2 == cache.end()) {
                // we found only non-.-suffix
                return aitr->second;
            } else {
                // we found both; make a copy
                std::vector<uint64_t> combined = aitr->second;
                combined.insert(combined.end(), aitr2->second.begin(), aitr2->second.end());
                std::inplace_merge(combined.begin(), combined.begin() + aitr->second.size(), combined.end(), std::greater<uint64_t>());
                return combined;
            }
        }
        // we found the non-.-suffix version (but didn't look for the .-suffix version)
        return aitr->second;
    }
    return array;
}

intarray __get(RocksDBCache const* c, std::string id, bool ignorePrefixFlag) {
    std::shared_ptr<rocksdb::DB> db = c->db;
    intarray array;

    std::string search_id = id;
    std::vector<size_t> end_positions;
    for (uint32_t i = 0; i < 2; i++) {
        std::string message;
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), search_id, &message);
        if (s.ok()) {
            protozero::pbf_reader item(message);
            item.next(CACHE_ITEM);
            auto vals = item.get_packed_uint64();
            uint64_t lastval = 0;
            // delta decode values.
            for (auto it = vals.first; it != vals.second; ++it) {
                if (lastval == 0) {
                    lastval = *it;
                    array.emplace_back(lastval);
                } else {
                    lastval = lastval - *it;
                    array.emplace_back(lastval);
                }
            }
        }
        end_positions.emplace_back(array.size());
        if (i == 0) {
            if (!ignorePrefixFlag) {
                break;
            } else {
                search_id = search_id + ".";
            }
        }
    }

    if (end_positions.size() == 2 && end_positions[0] != 0 && end_positions[0] != end_positions[1]) {
        // we've found things for both the non-dot version and the dot version
        // so we need to merge them so the order is sorted
        std::inplace_merge(array.begin(), array.begin() + end_positions[0], array.end(), std::greater<uint64_t>());
    }
    return array;
}

struct sortableGrid {
    protozero::const_varint_iterator<uint64_t> it;
    protozero::const_varint_iterator<uint64_t> end;
};

intarray __getbyprefix(MemoryCache const* c, std::string prefix) {
    intarray array;
    size_t prefix_length = prefix.length();
    const char* prefix_cstr = prefix.c_str();

    // Load values from memory cache

    for (auto const& item : c->cache_) {
        const char* item_cstr = item.first.c_str();
        size_t item_length = item.first.length();
        // here, we skip this iteration if either the key is shorter than
        // the prefix, or, if the key has the no-prefix flag, its length
        // without the flag isn't the same as the item length (which will
        // make the prefix comparison in the next step effectively an
        // equality check)
        if (
            item_length < prefix_length ||
            (item_cstr[item_length - 1] == '.' && item_length != prefix_length + 1)
        ) {
            continue;
        }
        if (memcmp(prefix_cstr, item_cstr, prefix_length) == 0) {
            array.insert(array.end(), item.second.begin(), item.second.end());
        }
    }
    std::sort(array.begin(), array.end(), std::greater<uint64_t>());
    return array;
}

intarray __getbyprefix(RocksDBCache const* c, std::string prefix) {
    intarray array;
    size_t prefix_length = prefix.length();
    const char* prefix_cstr = prefix.c_str();

    // Load values from message cache
    std::vector<std::string> messages;
    std::vector<sortableGrid> grids;

    if (prefix_length <= MEMO_PREFIX_LENGTH_T1) {
        prefix = "=1" + prefix.substr(0, MEMO_PREFIX_LENGTH_T1);
    } else if (prefix_length <= MEMO_PREFIX_LENGTH_T2) {
        prefix = "=2" + prefix.substr(0, MEMO_PREFIX_LENGTH_T2);
    }
    radix_max_heap::pair_radix_max_heap<uint64_t, size_t> rh;

    std::shared_ptr<rocksdb::DB> db = c->db;

    std::unique_ptr<rocksdb::Iterator> rit(db->NewIterator(rocksdb::ReadOptions()));
    for (rit->Seek(prefix); rit->Valid() && rit->key().ToString().compare(0, prefix.size(), prefix) == 0; rit->Next()) {
        std::string key_id = rit->key().ToString();

        // same skip operation as for the memory cache; see above
        if (
            key_id.length() < prefix.length() ||
            (key_id.at(key_id.length() - 1) == '.' && key_id.length() != prefix.length() + 1)
        ) {
            continue;
        }

        messages.emplace_back(rit->value().ToString());
    }
    for (std::string& message : messages) {
        protozero::pbf_reader item(message);
        item.next(CACHE_ITEM);
        auto vals = item.get_packed_uint64();

        if (vals.first != vals.second) {
            grids.emplace_back(sortableGrid{vals.first, vals.second});
            rh.push(*(vals.first), grids.size() - 1);
        }
    }

    while (!rh.empty() && array.size() < PREFIX_MAX_GRID_LENGTH) {
        size_t gridIdx = rh.top_value();
        uint64_t lastval = rh.top_key();
        rh.pop();

        array.emplace_back(lastval);
        grids[gridIdx].it++;
        if (grids[gridIdx].it != grids[gridIdx].end) {
            lastval = lastval - *(grids[gridIdx].it);
            rh.push(lastval, gridIdx);
        }
    }

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
    Nan::SetPrototypeMethod(t, "_getByPrefix", _getbyprefix);
    Nan::SetMethod(t, "coalesce", coalesce);
    target->Set(Nan::New("MemoryCache").ToLocalChecked(), t->GetFunction());
    constructor.Reset(t);
}

MemoryCache::MemoryCache()
  : ObjectWrap(),
    cache_() {}

MemoryCache::~MemoryCache() { }

void RocksDBCache::Initialize(Handle<Object> target) {
    Nan::HandleScope scope;
    Local<FunctionTemplate> t = Nan::New<FunctionTemplate>(RocksDBCache::New);
    t->InstanceTemplate()->SetInternalFieldCount(1);
    t->SetClassName(Nan::New("RocksDBCache").ToLocalChecked());
    Nan::SetPrototypeMethod(t, "pack", RocksDBCache::pack);
    Nan::SetPrototypeMethod(t, "merge", merge);
    Nan::SetPrototypeMethod(t, "list", RocksDBCache::list);
    Nan::SetPrototypeMethod(t, "_get", _get);
    Nan::SetPrototypeMethod(t, "_getByPrefix", _getbyprefix);
    Nan::SetMethod(t, "coalesce", coalesce);
    target->Set(Nan::New("RocksDBCache").ToLocalChecked(), t->GetFunction());
    constructor.Reset(t);
}

RocksDBCache::RocksDBCache()
  : ObjectWrap(),
    db() {}

RocksDBCache::~RocksDBCache() { }

inline void __packVec(intarray const& varr, std::unique_ptr<rocksdb::DB> const& db, std::string const& key) {
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

NAN_METHOD(MemoryCache::pack)
{
    if (info.Length() < 2) {
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

                __packVec(varr, db, item.first);

                std::string prefix_t1 = "";
                std::string prefix_t2 = "";

                // add this to the memoized prefix array too, maybe
                auto item_length = item.first.length();
                if (item.first.at(item_length - 1) == '.') {
                    // this is an entry that bans degens
                    // so only include it if it itself smaller than the
                    // prefix limit (minus dot), and leave it dot-suffixed
                    if (item_length <= (MEMO_PREFIX_LENGTH_T1 + 1)) {
                        prefix_t1 = "=1" + item.first;
                    } else if (item_length > (MEMO_PREFIX_LENGTH_T1 + 1) && item_length <= (MEMO_PREFIX_LENGTH_T2 + 1)) {
                        prefix_t2 = "=2" + item.first;
                    }
                } else {
                    // use the full string for things shorter than the limit
                    // or the prefix otherwise
                    if (item_length < MEMO_PREFIX_LENGTH_T1) {
                        prefix_t1 = "=1" + item.first;
                    } else {
                        prefix_t1 = "=1" + item.first.substr(0, MEMO_PREFIX_LENGTH_T1);
                        if (item_length < MEMO_PREFIX_LENGTH_T2) {
                            prefix_t2 = "=2" + item.first;
                        } else {
                            prefix_t2 = "=2" + item.first.substr(0, MEMO_PREFIX_LENGTH_T2);
                        }
                    }
                }

                if (prefix_t1 != "") {
                    std::map<key_type, std::deque<value_type>>::const_iterator mitr = memoized_prefixes.find(prefix_t1);
                    if (mitr == memoized_prefixes.end()) {
                        memoized_prefixes.emplace(prefix_t1, std::deque<value_type>());
                    }
                    std::deque<value_type> & buf = memoized_prefixes[prefix_t1];

                    buf.insert(buf.end(), varr.begin(), varr.end());
                }
                if (prefix_t2 != "") {
                    std::map<key_type, std::deque<value_type>>::const_iterator mitr = memoized_prefixes.find(prefix_t2);
                    if (mitr == memoized_prefixes.end()) {
                        memoized_prefixes.emplace(prefix_t2, std::deque<value_type>());
                    }
                    std::deque<value_type> & buf = memoized_prefixes[prefix_t2];

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

            __packVec(varr, db, item.first);
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(RocksDBCache::pack)
{
    if (info.Length() < 2) {
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

        RocksDBCache* c = node::ObjectWrap::Unwrap<RocksDBCache>(info.This());

        if (c->db && c->db->GetName() == filename) {
            return Nan::ThrowTypeError("rocksdb file is already loaded read-only; unload first");
        } else {
            std::shared_ptr<rocksdb::DB> existing = c->db;

            std::unique_ptr<rocksdb::DB> db;
            rocksdb::Options options;
            options.create_if_missing = true;
            rocksdb::Status status = OpenDB(options, filename, db);

            if (!status.ok()) {
                return Nan::ThrowTypeError("unable to open rocksdb file for packing");
            }

            // if what we have now is already a rocksdb, and it's a different
            // one from what we're being asked to pack into, copy from one to the other
            std::unique_ptr<rocksdb::Iterator> existingIt(existing->NewIterator(rocksdb::ReadOptions()));
            for (existingIt->SeekToFirst(); existingIt->Valid(); existingIt->Next()) {
                db->Put(rocksdb::WriteOptions(), existingIt->key(), existingIt->value());
            }
        }
        info.GetReturnValue().Set(true);
        return;
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

struct MergeBaton : carmen::noncopyable {
    uv_work_t request;
    std::string filename1;
    std::string filename2;
    std::string filename3;
    std::string method;
    std::string error;
    Nan::Persistent<v8::Function> callback;
};

void mergeQueue(uv_work_t* req) {
    MergeBaton *baton = static_cast<MergeBaton *>(req->data);
    std::string const& filename1 = baton->filename1;
    std::string const& filename2 = baton->filename2;
    std::string const& filename3 = baton->filename3;
    std::string const& method = baton->method;

    // input 1
    std::unique_ptr<rocksdb::DB> db1;
    rocksdb::Options options1;
    options1.create_if_missing = true;
    rocksdb::Status status1 = OpenForReadOnlyDB(options1, filename1, db1);
    if (!status1.ok()) {
        return Nan::ThrowTypeError("unable to open rocksdb input file #1");
    }

    // input 2
    std::unique_ptr<rocksdb::DB> db2;
    rocksdb::Options options2;
    options2.create_if_missing = true;
    rocksdb::Status status2 = OpenForReadOnlyDB(options2, filename2, db2);
    if (!status2.ok()) {
        return Nan::ThrowTypeError("unable to open rocksdb input file #2");
    }

    // output
    std::unique_ptr<rocksdb::DB> db3;
    rocksdb::Options options3;
    options3.create_if_missing = true;
    rocksdb::Status status3 = OpenDB(options3, filename3, db3);
    if (!status1.ok()) {
        return Nan::ThrowTypeError("unable to open rocksdb output file");
    }

    // Ids that have been seen
    std::map<key_type,bool> ids1;
    std::map<key_type,bool> ids2;

    try {
        // Store ids from 1
        std::unique_ptr<rocksdb::Iterator> it1(db1->NewIterator(rocksdb::ReadOptions()));
        for (it1->SeekToFirst(); it1->Valid(); it1->Next()) {
            ids1.emplace(it1->key().ToString(), true);
        }

        // Store ids from 2
        std::unique_ptr<rocksdb::Iterator> it2(db2->NewIterator(rocksdb::ReadOptions()));
        for (it2->SeekToFirst(); it2->Valid(); it2->Next()) {
            ids2.emplace(it2->key().ToString(), true);
        }

        // No delta writes from message1
        it1 = std::unique_ptr<rocksdb::Iterator>(db1->NewIterator(rocksdb::ReadOptions()));
        for (it1->SeekToFirst(); it1->Valid(); it1->Next()) {
            std::string key_id = it1->key().ToString();

            // Skip this id if also in message 2
            if (ids2.find(key_id) != ids2.end()) continue;

            // get input proto
            std::string in_message = it1->value().ToString();
            protozero::pbf_reader item(in_message);
            item.next(CACHE_ITEM);

            std::string message;
            message.clear();

            protozero::pbf_writer item_writer(message);
            {
                protozero::packed_field_uint64 field{item_writer, 1};
                auto vals = item.get_packed_uint64();
                for (auto it = vals.first; it != vals.second; ++it) {
                    field.add_element(static_cast<uint64_t>(*it));
                }
            }

            rocksdb::Status putStatus = db3->Put(rocksdb::WriteOptions(), key_id, message);
            assert(putStatus.ok());
        }

        // No delta writes from message2
        it2 = std::unique_ptr<rocksdb::Iterator>(db2->NewIterator(rocksdb::ReadOptions()));
        for (it2->SeekToFirst(); it2->Valid(); it2->Next()) {
            std::string key_id = it2->key().ToString();


            // Skip this id if also in message 1
            if (ids1.find(key_id) != ids1.end()) continue;

            // get input proto
            std::string in_message = it2->value().ToString();
            protozero::pbf_reader item(in_message);
            item.next(CACHE_ITEM);

            std::string message;
            message.clear();

            protozero::pbf_writer item_writer(message);
            {
                protozero::packed_field_uint64 field{item_writer, 1};
                auto vals = item.get_packed_uint64();
                for (auto it = vals.first; it != vals.second; ++it) {
                    field.add_element(static_cast<uint64_t>(*it));
                }
            }

            rocksdb::Status putStatus = db3->Put(rocksdb::WriteOptions(), key_id, message);
            assert(putStatus.ok());
        }

        // Delta writes for ids in both message1 and message2
        it1 = std::unique_ptr<rocksdb::Iterator>(db1->NewIterator(rocksdb::ReadOptions()));
        for (it1->SeekToFirst(); it1->Valid(); it1->Next()) {
            std::string key_id = it1->key().ToString();

            // Skip ids that are only in one or the other lists
            if (ids1.find(key_id) == ids1.end() || ids2.find(key_id) == ids2.end()) continue;

            // get input proto
            std::string in_message1 = it1->value().ToString();
            protozero::pbf_reader item(in_message1);
            item.next(CACHE_ITEM);

            uint64_t lastval = 0;
            intarray varr;

            // Add values from filename1
            auto vals = item.get_packed_uint64();
            for (auto it = vals.first; it != vals.second; ++it) {
                if (method == "freq") {
                    varr.emplace_back(*it);
                    break;
                } else if (lastval == 0) {
                    lastval = *it;
                    varr.emplace_back(lastval);
                } else {
                    lastval = lastval - *it;
                    varr.emplace_back(lastval);
                }
            }

            std::string in_message2;
            rocksdb::Status s = db2->Get(rocksdb::ReadOptions(), key_id, &in_message2);
            if (s.ok()) {
                // get input proto 2
                protozero::pbf_reader item2(in_message2);
                item2.next(CACHE_ITEM);

                auto vals2 = item2.get_packed_uint64();
                lastval = 0;
                for (auto it = vals2.first; it != vals2.second; ++it) {
                    if (method == "freq") {
                        if (key_id == "__MAX__") {
                            varr[0] = varr[0] > *it ? varr[0] : *it;
                        } else {
                            varr[0] = varr[0] + *it;
                        }
                        break;
                    } else if (lastval == 0) {
                        lastval = *it;
                        varr.emplace_back(lastval);
                    } else {
                        lastval = lastval - *it;
                        varr.emplace_back(lastval);
                    }
                }
            }

            // Sort for proper delta encoding
            std::sort(varr.begin(), varr.end(), std::greater<uint64_t>());

            // if this is the merging of a prefix cache entry
            // (which would start with '=' and have been truncated)
            // truncate the merged result
            if (key_id.at(0) == '=' && varr.size() > PREFIX_MAX_GRID_LENGTH) {
                varr.resize(PREFIX_MAX_GRID_LENGTH);
            }

            // Write varr to merged protobuf
            std::string message;
            message.clear();

            protozero::pbf_writer item_writer(message);
            {
                protozero::packed_field_uint64 field{item_writer, 1};
                lastval = 0;
                for (auto const& vitem : varr) {
                    if (lastval == 0) {
                        field.add_element(static_cast<uint64_t>(vitem));
                    } else {
                        field.add_element(static_cast<uint64_t>(lastval - vitem));
                    }
                    lastval = vitem;
                }
            }

            rocksdb::Status putStatus = db3->Put(rocksdb::WriteOptions(), key_id, message);
            assert(putStatus.ok());
        }

    } catch (std::exception const& ex) {
        baton->error = ex.what();
    }
}

void mergeAfter(uv_work_t* req) {
    Nan::HandleScope scope;
    MergeBaton *baton = static_cast<MergeBaton *>(req->data);
    if (!baton->error.empty()) {
        v8::Local<v8::Value> argv[1] = { Nan::Error(baton->error.c_str()) };
        Nan::MakeCallback(Nan::GetCurrentContext()->Global(), Nan::New(baton->callback), 1, argv);
    } else {
        Local<Value> argv[2] = { Nan::Null() };
        Nan::MakeCallback(Nan::GetCurrentContext()->Global(), Nan::New(baton->callback), 1, argv);
    }
    baton->callback.Reset();
    delete baton;
}

NAN_METHOD(MemoryCache::list)
{
    if (info.Length() < 1) {
        return Nan::ThrowTypeError("expected one arg: 'type'");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first argument must be a String");
    }

    try {
        Nan::Utf8String utf8_value(info[0]);
        if (utf8_value.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        std::string type(*utf8_value);
        MemoryCache* c = node::ObjectWrap::Unwrap<MemoryCache>(info.This());
        Local<Array> ids = Nan::New<Array>();

        unsigned idx = 0;
        for (auto const& item : c->cache_) {
            ids->Set(idx++,Nan::New(item.first).ToLocalChecked());
        }

        info.GetReturnValue().Set(ids);
        return;
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(RocksDBCache::list)
{
    if (info.Length() < 1) {
        return Nan::ThrowTypeError("expected one arg: 'type'");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first argument must be a String");
    }

    try {
        Nan::Utf8String utf8_value(info[0]);
        if (utf8_value.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        std::string type(*utf8_value);
        RocksDBCache* c = node::ObjectWrap::Unwrap<RocksDBCache>(info.This());
        Local<Array> ids = Nan::New<Array>();

        std::shared_ptr<rocksdb::DB> db = c->db;

        std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
        unsigned idx = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string key_id = it->key().ToString();
            if (key_id.at(0) == '=') continue;
            ids->Set(idx++, Nan::New(key_id).ToLocalChecked());
        }

        info.GetReturnValue().Set(ids);
        return;
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(RocksDBCache::merge)
{
    if (!info[0]->IsString()) return Nan::ThrowTypeError("argument 1 must be a String (infile 1)");
    if (!info[1]->IsString()) return Nan::ThrowTypeError("argument 2 must be a String (infile 2)");
    if (!info[2]->IsString()) return Nan::ThrowTypeError("argument 3 must be a String (outfile)");
    if (!info[3]->IsString()) return Nan::ThrowTypeError("argument 4 must be a String (method)");
    if (!info[4]->IsFunction()) return Nan::ThrowTypeError("argument 5 must be a callback function");

    std::string in1 = *String::Utf8Value(info[0]->ToString());
    std::string in2 = *String::Utf8Value(info[1]->ToString());
    std::string out = *String::Utf8Value(info[2]->ToString());
    Local<Value> callback = info[4];
    std::string method = *String::Utf8Value(info[3]->ToString());

    MergeBaton *baton = new MergeBaton();
    baton->filename1 = in1;
    baton->filename2 = in2;
    baton->filename3 = out;
    baton->method = method;
    baton->callback.Reset(callback.As<Function>());
    baton->request.data = baton;
    uv_queue_work(uv_default_loop(), &baton->request, mergeQueue, (uv_after_work_cb)mergeAfter);
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(MemoryCache::_set)
{
    if (info.Length() < 3) {
        return Nan::ThrowTypeError("expected two info: 'id', 'data'");
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

        MemoryCache* c = node::ObjectWrap::Unwrap<MemoryCache>(info.This());
        arraycache & arrc = c->cache_;
        key_type key_id = static_cast<key_type>(id);
        arraycache::iterator itr2 = arrc.find(key_id);
        if (itr2 == arrc.end()) {
            arrc.emplace(key_id,intarray());
        }
        intarray & vv = arrc[key_id];

        unsigned array_size = data->Length();
        if (info[2]->IsBoolean() && info[2]->BooleanValue()) {
            vv.reserve(vv.size() + array_size);
        } else {
            if (itr2 != arrc.end()) vv.clear();
            vv.reserve(array_size);
        }

        for (unsigned i=0;i<array_size;++i) {
            vv.emplace_back(static_cast<uint64_t>(data->Get(i)->NumberValue()));
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

template <typename T>
inline NAN_METHOD(_genericget)
{
    if (info.Length() < 2) {
        return Nan::ThrowTypeError("expected two info: type and id");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first arg must be a String");
    }
    if (!info[1]->IsString()) {
        return Nan::ThrowTypeError("second arg must be a String");
    }
    try {
        Nan::Utf8String utf8_type(info[0]);
        if (utf8_type.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        std::string type(*utf8_type);

        Nan::Utf8String utf8_id(info[1]);
        if (utf8_id.length() < 1) {
            return Nan::ThrowTypeError("second arg must be a String");
        }
        std::string id(*utf8_id);

        bool ignorePrefixFlag = (info.Length() >= 3 && info[2]->IsBoolean()) ? info[2]->BooleanValue() : false;

        T* c = node::ObjectWrap::Unwrap<T>(info.This());
        intarray vector = __get(c, id, ignorePrefixFlag);
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

NAN_METHOD(MemoryCache::_get) {
    return _genericget<MemoryCache>(info);
}

NAN_METHOD(RocksDBCache::_get) {
    return _genericget<RocksDBCache>(info);
}

template <typename T>
inline NAN_METHOD(_genericgetbyprefix)
{
    if (info.Length() < 2) {
        return Nan::ThrowTypeError("expected two info: type and id");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first arg must be a String");
    }
    if (!info[1]->IsString()) {
        return Nan::ThrowTypeError("second arg must be a String");
    }
    try {
        Nan::Utf8String utf8_type(info[0]);
        if (utf8_type.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        std::string type(*utf8_type);

        Nan::Utf8String utf8_id(info[1]);
        if (utf8_id.length() < 1) {
            return Nan::ThrowTypeError("second arg must be a String");
        }
        std::string id(*utf8_id);

        T* c = node::ObjectWrap::Unwrap<T>(info.This());
        intarray vector = __getbyprefix(c, id);
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

NAN_METHOD(MemoryCache::_getbyprefix) {
    return _genericgetbyprefix<MemoryCache>(info);
}

NAN_METHOD(RocksDBCache::_getbyprefix) {
    return _genericgetbyprefix<RocksDBCache>(info);
}

NAN_METHOD(MemoryCache::New)
{
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
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(RocksDBCache::New)
{
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
        RocksDBCache* im = new RocksDBCache();

        Nan::Utf8String utf8_filename(info[1]);
        if (utf8_filename.length() < 1) {
            return Nan::ThrowTypeError("second arg must be a String");
        }
        std::string filename(*utf8_filename);

        std::unique_ptr<rocksdb::DB> db;
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::Status status = OpenForReadOnlyDB(options, filename, db);
        im->db = std::move(db);

        if (!status.ok()) {
            return Nan::ThrowTypeError("unable to open rocksdb file for loading");
        }

        im->Wrap(info.This());
        info.This()->Set(Nan::New("id").ToLocalChecked(), info[0]);
        info.GetReturnValue().Set(info.This());
        return;
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
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

template <typename T>
struct PhrasematchSubq {
    PhrasematchSubq(T *c,
                    double w,
                    std::string p,
                    bool pf,
                    unsigned short i,
                    unsigned short z,
                    uint32_t m) :
        cache(c),
        weight(w),
        phrase(p),
        prefix(pf),
        idx(i),
        zoom(z),
        mask(m) {}
    T *cache;
    double weight;
    std::string phrase;
    bool prefix;
    unsigned short idx;
    unsigned short zoom;
    uint32_t mask;
    PhrasematchSubq& operator=(PhrasematchSubq && c) = default;
    PhrasematchSubq(PhrasematchSubq && c) = default;
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
};

struct Context {
    std::vector<Cover> coverList;
    uint32_t mask;
    double relev;

    Context(Context const& c) = default;
    Context(Cover && cov,
            uint32_t mask,
            double relev)
     : coverList(),
       mask(mask),
       relev(relev) {
          coverList.emplace_back(std::move(cov));
       }
    Context& operator=(Context && c) {
        coverList = std::move(c.coverList);
        mask = std::move(c.mask);
        relev = std::move(c.relev);
        return *this;
    }
    Context(std::vector<Cover> && cl,
            uint32_t mask,
            double relev)
     : coverList(std::move(cl)),
       mask(mask),
       relev(relev) {}

    Context(Context && c)
     : coverList(std::move(c.coverList)),
       mask(std::move(c.mask)),
       relev(std::move(c.relev)) {}

};

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
    cover.mask = 0;
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

template <typename T>
inline bool subqSortByZoom(PhrasematchSubq<T> const& a, PhrasematchSubq<T> const& b) noexcept {
    if (a.zoom < b.zoom) return true;
    if (a.zoom > b.zoom) return false;
    return (a.idx < b.idx);
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

inline double tileDist(unsigned px, unsigned py, unsigned tileX, unsigned tileY) {
    const double dx = static_cast<double>(px - tileX);
    const double dy = static_cast<double>(py - tileY);
    const double distance = dx * dx + dy * dy;

    return distance;
}

template <typename T>
struct CoalesceBaton : carmen::noncopyable {
    uv_work_t request;
    // params
    std::vector<PhrasematchSubq<T>> stack;
    std::vector<uint64_t> centerzxy;
    std::vector<uint64_t> bboxzxy;
    Nan::Persistent<v8::Function> callback;
    // return
    std::vector<Context> features;
    // error
    std::string error;
};

// 32 tiles is about 40 miles at z14.
// Simulates 40 mile cutoff in carmen.
double scoredist(unsigned zoom, double distance, double score) {
    if (distance == 0.0) distance = 0.01;
    double scoredist = 0;
    if (zoom >= 13) scoredist = 32.0 / distance;
    if (zoom == 12) scoredist = 24.0 / distance;
    if (zoom == 11) scoredist = 16.0 / distance;
    if (zoom == 10) scoredist = 10.0 / distance;
    if (zoom == 9)  scoredist = 6.0 / distance;
    if (zoom == 8)  scoredist = 3.5 / distance;
    if (zoom == 7)  scoredist = 2.0 / distance;
    if (zoom <= 6)  scoredist = 1.125 / distance;
    return score > scoredist ? score : scoredist;
}

template <typename T>
void coalesceFinalize(CoalesceBaton<T>* baton, std::vector<Context> && contexts) {
    if (!contexts.empty()) {
        // Coalesce stack, generate relevs.
        double relevMax = contexts[0].relev;
        std::size_t total = 0;
        std::map<uint64_t,bool> sets;
        std::map<uint64_t,bool>::iterator sit;
        std::size_t max_contexts = 40;
        baton->features.reserve(max_contexts);
        for (auto && context : contexts) {
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
template <typename T>
void coalesceSingle(uv_work_t* req) {
    CoalesceBaton<T> *baton = static_cast<CoalesceBaton<T> *>(req->data);

    try {
        std::vector<PhrasematchSubq<T>> const& stack = baton->stack;
        PhrasematchSubq<T> const& subq = stack[0];

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
        if (subq.prefix) {
            grids = __getbyprefix(subq.cache, subq.phrase);
        } else {
            grids = __get(subq.cache, subq.phrase, true);
        }

        unsigned long m = grids.size();
        double relevMax = 0;
        std::vector<Cover> covers;
        covers.reserve(m);

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
            cover.distance = proximity ? tileDist(cx, cy, cover.x, cover.y) : 0;
            cover.scoredist = proximity ? scoredist(cz, cover.distance, cover.score) : cover.score;

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
        contexts.reserve(max_contexts);
        for (auto && cover : covers) {
            // Stop at 40 contexts
            if (added == max_contexts) break;

            // Attempt not to add the same feature but by diff cover twice
            if (lastid == cover.id) continue;

            lastid = cover.id;
            added++;

            double relev = cover.relev;
            uint32_t mask = 0;
            contexts.emplace_back(std::move(cover),mask,relev);
        }

        coalesceFinalize(baton, std::move(contexts));
    } catch (std::exception const& ex) {
        baton->error = ex.what();
    }
}

template <typename T>
void coalesceMulti(uv_work_t* req) {
    CoalesceBaton<T> *baton = static_cast<CoalesceBaton<T> *>(req->data);

    try {
        std::vector<PhrasematchSubq<T>> &stack = baton->stack;
        std::sort(stack.begin(), stack.end(), subqSortByZoom<T>);
        std::size_t stackSize = stack.size();

        // Cache zoom levels to iterate over as coalesce occurs.
        std::vector<intarray> zoomCache;
        zoomCache.reserve(stackSize);
        for (auto const& subq : stack) {
            intarray zooms;
            std::vector<bool> zoomUniq(22, false);
            for (auto const& subqB : stack) {
                if (subq.idx == subqB.idx) continue;
                if (zoomUniq[subqB.zoom]) continue;
                if (subq.zoom < subqB.zoom) continue;
                zoomUniq[subqB.zoom] = true;
                zooms.emplace_back(subqB.zoom);
            }
            zoomCache.push_back(std::move(zooms));
        }

        // Coalesce relevs into higher zooms, e.g.
        // z5 inherits relev of overlapping tiles at z4.
        // @TODO assumes sources are in zoom ascending order.
        std::map<uint64_t,std::vector<Context>> coalesced;
        std::map<uint64_t,std::vector<Context>>::iterator cit;
        std::map<uint64_t,std::vector<Context>>::iterator pit;
        std::map<uint64_t,bool> done;
        std::map<uint64_t,bool>::iterator dit;

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
            if (subq.prefix) {
                grids = __getbyprefix(subq.cache, subq.phrase);
            } else {
                grids = __get(subq.cache, subq.phrase, true);
            }

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

                // Reserve stackSize for the coverList. The vector
                // will grow no larger that the size of the input
                // subqueries that are being coalesced.
                std::vector<Cover> covers;
                covers.reserve(stackSize);
                covers.push_back(cover);
                uint32_t context_mask = cover.mask;
                double context_relev = cover.relev;

                for (unsigned a = 0; a < zCacheSize; a++) {
                    uint64_t p = zCache[a];
                    double s = static_cast<double>(1 << (z-p));
                    uint64_t pxy = static_cast<uint64_t>(p * POW2_28) +
                        static_cast<uint64_t>(std::floor(cover.x/s) * POW2_14) +
                        static_cast<uint64_t>(std::floor(cover.y/s));
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
                    contexts.emplace_back(std::move(covers),context_mask,context_relev);
                } else if (first || covers.size() > 1) {
                    cit = coalesced.find(zxy);
                    if (cit == coalesced.end()) {
                        std::vector<Context> local_contexts;
                        local_contexts.emplace_back(std::move(covers),context_mask,context_relev);
                        coalesced.emplace(zxy, std::move(local_contexts));
                    } else {
                        cit->second.emplace_back(std::move(covers),context_mask,context_relev);
                    }
                }
            }

            i++;
        }

        // append coalesced to contexts by moving memory
        for (auto && matched : coalesced) {
            for (auto &&context : matched.second) {
                contexts.emplace_back(std::move(context));
            }
        }

        std::sort(contexts.begin(), contexts.end(), contextSortByRelev);
        coalesceFinalize<T>(baton, std::move(contexts));
    } catch (std::exception const& ex) {
       baton->error = ex.what();
    }
}

Local<Object> coverToObject(Cover const& cover) {
    Local<Object> object = Nan::New<Object>();
    object->Set(Nan::New("x").ToLocalChecked(), Nan::New<Number>(cover.x));
    object->Set(Nan::New("y").ToLocalChecked(), Nan::New<Number>(cover.y));
    object->Set(Nan::New("relev").ToLocalChecked(), Nan::New<Number>(cover.relev));
    object->Set(Nan::New("score").ToLocalChecked(), Nan::New<Number>(cover.score));
    object->Set(Nan::New("id").ToLocalChecked(), Nan::New<Number>(cover.id));
    object->Set(Nan::New("idx").ToLocalChecked(), Nan::New<Number>(cover.idx));
    object->Set(Nan::New("tmpid").ToLocalChecked(), Nan::New<Number>(cover.tmpid));
    object->Set(Nan::New("distance").ToLocalChecked(), Nan::New<Number>(cover.distance));
    object->Set(Nan::New("scoredist").ToLocalChecked(), Nan::New<Number>(cover.scoredist));
    return object;
}
Local<Array> contextToArray(Context const& context) {
    std::size_t size = context.coverList.size();
    Local<Array> array = Nan::New<Array>(static_cast<int>(size));
    for (uint32_t i = 0; i < size; i++) {
        array->Set(i, coverToObject(context.coverList[i]));
    }
    array->Set(Nan::New("relev").ToLocalChecked(), Nan::New(context.relev));
    return array;
}
template <typename T>
void coalesceAfter(uv_work_t* req) {
    Nan::HandleScope scope;
    CoalesceBaton<T> *baton = static_cast<CoalesceBaton<T> *>(req->data);

    // Reference count the cache objects
    for (auto & subq : baton->stack) {
       subq.cache->_unref();
    }

    if (!baton->error.empty()) {
        v8::Local<v8::Value> argv[1] = { Nan::Error(baton->error.c_str()) };
        Nan::MakeCallback(Nan::GetCurrentContext()->Global(), Nan::New(baton->callback), 1, argv);
    }
    else {
        std::vector<Context> const& features = baton->features;

        Local<Array> jsFeatures = Nan::New<Array>(static_cast<int>(features.size()));
        for (std::size_t i = 0; i < features.size(); i++) {
            jsFeatures->Set(i, contextToArray(features[i]));
        }

        Local<Value> argv[2] = { Nan::Null(), jsFeatures };
        Nan::MakeCallback(Nan::GetCurrentContext()->Global(), Nan::New(baton->callback), 2, argv);
    }

    baton->callback.Reset();
    delete baton;
}
template <typename T>
NAN_METHOD(genericcoalesce) {
    // PhrasematchStack (js => cpp)
    if (info.Length() < 3) {
        return Nan::ThrowTypeError("Expects 3 arguments: a PhrasematchSubq array, an option object, and a callback");
    }

    if (!info[0]->IsArray()) {
        return Nan::ThrowTypeError("Arg 1 must be a PhrasematchSubq array");
    }

    Local<Array> array = Local<Array>::Cast(info[0]);
    auto array_length = array->Length();
    if (array_length < 1) {
        return Nan::ThrowTypeError("Arg 1 must be an array with one or more PhrasematchSubq objects");
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
    std::unique_ptr<CoalesceBaton<T>> baton_ptr = std::make_unique<CoalesceBaton<T>>();
    CoalesceBaton<T>* baton = baton_ptr.get();
    try {
        for (uint32_t i = 0; i < array_length; i++) {
            Local<Value> val = array->Get(i);
            if (!val->IsObject()) {
                return Nan::ThrowTypeError("All items in array must be valid PhrasematchSubq objects");
            }
            Local<Object> jsStack = val->ToObject();
            if (jsStack->IsNull() || jsStack->IsUndefined()) {
                return Nan::ThrowTypeError("All items in array must be valid PhrasematchSubq objects");
            }

            double weight;
            std::string phrase;
            bool prefix;
            unsigned short idx;
            unsigned short zoom;
            uint32_t mask;

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

            if (!jsStack->Has(Nan::New("cache").ToLocalChecked())) {
                return Nan::ThrowTypeError("missing cache property");
            } else {
                Local<Value> prop_val = jsStack->Get(Nan::New("cache").ToLocalChecked());
                if (!prop_val->IsObject()) {
                    return Nan::ThrowTypeError("cache value must be a Cache object");
                }
                Local<Object> _cache = prop_val->ToObject();
                if (_cache->IsNull() || _cache->IsUndefined() || !Nan::New(T::constructor)->HasInstance(prop_val)) {
                    return Nan::ThrowTypeError("cache value must be a Cache object");
                }
                baton->stack.emplace_back(
                    node::ObjectWrap::Unwrap<T>(_cache),
                    weight,
                    phrase,
                    prefix,
                    idx,
                    zoom,
                    mask
                );
            }
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
        for (auto & subq : baton->stack) {
           subq.cache->_ref();
        }
        // optimization: for stacks of 1, use coalesceSingle
        if (baton->stack.size() == 1) {
            uv_queue_work(uv_default_loop(), &baton->request, coalesceSingle<T>, (uv_after_work_cb)coalesceAfter<T>);
        } else {
            uv_queue_work(uv_default_loop(), &baton->request, coalesceMulti<T>, (uv_after_work_cb)coalesceAfter<T>);
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }

    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(MemoryCache::coalesce) {
    return genericcoalesce<MemoryCache>(info);
}

NAN_METHOD(RocksDBCache::coalesce) {
    return genericcoalesce<RocksDBCache>(info);
}

extern "C" {
    static void start(Handle<Object> target) {
        MemoryCache::Initialize(target);
        RocksDBCache::Initialize(target);
    }
}

} // namespace carmen


NODE_MODULE(carmen, carmen::start)
