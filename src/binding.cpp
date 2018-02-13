
#include "binding.hpp"
#include "node_util.hpp"
#include "coalesce.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <memory>
#include <sstream>

#include <protozero/pbf_reader.hpp>
#include <protozero/pbf_writer.hpp>

#include <chrono>
typedef std::chrono::high_resolution_clock Clock;

namespace carmen {

using namespace v8;

Nan::Persistent<FunctionTemplate> MemoryCache::constructor;
Nan::Persistent<FunctionTemplate> RocksDBCache::constructor;
Nan::Persistent<FunctionTemplate> NormalizationCache::constructor;

rocksdb::Status OpenDB(const rocksdb::Options& options, const std::string& name, std::unique_ptr<rocksdb::DB>& dbptr) {
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, name, &db);
    dbptr = std::unique_ptr<rocksdb::DB>(db);
    return status;
}

rocksdb::Status OpenForReadOnlyDB(const rocksdb::Options& options, const std::string& name, std::unique_ptr<rocksdb::DB>& dbptr) {
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::OpenForReadOnly(options, name, &db);
    dbptr = std::unique_ptr<rocksdb::DB>(db);
    return status;
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

inline void decodeMessage(std::string const& message, intarray& array) {
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

inline void decodeAndBoostMessage(std::string const& message, intarray& array) {
    protozero::pbf_reader item(message);
    item.next(CACHE_ITEM);
    auto vals = item.get_packed_uint64();
    uint64_t lastval = 0;
    // delta decode values.
    for (auto it = vals.first; it != vals.second; ++it) {
        if (lastval == 0) {
            lastval = *it;
            array.emplace_back(lastval | LANGUAGE_MATCH_BOOST);
        } else {
            lastval = lastval - *it;
            array.emplace_back(lastval | LANGUAGE_MATCH_BOOST);
        }
    }
}

intarray __get(RocksDBCache const* c, std::string phrase, langfield_type langfield) {
    std::shared_ptr<rocksdb::DB> db = c->db;
    intarray array;

    add_langfield(phrase, langfield);
    std::string message;
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), phrase, &message);
    if (s.ok()) {
        decodeMessage(message, array);
    }

    return array;
}

struct sortableGrid {
    protozero::const_varint_iterator<uint64_t> it;
    protozero::const_varint_iterator<uint64_t> end;
    value_type unadjusted_lastval;
    bool matches_language;
};

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

intarray __getmatching(RocksDBCache const* c, std::string phrase, bool match_prefixes, langfield_type langfield) {
    intarray array;

    if (!match_prefixes) phrase.push_back(LANGFIELD_SEPARATOR);
    size_t phrase_length = phrase.length();

    // Load values from message cache
    std::vector<std::tuple<std::string, bool>> messages;
    std::vector<sortableGrid> grids;

    if (match_prefixes) {
        // if this is an autocomplete scan, use the prefix cache
        if (phrase_length <= MEMO_PREFIX_LENGTH_T1) {
            phrase = "=1" + phrase.substr(0, MEMO_PREFIX_LENGTH_T1);
        } else if (phrase_length <= MEMO_PREFIX_LENGTH_T2) {
            phrase = "=2" + phrase.substr(0, MEMO_PREFIX_LENGTH_T2);
        }
    }

    radix_max_heap::pair_radix_max_heap<uint64_t, size_t> rh;

    std::shared_ptr<rocksdb::DB> db = c->db;

    std::unique_ptr<rocksdb::Iterator> rit(db->NewIterator(rocksdb::ReadOptions()));
    for (rit->Seek(phrase); rit->Valid() && rit->key().ToString().compare(0, phrase.size(), phrase) == 0; rit->Next()) {
        std::string key = rit->key().ToString();

        // grab the langfield from the end of the key
        langfield_type message_langfield = extract_langfield(key);
        bool matches_language = (bool)(message_langfield & langfield);

        messages.emplace_back(std::make_tuple(rit->value().ToString(), matches_language));
    }

    // short-circuit the priority queue merging logic if we only found one message
    // as will be the norm for exact matches in translationless indexes
    if (messages.size() == 1) {
        if (std::get<1>(messages[0])) {
            decodeAndBoostMessage(std::get<0>(messages[0]), array);
        } else {
            decodeMessage(std::get<0>(messages[0]), array);
        }
        return array;
    }

    for (std::tuple<std::string, bool>& message : messages) {
        protozero::pbf_reader item(std::get<0>(message));
        bool matches_language = std::get<1>(message);

        item.next(CACHE_ITEM);
        auto vals = item.get_packed_uint64();

        if (vals.first != vals.second) {
            value_type unadjusted_lastval = *(vals.first);
            grids.emplace_back(sortableGrid{
                vals.first,
                vals.second,
                unadjusted_lastval,
                matches_language});
            rh.push(matches_language ? unadjusted_lastval | LANGUAGE_MATCH_BOOST : unadjusted_lastval, grids.size() - 1);
        }
    }

    while (!rh.empty() && array.size() < PREFIX_MAX_GRID_LENGTH) {
        size_t gridIdx = rh.top_value();
        uint64_t gridId = rh.top_key();
        rh.pop();

        array.emplace_back(gridId);
        sortableGrid* sg = &(grids[gridIdx]);
        sg->it++;
        if (sg->it != sg->end) {
            sg->unadjusted_lastval -= *(grids[gridIdx].it);
            rh.push(
                sg->matches_language ? sg->unadjusted_lastval | LANGUAGE_MATCH_BOOST : sg->unadjusted_lastval,
                gridIdx);
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
    Nan::SetPrototypeMethod(t, "_getMatching", _getmatching);
    target->Set(Nan::New("MemoryCache").ToLocalChecked(), t->GetFunction());
    constructor.Reset(t);
}

MemoryCache::MemoryCache()
    : ObjectWrap(),
      cache_() {}

MemoryCache::~MemoryCache() {}

void RocksDBCache::Initialize(Handle<Object> target) {
    Nan::HandleScope scope;
    Local<FunctionTemplate> t = Nan::New<FunctionTemplate>(RocksDBCache::New);
    t->InstanceTemplate()->SetInternalFieldCount(1);
    t->SetClassName(Nan::New("RocksDBCache").ToLocalChecked());
    Nan::SetPrototypeMethod(t, "pack", RocksDBCache::pack);
    Nan::SetPrototypeMethod(t, "list", RocksDBCache::list);
    Nan::SetPrototypeMethod(t, "_get", _get);
    Nan::SetPrototypeMethod(t, "_getMatching", _getmatching);
    Nan::SetMethod(t, "merge", merge);
    target->Set(Nan::New("RocksDBCache").ToLocalChecked(), t->GetFunction());
    constructor.Reset(t);
}

RocksDBCache::RocksDBCache()
    : ObjectWrap(),
      db() {}

RocksDBCache::~RocksDBCache() {}

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

NAN_METHOD(RocksDBCache::pack) {
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
    MergeBaton* baton = static_cast<MergeBaton*>(req->data);
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
    std::map<key_type, bool> ids1;
    std::map<key_type, bool> ids2;

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
            std::string max_key = "__MAX__";
            auto max_key_length = max_key.length();
            rocksdb::Status s = db2->Get(rocksdb::ReadOptions(), key_id, &in_message2);
            if (s.ok()) {
                // get input proto 2
                protozero::pbf_reader item2(in_message2);
                item2.next(CACHE_ITEM);

                auto vals2 = item2.get_packed_uint64();
                lastval = 0;
                for (auto it = vals2.first; it != vals2.second; ++it) {
                    if (method == "freq") {
                        if (key_id.compare(0, max_key_length, max_key) == 0) {
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
    MergeBaton* baton = static_cast<MergeBaton*>(req->data);
    if (!baton->error.empty()) {
        v8::Local<v8::Value> argv[1] = {Nan::Error(baton->error.c_str())};
        Nan::MakeCallback(Nan::GetCurrentContext()->Global(), Nan::New(baton->callback), 1, argv);
    } else {
        Local<Value> argv[2] = {Nan::Null()};
        Nan::MakeCallback(Nan::GetCurrentContext()->Global(), Nan::New(baton->callback), 1, argv);
    }
    baton->callback.Reset();
    delete baton;
}

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
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(RocksDBCache::list) {
    try {
        RocksDBCache* c = node::ObjectWrap::Unwrap<RocksDBCache>(info.This());
        Local<Array> ids = Nan::New<Array>();

        std::shared_ptr<rocksdb::DB> db = c->db;

        std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
        unsigned idx = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string key_id = it->key().ToString();
            if (key_id.at(0) == '=') continue;

            Local<Array> out = Nan::New<Array>();
            out->Set(0, Nan::New(key_id.substr(0, key_id.find(LANGFIELD_SEPARATOR))).ToLocalChecked());

            langfield_type langfield = extract_langfield(key_id);
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
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(RocksDBCache::merge) {
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

    MergeBaton* baton = new MergeBaton();
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

template <typename T>
inline NAN_METHOD(_genericget) {
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

        T* c = node::ObjectWrap::Unwrap<T>(info.This());
        intarray vector = __get(c, id, langfield);
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
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(RocksDBCache::New) {
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

        std::unique_ptr<rocksdb::DB> db;
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::Status status = OpenForReadOnlyDB(options, filename, db);

        if (!status.ok()) {
            return Nan::ThrowTypeError("unable to open rocksdb file for loading");
        }
        RocksDBCache* im = new RocksDBCache();
        im->db = std::move(db);
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

template <typename T>
inline NAN_METHOD(_genericgetmatching) {
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

        T* c = node::ObjectWrap::Unwrap<T>(info.This());
        intarray vector = __getmatching(c, id, match_prefixes, langfield);
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

NAN_METHOD(MemoryCache::_getmatching) {
    return _genericgetmatching<MemoryCache>(info);
}

NAN_METHOD(RocksDBCache::_getmatching) {
    return _genericgetmatching<RocksDBCache>(info);
}

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
    void FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const override {}
    void FindShortSuccessor(std::string* key) const override {}
};
UInt32Comparator UInt32ComparatorInstance;

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
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

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

NAN_METHOD(NormalizationCache::writebatch) {
    if (info.Length() < 1) {
        return Nan::ThrowTypeError("expected one info: data");
    }
    if (!info[0]->IsArray()) {
        return Nan::ThrowTypeError("second arg must be an Array");
    }
    Local<Array> data = Local<Array>::Cast(info[0]);
    if (data->IsNull() || data->IsUndefined()) {
        return Nan::ThrowTypeError("an array expected for second argument");
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

extern "C" {
static void start(Handle<Object> target) {
    MemoryCache::Initialize(target);
    RocksDBCache::Initialize(target);
    NormalizationCache::Initialize(target);
    Nan::SetMethod(target, "coalesce", coalesce);
}
}

} // namespace carmen

NODE_MODULE(carmen, carmen::start)
