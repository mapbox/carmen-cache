
#include "binding.hpp"

#include <sstream>
#include <cmath>
#include <cassert>
#include <cstring>
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

constexpr uint64_t LANGUAGE_MATCH_BOOST = (const uint64_t)(1) << 63;

// in general, the key format including language field is <key_text><separator character><8-byte langfield>
// but as a size optimization for language-less indexes, we omit the langfield
// if it would otherwise have been ALL_LANGUAGES (all 1's), so if the first occurence
// of the separator character is also the last character in the string, we just retur ALL_LANGUAGES
// otherwise we extract it from the string
//
// we centralize both the adding of the field and extracting of the field here to keep from having
// to handle that optimization everywhere
inline langfield_type extract_langfield(std::string const& s) {
    if (s.find(LANGFIELD_SEPARATOR) == s.length() - 1) {
        return ALL_LANGUAGES;
    } else {
        return *(reinterpret_cast<const unsigned __int128*>(&s[s.length() - sizeof(langfield_type)]));
    }
}

inline void add_langfield(std::string & s, langfield_type langfield) {
    if (langfield == ALL_LANGUAGES) {
        s.reserve(sizeof(LANGFIELD_SEPARATOR) + sizeof(langfield_type));
        s.push_back(LANGFIELD_SEPARATOR);
        s.append(reinterpret_cast<char*>(&langfield), sizeof(langfield_type));
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
        return aitr->second;
    }
    return array;
}

inline void decodeMessage(std::string const & message, intarray & array) {
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

inline void decodeAndBoostMessage(std::string const & message, intarray & array) {
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
    for (rit->Seek(phrase); rit->Valid() && rit->key().ToString().compare(0, phrase_length, phrase) == 0; rit->Next()) {
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
                matches_language
            });
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
                gridIdx
            );
        }
    }

    return array;
}

inline langfield_type langarrayToLangfield(Local<v8::Array> const& array) {
    size_t array_size = array->Length();
    langfield_type out = 0;
    for (unsigned i = 0; i < array_size; i++) {
        unsigned int val = static_cast<unsigned int>(array->Get(i)->NumberValue());
        if (val >= sizeof(langfield_type)) {
            // this should probably throw something
            continue;
        }
        out = out | (static_cast<langfield_type>(1) << val);
    }
    return out;
}

inline Local<v8::Array> langfieldToLangarray(langfield_type langfield) {
    Local<Array> langs = Nan::New<Array>();

    unsigned idx = 0;
    for (unsigned i = 0; i < sizeof(langfield_type); i++) {
        if (langfield & (static_cast<langfield_type>(1) << i)) {
            langs->Set(idx++,Nan::New(i));
        }
    }
    return langs;
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

MemoryCache::~MemoryCache() { }

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

RocksDBCache::~RocksDBCache() { }

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

NAN_METHOD(MemoryCache::pack)
{
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

            packVec(varr, db, item.first);
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(RocksDBCache::pack)
{
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
    try {
        Nan::Utf8String utf8_value(info[0]);
        if (utf8_value.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
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
    try {
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
        arraycache & arrc = c->cache_;
        key_type key_id = static_cast<key_type>(id);
        add_langfield(key_id, langfield);

        arraycache::iterator itr2 = arrc.find(key_id);
        if (itr2 == arrc.end()) {
            arrc.emplace(key_id,intarray());
        }
        intarray & vv = arrc[key_id];

        unsigned array_size = data->Length();
        if (append) {
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

template <typename T>
inline NAN_METHOD(_genericgetmatching)
{
    if (info.Length() < 3) {
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

NAN_METHOD(MemoryCache::_getmatching) {
    return _genericgetmatching<MemoryCache>(info);
}

NAN_METHOD(RocksDBCache::_getmatching) {
    return _genericgetmatching<RocksDBCache>(info);
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

struct PhrasematchSubq {
    PhrasematchSubq(void *c,
                    char t,
                    double w,
                    std::string p,
                    bool pf,
                    unsigned short i,
                    unsigned short z,
                    uint32_t m) :
        cache(c),
        type(t),
        weight(w),
        phrase(p),
        prefix(pf),
        idx(i),
        zoom(z),
        mask(m) {}
    void *cache;
    char type;
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
    bool matches_language;
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
    bool matches_language = static_cast<bool>(num & LANGUAGE_MATCH_BOOST);
    cover.x = x;
    cover.y = y;
    double relev = 0.4 + (0.2 * static_cast<double>((num >> 51) % POW2_2));
    cover.relev = relev;
    cover.score = score;
    cover.id = id;
    cover.matches_language = matches_language;

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

inline bool subqSortByZoom(PhrasematchSubq const& a, PhrasematchSubq const& b) noexcept {
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

struct CoalesceBaton : carmen::noncopyable {
    uv_work_t request;
    // params
    std::vector<PhrasematchSubq> stack;
    std::vector<uint64_t> centerzxy;
    std::vector<uint64_t> bboxzxy;
    langfield_type langfield;
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

void coalesceFinalize(CoalesceBaton* baton, std::vector<Context> && contexts) {
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
void coalesceSingle(uv_work_t* req) {
    CoalesceBaton *baton = static_cast<CoalesceBaton *>(req->data);

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
        if (subq.prefix) {
            grids = subq.type == TYPE_MEMORY ?
                __getmatching(reinterpret_cast<MemoryCache*>(subq.cache), subq.phrase, true, ALL_LANGUAGES) :
                __getmatching(reinterpret_cast<RocksDBCache*>(subq.cache), subq.phrase, true, ALL_LANGUAGES);
        } else {
            grids = subq.type == TYPE_MEMORY ?
                __getmatching(reinterpret_cast<MemoryCache*>(subq.cache), subq.phrase, false, ALL_LANGUAGES) :
                __getmatching(reinterpret_cast<RocksDBCache*>(subq.cache), subq.phrase, false, ALL_LANGUAGES);
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
            if (!cover.matches_language) cover.relev *= .8;
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

void coalesceMulti(uv_work_t* req) {
    CoalesceBaton *baton = static_cast<CoalesceBaton *>(req->data);

    try {
        std::vector<PhrasematchSubq> &stack = baton->stack;
        std::sort(stack.begin(), stack.end(), subqSortByZoom);
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
                grids = subq.type == TYPE_MEMORY ?
                    __getmatching(reinterpret_cast<MemoryCache*>(subq.cache), subq.phrase, true, ALL_LANGUAGES) :
                    __getmatching(reinterpret_cast<RocksDBCache*>(subq.cache), subq.phrase, true, ALL_LANGUAGES);
            } else {
                grids = subq.type == TYPE_MEMORY ?
                    __getmatching(reinterpret_cast<MemoryCache*>(subq.cache), subq.phrase, false, ALL_LANGUAGES) :
                    __getmatching(reinterpret_cast<RocksDBCache*>(subq.cache), subq.phrase, false, ALL_LANGUAGES);
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
                if (!cover.matches_language) cover.relev *= .8;
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
        coalesceFinalize(baton, std::move(contexts));
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
    object->Set(Nan::New("matches_language").ToLocalChecked(), Nan::New<Boolean>(cover.matches_language));
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
void coalesceAfter(uv_work_t* req) {
    Nan::HandleScope scope;
    CoalesceBaton *baton = static_cast<CoalesceBaton *>(req->data);

    // Reference count the cache objects
    for (auto & subq : baton->stack) {
        if (subq.type == TYPE_MEMORY) reinterpret_cast<MemoryCache*>(subq.cache)->_unref();
        else reinterpret_cast<RocksDBCache*>(subq.cache)->_unref();
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

NAN_METHOD(coalesce) {
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
    std::unique_ptr<CoalesceBaton> baton_ptr = std::make_unique<CoalesceBaton>();
    CoalesceBaton* baton = baton_ptr.get();
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
                        (void*) node::ObjectWrap::Unwrap<MemoryCache>(_cache),
                        TYPE_MEMORY,
                        weight,
                        phrase,
                        prefix,
                        idx,
                        zoom,
                        mask
                    );
                } else {
                    baton->stack.emplace_back(
                        (void*) node::ObjectWrap::Unwrap<RocksDBCache>(_cache),
                        TYPE_ROCKSDB,
                        weight,
                        phrase,
                        prefix,
                        idx,
                        zoom,
                        mask
                    );
                }
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

        langfield_type langfield = ALL_LANGUAGES;
        if (options->Has(Nan::New("languages").ToLocalChecked())) {
            Local<Value> c_array = options->Get(Nan::New("languages").ToLocalChecked());
            if (!c_array->IsArray()) {
                return Nan::ThrowTypeError("languages must be an array");
            }
            Local<Array> carray = Local<Array>::Cast(c_array);
            langfield = langarrayToLangfield(carray);
        }

        baton->langfield = langfield;

        baton->callback.Reset(callback.As<Function>());

        // queue work
        baton->request.data = baton;
        // Release the managed baton
        baton_ptr.release();
        // Reference count the cache objects
        for (auto & subq : baton->stack) {
           if (subq.type == TYPE_MEMORY) reinterpret_cast<MemoryCache*>(subq.cache)->_ref();
           else reinterpret_cast<RocksDBCache*>(subq.cache)->_ref();
        }
        // optimization: for stacks of 1, use coalesceSingle
        if (baton->stack.size() == 1) {
            uv_queue_work(uv_default_loop(), &baton->request, coalesceSingle, (uv_after_work_cb)coalesceAfter);
        } else {
            uv_queue_work(uv_default_loop(), &baton->request, coalesceMulti, (uv_after_work_cb)coalesceAfter);
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }

    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

extern "C" {
    static void start(Handle<Object> target) {
        MemoryCache::Initialize(target);
        RocksDBCache::Initialize(target);
        Nan::SetMethod(target, "coalesce", coalesce);
    }
}

} // namespace carmen


NODE_MODULE(carmen, carmen::start)
