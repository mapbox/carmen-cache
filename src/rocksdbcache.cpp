
#include "rocksdbcache.hpp"
#include "cpp_util.hpp"

namespace carmen {

using namespace v8;

/**
 *
 * @class RocksDBCache
 *
 */

Nan::Persistent<FunctionTemplate> RocksDBCache::constructor;


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
        bool matches_language = static_cast<bool>(message_langfield & langfield);

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

/**
 * Writes an identical copy RocksDBCache from another RocksDBCache; not really used
 *
 * @name pack
 * @memberof RocksDBCache
 * @param {String}, filename
 * @returns {Boolean}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const RocksDBCache = new cache.RocksDBCache('a');
 *
 * cache.pack('filename');
 *
 */

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
}

// Used in merge() to queue files for merging; result is undefined
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

// Used in merge() to queue files for merging

// we don't use the 'status' parameter, but it's required as part of the uv_after_work_cb
// function signature, so suppress the warning about it
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
void mergeAfter(uv_work_t* req, int status) {
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
#pragma clang diagnostic pop

/**
 * Retrieves data exactly matching phrase and language settings by id
 *
 * @name get
 * @memberof RocksDBCache
 * @param {String} id
 * @param {Boolean} matches_prefixes: T if it matches exactly, F: if it does not
 * @param {Array} optional; array of languages
 * @returns {Array} integers referring to grids
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const RocksDBCache = new cache.RocksDBCache('a');
 *
 * RocksDBCache.get(id, languages);
 *  // => [grid, grid, grid, grid... ]
 *
 */

NAN_METHOD(RocksDBCache::_get) {
    return _genericget<RocksDBCache>(info);
}

/**
 * lists the keys in the RocksDBCache object
 *
 * @name list
 * @memberof RocksDBCache
 * @param {String} id
 * @returns {Array} Set of keys/ids
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const RocksDBCache = new cache.RocksDBCache('a');
 *
 * cache.list('a', (err, result) => {
 *    if (err) throw err;
 *    console.log(result);
 * });
 *
 */

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
}

/**
 * merges the contents from 2 RocksDBCaches
 *
 * @name merge
 * @memberof RocksDBCache
 * @param {String} RocksDBCache file 1
 * @param {String} RocksDBCache file 2
 * @param {String} result RocksDBCache file
 * @param {String} method which is either concat or freq
 * @param {Function} callback called from the mergeAfter method
 * @returns {Set} Set of ids
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const RocksDBCache = new cache.RocksDBCache('a');
 *
 * cache.merge('file1', 'file2', 'resultFile', 'method', (err, result) => {
 *    if (err) throw err;
 *    console.log(result);
 * });
 *
 */

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
    uv_queue_work(uv_default_loop(), &baton->request, mergeQueue, static_cast<uv_after_work_cb>(mergeAfter));
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

/**
* Creates an in-memory key-value store mapping phrases  and language IDs
* to lists of corresponding grids (grids ie are integer representations of occurrences of the phrase within an index)
*
 * @name RocksDBCache
 * @memberof RocksDBCache
 * @param {String} id
 * @param {String} filename
 * @returns {Object}
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const RocksDBCache = new cache.RocksDBCache('a', 'filename');
 *
 */

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
}

/**
 * Retrieves grid that at least partially matches phrase and/or language inputs
 *
 * @name get
 * @memberof RocksDBCache
 * @param {String} id
 * @param {Boolean} matches_prefixes: T if it matches exactly, F: if it does not
 * @param {Array} optional; array of languages
 * @returns {Array} integers referring to grids
 * @example
 * const cache = require('@mapbox/carmen-cache');
 * const RocksDBCache = new cache.RocksDBCache('a');
 *
 * RocksDBCache.get(id, languages);
 *  // => [grid, grid, grid, grid... ]
 *
 */

NAN_METHOD(RocksDBCache::_getmatching) {
    return _genericgetmatching<RocksDBCache>(info);
}

} // namespace carmen
