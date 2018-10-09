
#include "rocksdbcache.hpp"
#include "cpp_util.hpp"

namespace carmen {

intarray RocksDBCache::__get(std::string phrase, langfield_type langfield) {
    std::shared_ptr<rocksdb::DB> db = this->db;
    intarray array;

    add_langfield(phrase, langfield);
    std::string message;
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), phrase, &message);
    if (s.ok()) {
        decodeMessage(message, array);
    }

    return array;
}

intarray RocksDBCache::__getmatching(std::string phrase, bool match_prefixes, langfield_type langfield) {
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

    std::shared_ptr<rocksdb::DB> db = this->db;

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
            grids.emplace_back(
                vals.first,
                vals.second,
                unadjusted_lastval,
                matches_language);
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

RocksDBCache::RocksDBCache() : db() {}

RocksDBCache::~RocksDBCache() {}

bool RocksDBCache::pack(std::string filename) {
    std::shared_ptr<rocksdb::DB> existing = this->db;

    if (existing && existing->GetName() == filename) {
        throw std::invalid_argument("rocksdb file is already loaded read-only; unload first");
    }

    std::unique_ptr<rocksdb::DB> db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = OpenDB(options, filename, db);

    if (!status.ok()) {
        throw std::invalid_argument("unable to open rocksdb file for packing");
    }

    // if what we have now is already a rocksdb, and it's a different
    // one from what we're being asked to pack into, copy from one to the other
    std::unique_ptr<rocksdb::Iterator> existingIt(existing->NewIterator(rocksdb::ReadOptions()));
    for (existingIt->SeekToFirst(); existingIt->Valid(); existingIt->Next()) {
        db->Put(rocksdb::WriteOptions(), existingIt->key(), existingIt->value());
    }

    return true;
}

std::vector<std::pair<std::string, langfield_type>> RocksDBCache::list() {
    std::shared_ptr<rocksdb::DB> db = this->db;

    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
    std::vector<std::pair<std::string, langfield_type>> out;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key_id = it->key().ToString();
        if (key_id.at(0) == '=') continue;

        std::string phrase = key_id.substr(0, key_id.find(LANGFIELD_SEPARATOR));
        langfield_type langfield = extract_langfield(key_id);

        out.emplace_back(phrase, langfield);
    }
    return out;
}

RocksDBCache::RocksDBCache(std::string filename) {
    std::unique_ptr<rocksdb::DB> db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = OpenForReadOnlyDB(options, filename, db);

    if (!status.ok()) {
        throw std::invalid_argument("unable to open rocksdb file for loading");
    }
    this->db = std::move(db);
}

} // namespace carmen
