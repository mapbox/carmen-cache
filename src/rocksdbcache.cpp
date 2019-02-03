
#include "rocksdbcache.hpp"
#include "cpp_util.hpp"

namespace carmen {

intarray RocksDBCache::__get(const std::string& phrase, langfield_type langfield) {
    intarray array;
    std::string phrase_with_langfield = phrase;

    add_langfield(phrase_with_langfield, langfield);
    std::string message;
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), phrase_with_langfield, &message);
    if (s.ok()) {
        decodeMessage(message, array, std::numeric_limits<size_t>::max());
    }

    return array;
}

intarray RocksDBCache::__getmatching(const std::string& phrase_ref, PrefixMatch match_prefixes, langfield_type langfield, size_t max_results) {
    intarray array;
    std::string phrase = phrase_ref;

    if (match_prefixes == PrefixMatch::disabled) {
        phrase.push_back(LANGFIELD_SEPARATOR);
    }

    size_t phrase_length = phrase.length();
    if (match_prefixes == PrefixMatch::word_boundary) {
        // If we're looking for a word boundary we need have one more character
        // available than the phrase is long. Incrementing this lengh ensures we
        // don't use a prefix cache that could cut off the word break.
        phrase_length++;
    }

    // Load values from message cache
    std::vector<std::tuple<std::string, bool>> messages;
    std::vector<sortableGrid> grids;

    if (match_prefixes != PrefixMatch::disabled) {
        // if this is an autocomplete scan, use the prefix cache
        if (phrase_length <= MEMO_PREFIX_LENGTH_T1) {
            phrase = "=1" + phrase.substr(0, MEMO_PREFIX_LENGTH_T1);
        } else if (phrase_length <= MEMO_PREFIX_LENGTH_T2) {
            phrase = "=2" + phrase.substr(0, MEMO_PREFIX_LENGTH_T2);
        }
    }

    radix_max_heap::pair_radix_max_heap<uint64_t, size_t> rh;

    std::unique_ptr<rocksdb::Iterator> rit(db->NewIterator(rocksdb::ReadOptions()));
    for (rit->Seek(phrase); rit->Valid() && rit->key().ToString().compare(0, phrase.size(), phrase) == 0; rit->Next()) {
        std::string key = rit->key().ToString();

        if (match_prefixes == PrefixMatch::word_boundary) {
            // Read one character beyond the input prefix length, should always
            // be safe because of the LANGFIELD_SEPARATOR
            char endChar = key.at(phrase.length());
            if (endChar != LANGFIELD_SEPARATOR && endChar != ' ') {
                continue;
            }
        }

        // grab the langfield from the end of the key
        langfield_type message_langfield = extract_langfield(key);
        auto matches_language = static_cast<bool>(message_langfield & langfield);

        messages.emplace_back(std::make_tuple(rit->value().ToString(), matches_language));
    }

    // short-circuit the priority queue merging logic if we only found one message
    // as will be the norm for exact matches in translationless indexes
    if (messages.size() == 1) {
        if (std::get<1>(messages[0])) {
            decodeAndBoostMessage(std::get<0>(messages[0]), array, max_results);
        } else {
            decodeMessage(std::get<0>(messages[0]), array, max_results);
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

    while (!rh.empty() && array.size() < max_results) {
        size_t gridIdx = rh.top_value();
        uint64_t gridId = rh.top_key();
        rh.pop();

        if (array.empty() || array.back() != gridId) array.emplace_back(gridId);
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

// this is an alternative version of getmatching specifically intended for the
// address/partial-number case that parses grid data eagerly rather than lazily
// and does bbox filtering before sorting
intarray RocksDBCache::__getmatchingBboxFiltered(const std::string& phrase_ref, PrefixMatch match_prefixes, langfield_type langfield, size_t max_results, const uint64_t box[4]) {
    intarray array;
    std::string phrase = phrase_ref;

    if (match_prefixes == PrefixMatch::disabled) {
        phrase.push_back(LANGFIELD_SEPARATOR);
    }

    size_t phrase_length = phrase.length();
    if (match_prefixes == PrefixMatch::word_boundary) {
        // If we're looking for a word boundary we need have one more character
        // available than the phrase is long. Incrementing this lengh ensures we
        // don't use a prefix cache that could cut off the word break.
        phrase_length++;
    }

    if (match_prefixes != PrefixMatch::disabled) {
        // if this is an autocomplete scan, use the prefix cache
        if (phrase_length <= MEMO_PREFIX_LENGTH_T1) {
            phrase = "=1" + phrase.substr(0, MEMO_PREFIX_LENGTH_T1);
        } else if (phrase_length <= MEMO_PREFIX_LENGTH_T2) {
            phrase = "=2" + phrase.substr(0, MEMO_PREFIX_LENGTH_T2);
        }
    }

    std::unique_ptr<rocksdb::Iterator> rit(db->NewIterator(rocksdb::ReadOptions()));
    for (rit->Seek(phrase); rit->Valid() && rit->key().ToString().compare(0, phrase.size(), phrase) == 0; rit->Next()) {
        std::string key = rit->key().ToString();

        if (match_prefixes == PrefixMatch::word_boundary) {
            // Read one character beyond the input prefix length, should always
            // be safe because of the LANGFIELD_SEPARATOR
            char endChar = key.at(phrase.length());
            if (endChar != LANGFIELD_SEPARATOR && endChar != ' ') {
                continue;
            }
        }

        // grab the langfield from the end of the key
        langfield_type message_langfield = extract_langfield(key);
        auto matches_language = static_cast<bool>(message_langfield & langfield);

        uint64_t boost = matches_language ? LANGUAGE_MATCH_BOOST : 0;
        decodeAndBboxFilter(rit->value().ToString(), array, boost, box);
    }

    std::sort(array.begin(), array.end(), std::greater<uint64_t>());
    array.erase(std::unique(array.begin(), array.end()), array.end());
    if (array.size() > max_results) array.resize(max_results);
    return array;
}

RocksDBCache::RocksDBCache() = default;

RocksDBCache::~RocksDBCache() = default;

bool RocksDBCache::pack(const std::string& filename) {
    std::shared_ptr<rocksdb::DB> existing = this->db;

    if (existing && existing->GetName() == filename) {
        throw std::invalid_argument("rocksdb file is already loaded read-only; unload first");
    }

    std::unique_ptr<rocksdb::DB> clone;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = OpenDB(options, filename, clone);

    if (!status.ok()) {
        throw std::invalid_argument("unable to open rocksdb file for packing");
    }

    // if what we have now is already a rocksdb, and it's a different
    // one from what we're being asked to pack into, copy from one to the other
    std::unique_ptr<rocksdb::Iterator> existingIt(existing->NewIterator(rocksdb::ReadOptions()));
    for (existingIt->SeekToFirst(); existingIt->Valid(); existingIt->Next()) {
        clone->Put(rocksdb::WriteOptions(), existingIt->key(), existingIt->value());
    }

    return true;
}

std::vector<std::pair<std::string, langfield_type>> RocksDBCache::list() {
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

RocksDBCache::RocksDBCache(const std::string& filename) {
    std::unique_ptr<rocksdb::DB> _db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = OpenForReadOnlyDB(options, filename, _db);

    if (!status.ok()) {
        throw std::invalid_argument("unable to open rocksdb file for loading");
    }
    this->db = std::move(_db);
}

} // namespace carmen
