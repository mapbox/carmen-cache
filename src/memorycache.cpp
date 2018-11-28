
#include "memorycache.hpp"
#include "cpp_util.hpp"

namespace carmen {

intarray MemoryCache::__get(const std::string& phrase, langfield_type langfield) {
    arraycache const& cache = this->cache_;
    intarray array;
    std::string phrase_with_langfield = phrase;

    add_langfield(phrase_with_langfield, langfield);
    auto aitr = cache.find(phrase_with_langfield);
    if (aitr != cache.end()) {
        array = aitr->second;
    }
    std::sort(array.begin(), array.end(), std::greater<uint64_t>());
    return array;
}

intarray MemoryCache::__getmatching(const std::string& phrase_ref, bool match_prefixes, langfield_type langfield) {
    intarray array;
    std::string phrase = phrase_ref;

    if (!match_prefixes) phrase.push_back(LANGFIELD_SEPARATOR);
    size_t phrase_length = phrase.length();
    const char* phrase_data = phrase.data();
    // Load values from memory cache

    for (auto const& item : this->cache_) {
        const char* item_data = item.first.data();
        size_t item_length = item.first.length();

        if (item_length < phrase_length) continue;

        if (memcmp(phrase_data, item_data, phrase_length) == 0) {
            langfield_type message_langfield = extract_langfield(item.first);

            if ((message_langfield & langfield) != 0u) {
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

MemoryCache::MemoryCache() = default;

MemoryCache::~MemoryCache() = default;

bool MemoryCache::pack(const std::string& filename) {
    std::unique_ptr<rocksdb::DB> db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = OpenDB(options, filename, db);

    if (!status.ok()) {
        throw std::invalid_argument("unable to open rocksdb file for packing");
    }

    std::map<key_type, std::deque<value_type>> memoized_prefixes;

    for (auto const& item : this->cache_) {
        std::size_t array_size = item.second.size();
        if (array_size > 0) {
            // make copy of intarray so we can sort without
            // modifying the original array
            intarray varr = item.second;

            // delta-encode values, sorted in descending order.
            std::sort(varr.begin(), varr.end(), std::greater<uint64_t>());

            packVec(varr, db, item.first);

            std::string prefix_t1;
            std::string prefix_t2;

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

            if (!prefix_t1.empty()) {
                std::map<key_type, std::deque<value_type>>::const_iterator mitr = memoized_prefixes.find(prefix_t1);
                if (mitr == memoized_prefixes.end()) {
                    memoized_prefixes.emplace(prefix_t1, std::deque<value_type>());
                }
                std::deque<value_type>& buf = memoized_prefixes[prefix_t1];

                buf.insert(buf.end(), varr.begin(), varr.end());
            }
            if (!prefix_t2.empty()) {
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

    return true;
}

std::vector<std::pair<std::string, langfield_type>> MemoryCache::list() {
    std::vector<std::pair<std::string, langfield_type>> out;

    for (auto const& item : this->cache_) {
        std::string phrase = item.first.substr(0, item.first.find(LANGFIELD_SEPARATOR));
        langfield_type langfield = extract_langfield(item.first);

        out.emplace_back(phrase, langfield);
    }

    return out;
}

/**
 * Replaces or appends the data for a given key
 *
 * @name set
 * @memberof MemoryCache
 * @param {String} id
 * @param {Array}, data; an array of numbers where each number represents a grid
 * @param {Array} an array of relevant languages
 * @param {Boolean} T: append to data, F: replace data
 * @returns undefined
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

void MemoryCache::_set(std::string key_id, std::vector<uint64_t> data, langfield_type langfield, bool append) {
    arraycache& arrc = this->cache_;
    add_langfield(key_id, langfield);

    auto itr2 = arrc.find(key_id);
    if (itr2 == arrc.end()) {
        arrc.emplace(key_id, intarray());
    }
    intarray& vv = arrc[key_id];

    size_t array_size = data.size();

    if (append) {
        vv.reserve(vv.size() + array_size);
    } else {
        if (itr2 != arrc.end()) vv.clear();
        vv.reserve(array_size);
    }

    vv.insert(vv.end(), data.begin(), data.end());
}

} // namespace carmen
