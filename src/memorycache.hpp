#ifndef __CARMEN_MEMORYCACHE_HPP__
#define __CARMEN_MEMORYCACHE_HPP__

#include "cpp_util.hpp"

namespace carmen {

class MemoryCache {
  public:
    MemoryCache();
    ~MemoryCache();

    bool pack(const std::string& filename);
    std::vector<std::pair<std::string, langfield_type>> list();

    void _set(std::string key_id, std::vector<uint64_t>, langfield_type langfield, bool append);

    std::vector<uint64_t> _get(std::string& phrase, std::vector<uint64_t> languages);
    std::vector<uint64_t> _getmatching(std::string phrase, bool match_prefixes, std::vector<uint64_t> languages);

    std::vector<uint64_t> __get(std::string& phrase, langfield_type langfield);
    std::vector<uint64_t> __getmatching(std::string phrase, bool match_prefixes, langfield_type langfield);

    arraycache cache_;
};

} // namespace carmen

#endif // __CARMEN_MEMORYCACHE_HPP__
