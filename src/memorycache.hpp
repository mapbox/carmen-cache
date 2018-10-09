#ifndef __CARMEN_MEMORYCACHE_HPP__
#define __CARMEN_MEMORYCACHE_HPP__

#include "cpp_util.hpp"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunknown-pragmas"
#pragma clang diagnostic ignored "-Wconversion"
#pragma clang diagnostic ignored "-Wshadow"
#pragma clang diagnostic ignored "-Wsign-compare"
#pragma clang diagnostic ignored "-Wunused-local-typedef"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wpadded"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wsign-conversion"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"

#include <nan.h>

#pragma clang diagnostic pop

namespace carmen {

class MemoryCache {
    public:
        MemoryCache();
        ~MemoryCache();

        bool pack(std::string filename);
        std::vector<std::pair<std::string, langfield_type>> list();

        void _set(std::string id, std::vector<uint64_t>, langfield_type langfield, bool append);

        std::vector<uint64_t> _get(std::string phrase, std::vector<uint64_t> languages);
        std::vector<uint64_t> _getmatching(std::string phrase, bool match_prefixes, std::vector<uint64_t> languages);

        std::vector<uint64_t> __get(std::string phrase, langfield_type langfield);
        std::vector<uint64_t> __getmatching(std::string phrase, bool match_prefixes, langfield_type langfield);

        arraycache cache_;
};

} // namespace carmen

#endif // __CARMEN_MEMORYCACHE_HPP__
