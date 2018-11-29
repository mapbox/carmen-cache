#ifndef __CARMEN_COALESCE_HPP__
#define __CARMEN_COALESCE_HPP__

#include "cpp_util.hpp"

namespace carmen {

std::vector<Context> coalesce(std::vector<PhrasematchSubq>& stack, const std::vector<uint64_t>& centerzxy, const std::vector<uint64_t>& bboxzxy, double radius);
inline std::vector<Context> coalesceSingle(std::vector<PhrasematchSubq>& stack, const std::vector<uint64_t>& centerzxy, const std::vector<uint64_t>& bboxzxy, double radius);
inline std::vector<Context> coalesceMulti(std::vector<PhrasematchSubq>& stack, const std::vector<uint64_t>& centerzxy, const std::vector<uint64_t>& bboxzxy, double radius);

} // namespace carmen

#endif // __CARMEN_COALESCE_HPP__
