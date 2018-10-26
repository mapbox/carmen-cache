#ifndef __CARMEN_COALESCE_HPP__
#define __CARMEN_COALESCE_HPP__

#include "cpp_util.hpp"
#include "node_util.hpp"

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

struct CoalesceBaton : carmen::noncopyable {
    uv_work_t request;
    // params
    std::vector<PhrasematchSubq> stack;
    std::vector<uint64_t> centerzxy;
    std::vector<uint64_t> bboxzxy;
    double radius;
    Nan::Persistent<v8::Function> callback;
    // return
    std::vector<Context> features;
    // error
    std::string error;
};

inline std::vector<Context> coalesce(std::vector<PhrasematchSubq>& stack, const std::vector<uint64_t>& centerzxy, const std::vector<uint64_t>& bboxzxy, double radius);
inline std::vector<Context> coalesceSingle(std::vector<PhrasematchSubq>& stack, const std::vector<uint64_t>& centerzxy, const std::vector<uint64_t> & bboxzxy, double radius);
inline std::vector<Context> coalesceMulti(std::vector<PhrasematchSubq>& stack, const std::vector<uint64_t>& centerzxy, const std::vector<uint64_t> & bboxzxy, double radius);

NAN_METHOD(JSCoalesce);
void jsCoalesceTask(uv_work_t* req);
void jsCoalesceAfter(uv_work_t* req, int status);

} // namespace carmen

#endif // __CARMEN_COALESCE_HPP__
