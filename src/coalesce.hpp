#ifndef __CARMEN_COALESCE_HPP__
#define __CARMEN_COALESCE_HPP__

#include "cpp_util.hpp"
#include "node_util.hpp"
#include <nan.h>

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

NAN_METHOD(coalesce);
void coalesceSingle(uv_work_t* req);
void coalesceMulti(uv_work_t* req);
void coalesceAfter(uv_work_t* req);
void coalesceFinalize(CoalesceBaton* baton, std::vector<Context>&& contexts);

} // namespace carmen

#endif // __CARMEN_COALESCE_HPP__
