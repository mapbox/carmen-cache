#ifndef __CARMEN_CACHE_HPP__
#define __CARMEN_CACHE_HPP__

#include <string>

namespace carmen {
    void coalesceSingle(uv_work_t* req);
    void coalesceMulti(uv_work_t* req);
}

#endif
