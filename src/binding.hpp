#ifndef __CARMEN_BINDING_HPP__
#define __CARMEN_BINDING_HPP__

#include "coalesce.hpp"
#include "memorycache.hpp"
#include "node_util.hpp"
#include "normalizationcache.hpp"
#include "rocksdbcache.hpp"

// this is an external library, so squash this warning
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-conversion"
#include "radix_max_heap.h"
#pragma clang diagnostic pop

namespace carmen {

using namespace v8;

} // namespace carmen

#endif // __CARMEN_BINDING_HPP__
