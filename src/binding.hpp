#ifndef __CARMEN_BINDING_HPP__
#define __CARMEN_BINDING_HPP__

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

#include "node_util.hpp"
#include "coalesce.hpp"
#include "normalizationcache.hpp"
#include "memorycache.hpp"
#include "rocksdbcache.hpp"

#include "radix_max_heap.h"

#include <algorithm>
#include <deque>
#include <exception>
#include <limits>
#include <list>
#include <map>
#include <nan.h>
#include <string>
#include <vector>
#pragma clang diagnostic pop
#include <cassert>
#include <cstring>
#include <fstream>
#include <iostream>
#include <tuple>

namespace carmen {

using namespace v8;


} // namespace carmen

#endif // __CARMEN_BINDING_HPP__
