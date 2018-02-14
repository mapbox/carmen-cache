#include "binding.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <memory>
#include <sstream>

#include <protozero/pbf_reader.hpp>
#include <protozero/pbf_writer.hpp>

#include <chrono>
typedef std::chrono::high_resolution_clock Clock;

namespace carmen {

using namespace v8;

extern "C" {
static void start(Handle<Object> target) {
    MemoryCache::Initialize(target);
    RocksDBCache::Initialize(target);
    NormalizationCache::Initialize(target);
    Nan::SetMethod(target, "coalesce", coalesce);
}
}

} // namespace carmen

NODE_MODULE(carmen, carmen::start)
