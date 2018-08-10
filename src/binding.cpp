#include "binding.hpp"

namespace carmen {

using namespace v8;

extern "C" {
static void start(Handle<Object> target) {
    MemoryCache::Initialize(target);
    RocksDBCache::Initialize(target);
    NormalizationCache::Initialize(target);
    Features::Initialize(target);
    Nan::SetMethod(target, "coalesce", coalesce);
}
}

} // namespace carmen

// this macro expansion includes an old-style cast and is beyond our control
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wold-style-cast"
NODE_MODULE(carmen, carmen::start)
#pragma clang diagnostic pop
