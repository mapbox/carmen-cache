#include "binding.hpp"

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
