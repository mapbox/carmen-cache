#ifndef __CARMEN_NODE_UTIL_HPP__
#define __CARMEN_NODE_UTIL_HPP__

#include "binding.hpp"
#include "cpp_util.hpp"

namespace carmen {

using namespace v8;

Local<Object> coverToObject(Cover const& cover);
Local<Array> contextToArray(Context const& context);

constexpr unsigned MAX_LANG = (sizeof(langfield_type) * 8) - 1;
inline langfield_type langarrayToLangfield(Local<v8::Array> const& array) {
    size_t array_size = array->Length();
    langfield_type out = 0;
    for (unsigned i = 0; i < array_size; i++) {
        unsigned int val = static_cast<unsigned int>(array->Get(i)->NumberValue());
        if (val > MAX_LANG) {
            // this should probably throw something
            continue;
        }
        out = out | (static_cast<langfield_type>(1) << val);
    }
    return out;
}

inline Local<v8::Array> langfieldToLangarray(langfield_type langfield) {
    Local<Array> langs = Nan::New<Array>();

    unsigned idx = 0;
    for (unsigned i = 0; i <= MAX_LANG; i++) {
        if (langfield & (static_cast<langfield_type>(1) << i)) {
            langs->Set(idx++, Nan::New(i));
        }
    }
    return langs;
}

} // namespace carmen

#endif // __CARMEN_NODE_UTIL_HPP__