#ifndef __CARMEN_NODE_UTIL_HPP__
#define __CARMEN_NODE_UTIL_HPP__

#include "binding.hpp"
#include "cpp_util.hpp"
#include "memorycache.hpp"

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

template <typename T>
inline NAN_METHOD(_genericget) {
    if (info.Length() < 1) {
        return Nan::ThrowTypeError("expected at least one info: id, [languages]");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first arg must be a String");
    }
    try {
        Nan::Utf8String utf8_id(info[0]);
        if (utf8_id.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        std::string id(*utf8_id);

        langfield_type langfield;
        if (info.Length() > 1 && !(info[1]->IsNull() || info[1]->IsUndefined())) {
            if (!info[1]->IsArray()) {
                return Nan::ThrowTypeError("second arg, if supplied must be an Array");
            }
            langfield = langarrayToLangfield(Local<Array>::Cast(info[1]));
        } else {
            langfield = ALL_LANGUAGES;
        }

        T* c = node::ObjectWrap::Unwrap<T>(info.This());
        intarray vector = __get(c, id, langfield);
        if (!vector.empty()) {
            std::size_t size = vector.size();
            Local<Array> array = Nan::New<Array>(static_cast<int>(size));
            for (uint32_t i = 0; i < size; ++i) {
                array->Set(i, Nan::New<Number>(vector[i]));
            }
            info.GetReturnValue().Set(array);
            return;
        } else {
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
}

template <typename T>
inline NAN_METHOD(_genericgetmatching) {
    if (info.Length() < 2) {
        return Nan::ThrowTypeError("expected two or three info: id, match_prefixes, [languages]");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first arg must be a String");
    }
    if (!info[1]->IsBoolean()) {
        return Nan::ThrowTypeError("second arg must be a Bool");
    }
    try {
        Nan::Utf8String utf8_id(info[0]);
        if (utf8_id.length() < 1) {
            return Nan::ThrowTypeError("first arg must be a String");
        }
        std::string id(*utf8_id);

        bool match_prefixes = info[1]->BooleanValue();

        langfield_type langfield;
        if (info.Length() > 2 && !(info[2]->IsNull() || info[2]->IsUndefined())) {
            if (!info[2]->IsArray()) {
                return Nan::ThrowTypeError("third arg, if supplied must be an Array");
            }
            langfield = langarrayToLangfield(Local<Array>::Cast(info[2]));
        } else {
            langfield = ALL_LANGUAGES;
        }

        T* c = node::ObjectWrap::Unwrap<T>(info.This());
        intarray vector = __getmatching(c, id, match_prefixes, langfield);
        if (!vector.empty()) {
            std::size_t size = vector.size();
            Local<Array> array = Nan::New<Array>(static_cast<int>(size));
            for (uint32_t i = 0; i < size; ++i) {
                auto obj = coverToObject(numToCover(vector[i]));

                // these values don't make any sense outside the context of coalesce, so delete them
                // it's a little clunky to set and then delete them, but this function as exposed
                // to node is only used in debugging/testing, so, meh
                obj->Delete(Nan::New("idx").ToLocalChecked());
                obj->Delete(Nan::New("tmpid").ToLocalChecked());
                obj->Delete(Nan::New("distance").ToLocalChecked());
                obj->Delete(Nan::New("scoredist").ToLocalChecked());
                array->Set(i, obj);
            }
            info.GetReturnValue().Set(array);
            return;
        } else {
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
}

} // namespace carmen

#endif // __CARMEN_NODE_UTIL_HPP__
