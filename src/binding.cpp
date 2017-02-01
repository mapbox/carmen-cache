
#include "binding.hpp"

#include <sstream>
#include <cmath>
#include <cassert>
#include <limits>
#include <algorithm>
#include <memory>

#include <protozero/pbf_writer.hpp>
#include <protozero/pbf_reader.hpp>

namespace carmen {

using namespace v8;

Nan::Persistent<FunctionTemplate> Cache::constructor;

inline std::string shard(uint64_t level, uint64_t id) {
    if (level == 0) return "0";
    unsigned int bits = 52 - (static_cast<unsigned int>(level) * 4);
    unsigned int shard_id = static_cast<unsigned int>(std::floor(id / static_cast<double>(std::pow(2, bits))));
    return std::to_string(shard_id);
}

inline std::vector<unsigned> arrayToVector(Local<Array> const& array) {
    std::vector<unsigned> cpp_array;
    cpp_array.reserve(array->Length());
    for (uint32_t i = 0; i < array->Length(); i++) {
        int64_t js_value = array->Get(i)->IntegerValue();
        if (js_value < 0 || js_value >= std::numeric_limits<unsigned>::max()) {
            std::stringstream s;
            s << "value in array too large (cannot fit '" << js_value << "' in unsigned)";
            throw std::runtime_error(s.str());
        }
        cpp_array.emplace_back(static_cast<unsigned>(js_value));
    }
    return cpp_array;
}

inline std::vector<double> arrayToDoubleVector(Local<Array> const& array) {
    std::vector<double> cpp_array;
    cpp_array.reserve(array->Length());
    for (uint32_t i = 0; i < array->Length(); i++) {
        double js_value = array->Get(i)->NumberValue();
        cpp_array.emplace_back(js_value);
    }
    return cpp_array;
}

inline Local<Array> vectorToArray(Cache::intarray const& vector) {
    std::size_t size = vector.size();
    Local<Array> array = Nan::New<Array>(static_cast<int>(size));
    for (uint32_t i = 0; i < size; i++) {
        array->Set(i, Nan::New<Number>(vector[i]));
    }
    return array;
}

inline Local<Object> mapToObject(std::map<std::uint64_t,std::uint64_t> const& map) {
    Local<Object> object = Nan::New<Object>();
    for (auto const& item : map) {
        object->Set(Nan::New<Number>(item.first), Nan::New<Number>(item.second));
    }
    return object;
}

inline std::string cacheGet(Cache const* c, std::string const& key) {
    Cache::message_cache const& messages = c->msg_;
    Cache::message_cache::const_iterator mitr = messages.find(key);
    return mitr->second->second;
}

inline bool cacheHas(Cache const* c, std::string const& key) {
    Cache::message_cache const& messages = c->msg_;
    Cache::message_cache::const_iterator mitr = messages.find(key);
    return mitr != messages.end();
}

inline void cacheInsert(Cache & c, std::string const& key, const char * data, std::size_t data_size) {
    Cache::message_cache &messages = c.msg_;
    Cache::message_cache::iterator mitr = messages.find(key);
    if (mitr == messages.end()) {
        Cache::message_list &list = c.msglist_;
        list.emplace_front(key, std::string(data,data_size));
        messages.emplace(key, list.begin());
        if (list.size() > c.cachesize) {
            messages.erase(list.back().first);
            list.pop_back();
        }
    }
}

inline bool cacheRemove(Cache * c, std::string const& key) {
    Cache::message_list &list = c->msglist_;
    Cache::message_cache &messages = c->msg_;
    Cache::message_cache::iterator mitr = messages.find(key);
    if (mitr != messages.end()) {
        list.erase(mitr->second);
        messages.erase(mitr);
        return true;
    } else {
        return false;
    }
}

Cache::intarray __get(Cache const* c, std::string const& type, std::string const& shard, uint64_t id) {
    std::string key = type + "-" + shard;
    Cache::memcache const& mem = c->cache_;
    Cache::memcache::const_iterator itr = mem.find(key);
    Cache::intarray array;
    if (itr == mem.end()) {
        if (!cacheHas(c, key)) return array;

        std::string ref = cacheGet(c, key);
        protozero::pbf_reader message(ref);
        while (message.next(CACHE_MESSAGE)) {
            protozero::pbf_reader item = message.get_message();
            while (item.next(CACHE_ITEM)) {
                uint64_t key_id = item.get_uint64();
                if (key_id != id) break;
                item.next();
                auto vals = item.get_packed_uint64();
                uint64_t lastval = 0;
                // delta decode values.
                for (auto it = vals.first; it != vals.second; ++it) {
                    if (lastval == 0) {
                        lastval = *it;
                        array.emplace_back(lastval);
                    } else {
                        lastval = lastval - *it;
                        array.emplace_back(lastval);
                    }
                }
                return array;
            }
        }
        return array;
    } else {
        Cache::arraycache::const_iterator aitr = itr->second.find(id);
        if (aitr == itr->second.end()) {
            return array;
        } else {
            return aitr->second;
        }
    }
}

Cache::intarray __getTruncated(Cache const* c, std::string const& type, std::string const& shard, uint64_t id, uint64_t truncate) {
    std::string key = type + "-" + shard;
    Cache::memcache const& mem = c->cache_;
    Cache::memcache::const_iterator itr = mem.find(key);
    Cache::intarray array;
    if (itr == mem.end()) {
        if (!cacheHas(c, key)) return array;

        std::string ref = cacheGet(c, key);
        protozero::pbf_reader message(ref);
        while (message.next(CACHE_MESSAGE)) {
            protozero::pbf_reader item = message.get_message();
            while (item.next(CACHE_ITEM)) {
                uint64_t key_id = item.get_uint64();
                if (key_id != id) break;
                item.next();
                auto vals = item.get_packed_uint64();
                uint64_t lastval = 0;
                uint64_t length = 0;
                // delta decode values.
                for (auto it = vals.first; it != vals.second; ++it) {
                    if (lastval == 0) {
                        lastval = *it;
                        array.emplace_back(lastval);
                    } else {
                        lastval = lastval - *it;
                        array.emplace_back(lastval);
                    }
                    length++;
                    if (length >= truncate) break;
                }
                return array;
            }
        }
        return array;
    } else {
        Cache::arraycache::const_iterator aitr = itr->second.find(id);
        if (aitr == itr->second.end()) {
            return array;
        } else {
            return aitr->second;
        }
    }
}

void Cache::Initialize(Handle<Object> target) {
    Nan::HandleScope scope;
    Local<FunctionTemplate> t = Nan::New<FunctionTemplate>(Cache::New);
    t->InstanceTemplate()->SetInternalFieldCount(1);
    t->SetClassName(Nan::New("Cache").ToLocalChecked());
    Nan::SetPrototypeMethod(t, "has", has);
    Nan::SetPrototypeMethod(t, "loadSync", loadSync);
    Nan::SetPrototypeMethod(t, "pack", pack);
    Nan::SetPrototypeMethod(t, "merge", merge);
    Nan::SetPrototypeMethod(t, "list", list);
    Nan::SetPrototypeMethod(t, "_set", _set);
    Nan::SetPrototypeMethod(t, "_get", _get);
    Nan::SetPrototypeMethod(t, "unload", unload);
    Nan::SetMethod(t, "coalesce", coalesce);
    target->Set(Nan::New("Cache").ToLocalChecked(), t->GetFunction());
    constructor.Reset(t);
}

Cache::Cache()
  : ObjectWrap(),
    cache_(),
    msg_() {}

Cache::~Cache() { }

NAN_METHOD(Cache::pack)
{
    if (info.Length() < 1) {
        return Nan::ThrowTypeError("expected two info: 'type', 'shard'");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first argument must be a String");
    }
    if (!info[1]->IsNumber()) {
        return Nan::ThrowTypeError("second arg must be an Integer");
    }
    try {
        std::string type = *String::Utf8Value(info[0]->ToString());
        std::string shard = *String::Utf8Value(info[1]->ToString());
        std::string key = type + "-" + shard;
        Cache* c = node::ObjectWrap::Unwrap<Cache>(info.This());
        Cache::memcache const& mem = c->cache_;
        Cache::memcache::const_iterator itr = mem.find(key);
        if (itr != mem.end()) {
            std::string message;
            // Optimization idea: pre-pass on arrays to assemble guess about
            // how long the final message will be in order to be able to call
            // message.reserve(<length>)
            protozero::pbf_writer writer(message);
            for (auto const& item : itr->second) {
                std::size_t array_size = item.second.size();
                if (array_size > 0) {
                    protozero::pbf_writer item_writer(writer,1);
                    item_writer.add_uint64(1,item.first);
                    // make copy of intarray so we can sort without
                    // modifying the original array
                    Cache::intarray varr = item.second;
                    // delta-encode values, sorted in descending order.
                    std::sort(varr.begin(), varr.end(), std::greater<uint64_t>());

                    // Using new (in protozero 1.3.0) packed writing API
                    // https://github.com/mapbox/protozero/commit/4e7e32ac5350ea6d3dcf78ff5e74faeee513a6e1
                    protozero::packed_field_uint64 field{item_writer, 2};
                    uint64_t lastval = 0;
                    for (auto const& vitem : varr) {
                        if (lastval == 0) {
                            field.add_element(static_cast<uint64_t>(vitem));
                        } else {
                            field.add_element(static_cast<uint64_t>(lastval - vitem));
                        }
                        lastval = vitem;
                    }
                }
            }
            if (message.empty()) {
                return Nan::ThrowTypeError("pack: invalid message ByteSize encountered");
            } else {
                info.GetReturnValue().Set(Nan::CopyBuffer(message.data(), message.size()).ToLocalChecked());
                return;
            }
        } else {
            if (!cacheHas(c, key)) {
                return Nan::ThrowTypeError("pack: cannot pack empty data");
            } else {
                std::string ref = cacheGet(c, key);
                Local<Object> buf = Nan::CopyBuffer((char*)ref.data(), ref.size()).ToLocalChecked();
                info.GetReturnValue().Set(buf);
                return;
            }
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

struct MergeBaton : carmen::noncopyable {
    uv_work_t request;
    std::unique_ptr<std::string> pbf3;
    const char * pbf1_data;
    const char * pbf2_data;
    std::string method;
    std::string error;
    Nan::Persistent<v8::Function> callback;
    Nan::Persistent<v8::Object> buffer1;
    Nan::Persistent<v8::Object> buffer2;
    std::size_t pbf1_size;
    std::size_t pbf2_size;
    MergeBaton(Local<Object> obj1,
               Local<Object> obj2,
               Local<Value> cb,
               std::string const& meth)
      : pbf1_data(node::Buffer::Data(obj1)),
        pbf1_size(node::Buffer::Length(obj1)),
        pbf2_data(node::Buffer::Data(obj2)),
        pbf2_size(node::Buffer::Length(obj2)),
        pbf3(std::make_unique<std::string>()),
        method(meth)
      {
        this->request.data = this;
        callback.Reset(cb.As<Function>());
        buffer1.Reset(obj1);
        buffer2.Reset(obj2);
        // Reserve memory at least at pbf1_size. TODO: should we reserve more?
        pbf3.get()->reserve(pbf1_size);
      }
    ~MergeBaton() {
        callback.Reset();
        buffer1.Reset();
        buffer2.Reset();        
    }

};

void mergeQueue(uv_work_t* req) {
    MergeBaton *baton = static_cast<MergeBaton *>(req->data);

    std::string const& method = baton->method;

    // Ids that have been seen
    std::map<uint64_t,bool> ids1;
    std::map<uint64_t,bool> ids2;

    std::string & merged = *baton->pbf3.get();
    try {

        protozero::pbf_writer writer(merged);

        // Store ids from 1
        protozero::pbf_reader pre1(baton->pbf1_data,baton->pbf1_size);
        while (pre1.next(CACHE_MESSAGE)) {
            protozero::pbf_reader item = pre1.get_message();
            while (item.next(CACHE_ITEM)) {
                ids1.emplace(item.get_uint64(), true);
            }
        }

        // Store ids from 2
        protozero::pbf_reader pre2(baton->pbf2_data,baton->pbf2_size);
        while (pre2.next(CACHE_MESSAGE)) {
            protozero::pbf_reader item = pre2.get_message();
            while (item.next(CACHE_ITEM)) {
                ids2.emplace(item.get_uint64(), true);
            }
        }

        // No delta writes from message1
        protozero::pbf_reader message1(baton->pbf1_data,baton->pbf1_size);
        while (message1.next(CACHE_MESSAGE)) {
            protozero::pbf_writer item_writer(writer,1);
            protozero::pbf_reader item = message1.get_message();
            while (item.next(CACHE_ITEM)) {
                uint64_t key_id = item.get_uint64();

                // Skip this id if also in message 2
                if (ids2.find(key_id) != ids2.end()) break;

                item_writer.add_uint64(1,key_id);
                item.next();
                protozero::packed_field_uint64 field{item_writer, 2};
                auto vals = item.get_packed_uint64();
                for (auto it = vals.first; it != vals.second; ++it) {
                    field.add_element(static_cast<uint64_t>(*it));
                }
            }
        }

        // No delta writes from message2
        protozero::pbf_reader message2(baton->pbf2_data,baton->pbf2_size);
        while (message2.next(CACHE_MESSAGE)) {
            protozero::pbf_writer item_writer(writer,1);
            protozero::pbf_reader item = message2.get_message();
            while (item.next(CACHE_ITEM)) {
                uint64_t key_id = item.get_uint64();

                // Skip this id if also in message 2
                if (ids1.find(key_id) != ids1.end()) break;

                item_writer.add_uint64(1,key_id);
                item.next();
                protozero::packed_field_uint64 field{item_writer, 2};
                auto vals = item.get_packed_uint64();
                for (auto it = vals.first; it != vals.second; ++it) {
                    field.add_element(static_cast<uint64_t>(*it));
                }
            }
        }

        // Delta writes for ids in both message1 and message2
        protozero::pbf_reader overlap1(baton->pbf1_data,baton->pbf1_size);
        while (overlap1.next(CACHE_MESSAGE)) {
            protozero::pbf_writer item_writer(writer,1);
            protozero::pbf_reader item = overlap1.get_message();
            while (item.next(CACHE_ITEM)) {
                uint64_t key_id = item.get_uint64();

                // Skip ids that are only in one or the other lists
                if (ids1.find(key_id) == ids1.end() || ids2.find(key_id) == ids2.end()) break;

                item_writer.add_uint64(1,key_id);

                item.next();
                uint64_t lastval = 0;
                Cache::intarray varr;

                // Add values from pbf1
                auto vals = item.get_packed_uint64();
                for (auto it = vals.first; it != vals.second; ++it) {
                    if (method == "freq") {
                        varr.emplace_back(*it);
                        break;
                    } else if (lastval == 0) {
                        lastval = *it;
                        varr.emplace_back(lastval);
                    } else {
                        lastval = lastval - *it;
                        varr.emplace_back(lastval);
                    }
                }

                // Check pbf2 for this id and merge its items if found
                protozero::pbf_reader overlap2(baton->pbf2_data,baton->pbf2_size);
                while (overlap2.next(CACHE_MESSAGE)) {
                    protozero::pbf_reader item2 = overlap2.get_message();
                    while (item2.next(CACHE_ITEM)) {
                        uint64_t key_id2 = item2.get_uint64();
                        if (key_id2 != key_id) break;
                        item2.next();
                        lastval = 0;
                        auto vals2 = item2.get_packed_uint64();
                        for (auto it = vals2.first; it != vals2.second; ++it) {
                            if (method == "freq") {
                                if (key_id2 == 1) {
                                    varr[0] = varr[0] > *it ? varr[0] : *it;
                                } else {
                                    varr[0] = varr[0] + *it;
                                }
                                break;
                            } else if (lastval == 0) {
                                lastval = *it;
                                varr.emplace_back(lastval);
                            } else {
                                lastval = lastval - *it;
                                varr.emplace_back(lastval);
                            }
                        }
                    }
                }

                // Sort for proper delta encoding
                std::sort(varr.begin(), varr.end(), std::greater<uint64_t>());

                // Write varr to merged protobuf
                protozero::packed_field_uint64 field{item_writer, 2};
                lastval = 0;
                for (auto const& vitem : varr) {
                    if (lastval == 0) {
                        field.add_element(static_cast<uint64_t>(vitem));
                    } else {
                        field.add_element(static_cast<uint64_t>(lastval - vitem));
                    }
                    lastval = vitem;
                }
            }
        }
    } catch (std::exception const& ex) {
        baton->error = ex.what();
    }
}

void mergeAfter(uv_work_t* req) {
    Nan::HandleScope scope;
    MergeBaton *baton = static_cast<MergeBaton *>(req->data);
    if (!baton->error.empty()) {
        v8::Local<v8::Value> argv[1] = { Nan::Error(baton->error.c_str()) };
        Nan::MakeCallback(Nan::GetCurrentContext()->Global(), Nan::New(baton->callback), 1, argv);
    } else {
        std::string & merged = *baton->pbf3.get();
        baton->pbf3.release();
        v8::Local<v8::Value> argv[2] = { Nan::Null(),
                                         Nan::NewBuffer(&merged[0],
                                            merged.size(),
                                            [](char *, void * hint) {
                                                delete reinterpret_cast<std::string*>(hint);
                                            },
                                            baton->pbf3.get()
                                         ).ToLocalChecked()
                                       };
        Nan::MakeCallback(Nan::GetCurrentContext()->Global(), Nan::New(baton->callback), 2, argv);
    }
    delete baton;
}

NAN_METHOD(Cache::list)
{
    if (info.Length() < 1) {
        return Nan::ThrowTypeError("expected at least one arg: 'type' and optional a 'shard'");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first argument must be a String");
    }
    try {
        std::string type = *String::Utf8Value(info[0]->ToString());
        Cache* c = node::ObjectWrap::Unwrap<Cache>(info.This());
        Cache::memcache const& mem = c->cache_;
        Cache::message_cache const& messages = c->msg_;
        Local<Array> ids = Nan::New<Array>();
        if (info.Length() == 1) {
            unsigned idx = 0;
            std::size_t type_size = type.size();
            for (auto const& item : mem) {
                std::size_t item_size = item.first.size();
                if (item_size > type_size && item.first.substr(0,type_size) == type) {
                    std::string shard = item.first.substr(type_size+1,item_size);
                    ids->Set(idx++, Nan::New(Nan::New(shard.c_str()).ToLocalChecked()->NumberValue()));
                }
            }
            for (auto const& item : messages) {
                std::size_t item_size = item.first.size();
                if (item_size > type_size && item.first.substr(0,type_size) == type) {
                    std::string shard = item.first.substr(type_size+1,item_size);
                    ids->Set(idx++, Nan::New(Nan::New(shard.c_str()).ToLocalChecked()->NumberValue()));
                }
            }
            info.GetReturnValue().Set(ids);
            return;
        } else if (info.Length() == 2) {
            std::string shard = *String::Utf8Value(info[1]->ToString());
            std::string key = type + "-" + shard;
            Cache::memcache::const_iterator itr = mem.find(key);
            unsigned idx = 0;
            if (itr != mem.end()) {
                for (auto const& item : itr->second) {
                    ids->Set(idx++,Nan::New<Number>(item.first)->ToString());
                }
            }

            // parse message for ids
            if (cacheHas(c, key)) {
                std::string ref = cacheGet(c, key);

                protozero::pbf_reader message(ref);
                while (message.next(CACHE_MESSAGE)) {
                    protozero::pbf_reader item = message.get_message();
                    while (item.next(CACHE_ITEM)) {
                        uint64_t key_id = item.get_uint64();
                        ids->Set(idx++, Nan::New<Number>(key_id)->ToString());
                    }
                }
            }

            info.GetReturnValue().Set(ids);
            return;
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(Cache::merge)
{
    if (!info[0]->IsObject()) return Nan::ThrowTypeError("argument 1 must be a Buffer");
    if (!info[1]->IsObject()) return Nan::ThrowTypeError("argument 2 must be a Buffer");
    if (!info[2]->IsString()) return Nan::ThrowTypeError("argument 3 must be a String");
    if (!info[3]->IsFunction()) return Nan::ThrowTypeError("argument 3 must be a callback function");

    Local<Object> obj1 = info[0]->ToObject();
    Local<Object> obj2 = info[1]->ToObject();
    Local<Value> callback = info[3];
    std::string method = *String::Utf8Value(info[2]->ToString());

    if (obj1->IsNull() || obj1->IsUndefined() || !node::Buffer::HasInstance(obj1)) return Nan::ThrowTypeError("argument 1 must be a Buffer");
    if (obj2->IsNull() || obj2->IsUndefined() || !node::Buffer::HasInstance(obj2)) return Nan::ThrowTypeError("argument 2 must be a Buffer");

    MergeBaton *baton = new MergeBaton(obj1,obj2,callback,method);
    uv_queue_work(uv_default_loop(), &baton->request, mergeQueue, (uv_after_work_cb)mergeAfter);
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(Cache::_set)
{
    if (info.Length() < 3) {
        return Nan::ThrowTypeError("expected four info: 'type', 'shard', 'id', 'data'");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first argument must be a String");
    }
    if (!info[1]->IsNumber()) {
        return Nan::ThrowTypeError("second arg must be an Integer");
    }
    if (!info[2]->IsNumber()) {
        return Nan::ThrowTypeError("third arg must be an Integer");
    }
    if (!info[3]->IsArray()) {
        return Nan::ThrowTypeError("fourth arg must be an Array");
    }
    Local<Array> data = Local<Array>::Cast(info[3]);
    if (data->IsNull() || data->IsUndefined()) {
        return Nan::ThrowTypeError("an array expected for fourth argument");
    }
    try {
        std::string type = *String::Utf8Value(info[0]->ToString());
        std::string shard = *String::Utf8Value(info[1]->ToString());
        std::string key = type + "-" + shard;
        Cache* c = node::ObjectWrap::Unwrap<Cache>(info.This());
        Cache::memcache & mem = c->cache_;
        Cache::memcache::const_iterator itr = mem.find(key);
        if (itr == mem.end()) {
            c->cache_.emplace(key,Cache::arraycache());
        }
        Cache::arraycache & arrc = c->cache_[key];
        Cache::arraycache::key_type key_id = static_cast<Cache::arraycache::key_type>(info[2]->IntegerValue());
        Cache::arraycache::iterator itr2 = arrc.find(key_id);
        if (itr2 == arrc.end()) {
            arrc.emplace(key_id,Cache::intarray());
        }
        Cache::intarray & vv = arrc[key_id];

        unsigned array_size = data->Length();
        if (info[4]->IsBoolean() && info[4]->BooleanValue()) {
            vv.reserve(vv.size() + array_size);
        } else {
            if (itr2 != arrc.end()) vv.clear();
            vv.reserve(array_size);
        }

        for (unsigned i=0;i<array_size;++i) {
            vv.emplace_back(static_cast<uint64_t>(data->Get(i)->NumberValue()));
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

/*

Note: This function will not override data for keys already inserted into the cache

*/


NAN_METHOD(Cache::loadSync)
{
    if (info.Length() < 2) {
        return Nan::ThrowTypeError("expected at least three args: 'buffer', 'type', and 'shard'");
    }
    if (!info[0]->IsObject()) {
        return Nan::ThrowTypeError("first argument must be a Buffer");
    }
    Local<Object> obj = info[0]->ToObject();
    if (obj->IsNull() || obj->IsUndefined()) {
        return Nan::ThrowTypeError("a buffer expected for first argument");
    }
    if (!node::Buffer::HasInstance(obj)) {
        return Nan::ThrowTypeError("first argument must be a Buffer");
    }
    if (!info[1]->IsString()) {
        return Nan::ThrowTypeError("second arg 'type' must be a String");
    }
    if (!info[2]->IsNumber()) {
        return Nan::ThrowTypeError("third arg 'shard' must be an Integer");
    }
    try {
        std::string type = *String::Utf8Value(info[1]->ToString());
        std::string shard = *String::Utf8Value(info[2]->ToString());
        std::string key = type + "-" + shard;
        Cache & c = *node::ObjectWrap::Unwrap<Cache>(info.This());
        cacheInsert(c, key, node::Buffer::Data(obj), node::Buffer::Length(obj));
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

NAN_METHOD(Cache::has)
{
    if (info.Length() < 2) {
        return Nan::ThrowTypeError("expected two info: 'type' and 'shard'");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first arg must be a String");
    }
    if (!info[1]->IsNumber()) {
        return Nan::ThrowTypeError("second arg must be an Integer");
    }
    try {
        std::string type = *String::Utf8Value(info[0]->ToString());
        std::string shard = *String::Utf8Value(info[1]->ToString());
        std::string key = type + "-" + shard;
        Cache* c = node::ObjectWrap::Unwrap<Cache>(info.This());
        Cache::memcache const& mem = c->cache_;
        Cache::memcache::const_iterator itr = mem.find(key);
        if (itr != mem.end()) {
            info.GetReturnValue().Set(Nan::True());
            return;
        } else {
            if (cacheHas(c, key)) {
                info.GetReturnValue().Set(Nan::True());
                return;
            } else {
                info.GetReturnValue().Set(Nan::False());
                return;
            }
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
}

NAN_METHOD(Cache::_get)
{
    if (info.Length() < 3) {
        return Nan::ThrowTypeError("expected three info: type, shard, and id");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first arg must be a String");
    }
    if (!info[1]->IsNumber()) {
        return Nan::ThrowTypeError("second arg must be an Integer");
    }
    if (!info[2]->IsNumber()) {
        return Nan::ThrowTypeError("third arg must be a positive Integer");
    }
    try {
        std::string type = *String::Utf8Value(info[0]->ToString());
        std::string shard = *String::Utf8Value(info[1]->ToString());
        int64_t id = info[2]->IntegerValue();
        if (id < 0) {
            return Nan::ThrowTypeError("third arg must be a positive Integer");
        }
        uint64_t id2 = static_cast<uint64_t>(id);
        Cache* c = node::ObjectWrap::Unwrap<Cache>(info.This());
        Cache::intarray vector = __get(c, type, shard, id2);
        if (!vector.empty()) {
            info.GetReturnValue().Set(vectorToArray(vector));
            return;
        } else {
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
}

NAN_METHOD(Cache::unload)
{
    if (info.Length() < 2) {
        return Nan::ThrowTypeError("expected at least two info: 'type' and 'shard'");
    }
    if (!info[0]->IsString()) {
        return Nan::ThrowTypeError("first arg must be a String");
    }
    if (!info[1]->IsNumber()) {
        return Nan::ThrowTypeError("second arg must be an Integer");
    }
    bool hit = false;
    try {
        std::string type = *String::Utf8Value(info[0]->ToString());
        std::string shard = *String::Utf8Value(info[1]->ToString());
        std::string key = type + "-" + shard;
        Cache* c = node::ObjectWrap::Unwrap<Cache>(info.This());
        Cache::memcache & mem = c->cache_;
        Cache::memcache::iterator itr = mem.find(key);
        if (itr != mem.end()) {
            hit = true;
            mem.erase(itr);
        }
        hit = hit || cacheRemove(c, key);
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::New<Boolean>(hit));
    return;
}

NAN_METHOD(Cache::New)
{
    if (!info.IsConstructCall()) {
        return Nan::ThrowTypeError("Cannot call constructor as function, you need to use 'new' keyword");
    }
    try {
        if (info.Length() < 1) {
            return Nan::ThrowTypeError("expected 'id' argument");
        }
        if (!info[0]->IsString()) {
            return Nan::ThrowTypeError("first argument 'id' must be a String");
        }
        Cache* im = new Cache();

        if (info[1]->IsNumber()) {
            im->cachesize = static_cast<unsigned>(info[1]->NumberValue());
        }

        im->Wrap(info.This());
        info.This()->Set(Nan::New("id").ToLocalChecked(), info[0]);
        info.GetReturnValue().Set(info.This());
        return;
    } catch (std::exception const& ex) {
        return Nan::ThrowTypeError(ex.what());
    }
    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

//relev = 5 bits
//count = 3 bits
//reason = 12 bits
//* 1 bit gap
//id = 32 bits
constexpr double _pow(double x, int y)
{
    return y == 0 ? 1.0 : x * _pow(x, y-1);
}

constexpr uint64_t POW2_51 = static_cast<uint64_t>(_pow(2.0,51));
constexpr uint64_t POW2_48 = static_cast<uint64_t>(_pow(2.0,48));
constexpr uint64_t POW2_34 = static_cast<uint64_t>(_pow(2.0,34));
constexpr uint64_t POW2_28 = static_cast<uint64_t>(_pow(2.0,28));
constexpr uint64_t POW2_25 = static_cast<uint64_t>(_pow(2.0,25));
constexpr uint64_t POW2_20 = static_cast<uint64_t>(_pow(2.0,20));
constexpr uint64_t POW2_14 = static_cast<uint64_t>(_pow(2.0,14));
constexpr uint64_t POW2_3 = static_cast<uint64_t>(_pow(2.0,3));
constexpr uint64_t POW2_2 = static_cast<uint64_t>(_pow(2.0,2));

struct PhrasematchSubq {
    carmen::Cache *cache;
    double weight;
    uint64_t phrase;
    unsigned short idx;
    unsigned short zoom;
    uint32_t mask;
};

struct Cover {
    double relev;
    uint32_t id;
    uint32_t tmpid;
    unsigned short x;
    unsigned short y;
    unsigned short score;
    unsigned short idx;
    uint32_t mask;
    double distance;
    double scoredist;
};

struct Context {
    std::vector<Cover> coverList;
    uint32_t mask;
    double relev;

    Context(Context const& c) = default;
    Context(Cover && cov,
            uint32_t mask,
            double relev)
     : coverList(),
       mask(mask),
       relev(relev) {
          coverList.emplace_back(std::move(cov));
       }
    Context& operator=(Context && c) {
        coverList = std::move(c.coverList);
        mask = std::move(c.mask);
        relev = std::move(c.relev);
        return *this;
    }
    Context(std::vector<Cover> && cl,
            uint32_t mask,
            double relev)
     : coverList(std::move(cl)),
       mask(mask),
       relev(relev) {}

    Context(Context && c)
     : coverList(std::move(c.coverList)),
       mask(std::move(c.mask)),
       relev(std::move(c.relev)) {}

};

Cover numToCover(uint64_t num) {
    Cover cover;
    assert(((num >> 34) % POW2_14) <= static_cast<double>(std::numeric_limits<unsigned short>::max()));
    assert(((num >> 34) % POW2_14) >= static_cast<double>(std::numeric_limits<unsigned short>::min()));
    unsigned short y = static_cast<unsigned short>((num >> 34) % POW2_14);
    assert(((num >> 20) % POW2_14) <= static_cast<double>(std::numeric_limits<unsigned short>::max()));
    assert(((num >> 20) % POW2_14) >= static_cast<double>(std::numeric_limits<unsigned short>::min()));
    unsigned short x = static_cast<unsigned short>((num >> 20) % POW2_14);
    assert(((num >> 48) % POW2_3) <= static_cast<double>(std::numeric_limits<unsigned short>::max()));
    assert(((num >> 48) % POW2_3) >= static_cast<double>(std::numeric_limits<unsigned short>::min()));
    unsigned short score = static_cast<unsigned short>((num >> 48) % POW2_3);
    uint32_t id = static_cast<uint32_t>(num % POW2_20);
    cover.x = x;
    cover.y = y;
    double relev = 0.4 + (0.2 * static_cast<double>((num >> 51) % POW2_2));
    cover.relev = relev;
    cover.score = score;
    cover.id = id;

    // These are not derived from decoding the input num but by
    // external values after initialization.
    cover.idx = 0;
    cover.mask = 0;
    cover.tmpid = 0;
    cover.distance = 0;

    return cover;
};

struct ZXY {
    unsigned z;
    unsigned x;
    unsigned y;
};

ZXY pxy2zxy(unsigned z, unsigned x, unsigned y, unsigned target_z) {
    ZXY zxy;
    zxy.z = target_z;

    // Interval between parent and target zoom level
    unsigned zDist = target_z - z;
    unsigned zMult = zDist - 1;
    if (zDist == 0) {
        zxy.x = x;
        zxy.y = y;
        return zxy;
    }

    // Midpoint length @ z for a tile at parent zoom level
    unsigned pMid_d = static_cast<unsigned>(std::pow(2,zDist) / 2);
    assert(pMid_d <= static_cast<double>(std::numeric_limits<unsigned>::max()));
    assert(pMid_d >= static_cast<double>(std::numeric_limits<unsigned>::min()));
    unsigned pMid = static_cast<unsigned>(pMid_d);
    zxy.x = (x * zMult) + pMid;
    zxy.y = (y * zMult) + pMid;
    return zxy;
}

ZXY bxy2zxy(unsigned z, unsigned x, unsigned y, unsigned target_z, bool max=false) {
    ZXY zxy;
    zxy.z = target_z;

    // Interval between parent and target zoom level
    signed zDist = target_z - z;
    if (zDist == 0) {
        zxy.x = x;
        zxy.y = y;
        return zxy;
    }

    // zoom conversion multiplier
    float mult = static_cast<float>(std::pow(2,zDist));

    // zoom in min
    if (zDist > 0 && !max) {
        zxy.x = static_cast<unsigned>(static_cast<float>(x) * mult);
        zxy.y = static_cast<unsigned>(static_cast<float>(y) * mult);
        return zxy;
    }
    // zoom in max
    else if (zDist > 0 && max) {
        zxy.x = static_cast<unsigned>(static_cast<float>(x) * mult + (mult - 1));
        zxy.y = static_cast<unsigned>(static_cast<float>(y) * mult + (mult - 1));
        return zxy;
    }
    // zoom out
    else {
        unsigned mod = static_cast<unsigned>(std::pow(2,target_z));
        unsigned xDiff = x % mod;
        unsigned yDiff = y % mod;
        unsigned newX = x - xDiff;
        unsigned newY = y - yDiff;

        zxy.x = static_cast<unsigned>(static_cast<float>(newX) * mult);
        zxy.y = static_cast<unsigned>(static_cast<float>(newY) * mult);
        return zxy;
    }
}

inline bool coverSortByRelev(Cover const& a, Cover const& b) noexcept {
    if (b.relev > a.relev) return false;
    else if (b.relev < a.relev) return true;
    else if (b.scoredist > a.scoredist) return false;
    else if (b.scoredist < a.scoredist) return true;
    else if (b.idx < a.idx) return false;
    else if (b.idx > a.idx) return true;
    else if (b.id < a.id) return false;
    else if (b.id > a.id) return true;
    // sorting by x and y is arbitrary but provides a more deterministic output order
    else if (b.x < a.x) return false;
    else if (b.x > a.x) return true;
    else return (b.y > a.y);
}

inline bool subqSortByZoom(PhrasematchSubq const& a, PhrasematchSubq const& b) noexcept {
    if (a.zoom < b.zoom) return true;
    if (a.zoom > b.zoom) return false;
    return (a.idx < b.idx);
}

inline bool contextSortByRelev(Context const& a, Context const& b) noexcept {
    if (b.relev > a.relev) return false;
    else if (b.relev < a.relev) return true;
    else if (b.coverList[0].scoredist > a.coverList[0].scoredist) return false;
    else if (b.coverList[0].scoredist < a.coverList[0].scoredist) return true;
    else if (b.coverList[0].idx < a.coverList[0].idx) return false;
    else if (b.coverList[0].idx > a.coverList[0].idx) return true;
    return (b.coverList[0].id > a.coverList[0].id);
}

inline double tileDist(double px, double py, unsigned tileX, unsigned tileY) {
    const double tileCenterX = tileX;
    const double tileCenterY = tileY;
    const double dx = px - tileCenterX;
    const double dy = py - tileCenterY;
    const double distance = std::sqrt(dx * dx + dy * dy);

    return distance;
}

struct CoalesceBaton : carmen::noncopyable {
    uv_work_t request;
    // params
    std::vector<PhrasematchSubq> stack;
    std::vector<double> centerzxy;
    std::vector<unsigned> bboxzxy;
    Nan::Persistent<v8::Function> callback;
    // return
    std::vector<Context> features;
    // error
    std::string error;
};

// 32 tiles is about 40 miles at z14.
// Simulates 40 mile cutoff in carmen.
double scoredist(unsigned zoom, double distance, double score) {
    if (distance == 0.0) distance = 0.01;
    double scoredist = 0;
    if (zoom >= 14) scoredist = 32.0 / distance;
    if (zoom == 13) scoredist = 16.0 / distance;
    if (zoom == 12) scoredist = 8.0 / distance;
    if (zoom == 11) scoredist = 4.0 / distance;
    if (zoom == 10) scoredist = 2.0 / distance;
    if (zoom <= 9)  scoredist = 1.0 / distance;
    return score > scoredist ? score : scoredist;
}

void coalesceFinalize(CoalesceBaton* baton, std::vector<Context> && contexts) {
    if (!contexts.empty()) {
        // Coalesce stack, generate relevs.
        double relevMax = contexts[0].relev;
        std::size_t total = 0;
        std::map<uint64_t,bool> sets;
        std::map<uint64_t,bool>::iterator sit;
        std::size_t max_contexts = 40;
        baton->features.reserve(max_contexts);
        for (auto && context : contexts) {
            // Maximum allowance of coalesced features: 40.
            if (total >= max_contexts) break;

            // Since `coalesced` is sorted by relev desc at first
            // threshold miss we can break the loop.
            if (relevMax - context.relev >= 0.25) break;

            // Only collect each feature once.
            uint32_t id = context.coverList[0].tmpid;
            sit = sets.find(id);
            if (sit != sets.end()) continue;

            sets.emplace(id, true);
            baton->features.emplace_back(std::move(context));
            total++;
        }
    }
}
void coalesceSingle(uv_work_t* req) {
    CoalesceBaton *baton = static_cast<CoalesceBaton *>(req->data);

    try {
        std::vector<PhrasematchSubq> const& stack = baton->stack;
        PhrasematchSubq const& subq = stack[0];
        std::string type = "grid";
        std::string shardId = shard(4, subq.phrase);

        // proximity (optional)
        bool proximity = !baton->centerzxy.empty();
        unsigned cz;
        double cx;
        double cy;
        if (proximity) {
            cz = baton->centerzxy[0];
            cx = baton->centerzxy[1];
            cy = baton->centerzxy[2];
        } else {
            cz = 0;
            cx = 0;
            cy = 0;
        }

        // bbox (optional)
        bool bbox = !baton->bboxzxy.empty();
        unsigned minx;
        unsigned miny;
        unsigned maxx;
        unsigned maxy;
        if (bbox) {
            minx = baton->bboxzxy[1];
            miny = baton->bboxzxy[2];
            maxx = baton->bboxzxy[3];
            maxy = baton->bboxzxy[4];
        } else {
            minx = 0;
            miny = 0;
            maxx = 0;
            maxy = 0;
        }

        // sort grids by distance to proximity point
        Cache::intarray grids = (proximity || bbox) ?
            __getTruncated(subq.cache, type, shardId, subq.phrase, 500000) :
            __getTruncated(subq.cache, type, shardId, subq.phrase, 100000);

        unsigned long m = grids.size();
        double relevMax = 0;
        std::vector<Cover> covers;
        covers.reserve(m);

        uint32_t length = 0;
        uint32_t lastId = 0;
        double lastRelev = 0;
        double lastScoredist = 0;
        double lastDistance = 0;
        double minScoredist = std::numeric_limits<double>::max();
        for (unsigned long j = 0; j < m; j++) {
            Cover cover = numToCover(grids[j]);

            cover.idx = subq.idx;
            cover.tmpid = static_cast<uint32_t>(cover.idx * POW2_25 + cover.id);
            cover.relev = cover.relev * subq.weight;
            cover.distance = proximity ? tileDist(cx, cy, cover.x, cover.y) : 0;
            cover.scoredist = proximity ? scoredist(cz, cover.distance, cover.score) : cover.score;

            // only add cover id if it's got a higer scoredist
            if (lastId == cover.id && cover.scoredist <= lastScoredist && cover.distance >= lastDistance) continue;

            // short circuit based on relevMax thres
            if (length > 40) {
                if (cover.scoredist < minScoredist) continue;
                if (cover.relev < lastRelev) break;
            }
            if (relevMax - cover.relev >= 0.25) break;
            if (cover.relev > relevMax) relevMax = cover.relev;

            if (bbox) {
                if (cover.x < minx || cover.y < miny || cover.x > maxx || cover.y > maxy) continue;
            }

            covers.emplace_back(cover);
            if (lastId != cover.id) length++;
            if (!proximity && length > 40) break;
            if (cover.scoredist < minScoredist) minScoredist = cover.scoredist;
            lastId = cover.id;
            lastRelev = cover.relev;
            lastScoredist = cover.scoredist;
            lastDistance = cover.distance;
        }

        std::sort(covers.begin(), covers.end(), coverSortByRelev);

        uint32_t lastid = 0;
        std::size_t added = 0;
        std::vector<Context> contexts;
        std::size_t max_contexts = 40;
        contexts.reserve(max_contexts);
        for (auto && cover : covers) {
            // Stop at 40 contexts
            if (added == max_contexts) break;

            // Attempt not to add the same feature but by diff cover twice
            if (lastid == cover.id) continue;

            lastid = cover.id;
            added++;

            double relev = cover.relev;
            uint32_t mask = 0;
            contexts.emplace_back(std::move(cover),mask,relev);
        }

        coalesceFinalize(baton, std::move(contexts));
    } catch (std::exception const& ex) {
        baton->error = ex.what();
    }
}

void coalesceMulti(uv_work_t* req) {
    CoalesceBaton *baton = static_cast<CoalesceBaton *>(req->data);

    try {
        std::vector<PhrasematchSubq> &stack = baton->stack;
        std::sort(stack.begin(), stack.end(), subqSortByZoom);
        std::size_t stackSize = stack.size();

        // Cache zoom levels to iterate over as coalesce occurs.
        std::vector<Cache::intarray> zoomCache;
        zoomCache.reserve(stackSize);
        for (auto const& subq : stack) {
            Cache::intarray zooms;
            std::vector<bool> zoomUniq(22, false);
            for (auto const& subqB : stack) {
                if (subq.idx == subqB.idx) continue;
                if (zoomUniq[subqB.zoom]) continue;
                if (subq.zoom < subqB.zoom) continue;
                zoomUniq[subqB.zoom] = true;
                zooms.emplace_back(subqB.zoom);
            }
            zoomCache.push_back(std::move(zooms));
        }

        // Coalesce relevs into higher zooms, e.g.
        // z5 inherits relev of overlapping tiles at z4.
        // @TODO assumes sources are in zoom ascending order.
        std::string type = "grid";
        std::map<uint64_t,std::vector<Context>> coalesced;
        std::map<uint64_t,std::vector<Context>>::iterator cit;
        std::map<uint64_t,std::vector<Context>>::iterator pit;
        std::map<uint64_t,bool> done;
        std::map<uint64_t,bool>::iterator dit;

        // proximity (optional)
        bool proximity = baton->centerzxy.size() > 0;
        unsigned cz;
        double cx;
        double cy;
        if (proximity) {
            cz = baton->centerzxy[0];
            cx = baton->centerzxy[1];
            cy = baton->centerzxy[2];
        } else {
            cz = 0;
            cx = 0;
            cy = 0;
        }

        // bbox (optional)
        bool bbox = !baton->bboxzxy.empty();
        unsigned bboxz;
        unsigned minx;
        unsigned miny;
        unsigned maxx;
        unsigned maxy;
        if (bbox) {
            bboxz = baton->bboxzxy[0];
            minx = baton->bboxzxy[1];
            miny = baton->bboxzxy[2];
            maxx = baton->bboxzxy[3];
            maxy = baton->bboxzxy[4];
        } else {
            bboxz = 0;
            minx = 0;
            miny = 0;
            maxx = 0;
            maxy = 0;
        }

        std::vector<Context> contexts;
        std::size_t i = 0;
        for (auto const& subq : stack) {
            std::string shardId = shard(4, subq.phrase);

            Cache::intarray grids = __getTruncated(subq.cache, type, shardId, subq.phrase, 500000);

            bool first = i == 0;
            bool last = i == (stack.size() - 1);
            unsigned short z = subq.zoom;
            auto const& zCache = zoomCache[i];
            std::size_t zCacheSize = zCache.size();

            unsigned long m = grids.size();

            for (unsigned long j = 0; j < m; j++) {
                Cover cover = numToCover(grids[j]);
                cover.idx = subq.idx;
                cover.mask = subq.mask;
                cover.tmpid = static_cast<uint32_t>(cover.idx * POW2_25 + cover.id);
                cover.relev = cover.relev * subq.weight;
                if (proximity) {
                    ZXY dxy = pxy2zxy(z, cover.x, cover.y, cz);
                    cover.distance = tileDist(cx, cy, dxy.x, dxy.y);
                    cover.scoredist = scoredist(cz, cover.distance, cover.score);
                } else {
                    cover.distance = 0;
                    cover.scoredist = cover.score;
                }

                if (bbox) {
                    ZXY min = bxy2zxy(bboxz, minx, miny, z, false);
                    ZXY max = bxy2zxy(bboxz, maxx, maxy, z, true);
                    if (cover.x < min.x || cover.y < min.y || cover.x > max.x || cover.y > max.y) continue;
                }

                uint64_t zxy = (z * POW2_28) + (cover.x * POW2_14) + (cover.y);

                // Reserve stackSize for the coverList. The vector
                // will grow no larger that the size of the input
                // subqueries that are being coalesced.
                std::vector<Cover> covers;
                covers.reserve(stackSize);
                covers.push_back(cover);
                uint32_t context_mask = cover.mask;
                double context_relev = cover.relev;

                for (unsigned a = 0; a < zCacheSize; a++) {
                    uint64_t p = zCache[a];
                    double s = static_cast<double>(1 << (z-p));
                    uint64_t pxy = static_cast<uint64_t>(p * POW2_28) +
                        static_cast<uint64_t>(std::floor(cover.x/s) * POW2_14) +
                        static_cast<uint64_t>(std::floor(cover.y/s));
                    pit = coalesced.find(pxy);
                    if (pit != coalesced.end()) {
                        uint32_t lastMask = 0;
                        double lastRelev = 0.0;
                        for (auto const& parents : pit->second) {
                            for (auto const& parent : parents.coverList) {
                                // this cover is functionally identical with previous and
                                // is more relevant, replace the previous.
                                if (parent.mask == lastMask && parent.relev > lastRelev) {
                                    covers.pop_back();
                                    covers.emplace_back(parent);
                                    context_relev -= lastRelev;
                                    context_relev += parent.relev;
                                    lastMask = parent.mask;
                                    lastRelev = parent.relev;
                                // this cover doesn't overlap with used mask.
                                } else if (!(context_mask & parent.mask)) {
                                    covers.emplace_back(parent);
                                    context_relev += parent.relev;
                                    context_mask = context_mask | parent.mask;
                                    lastMask = parent.mask;
                                    lastRelev = parent.relev;
                                }
                            }
                        }
                    }
                }

                if (last) {
                    // Slightly penalize contexts that have no stacking
                    if (covers.size() == 1) {
                        context_relev -= 0.01;
                    // Slightly penalize contexts in ascending order
                    } else if (covers[0].mask > covers[1].mask) {
                        context_relev -= 0.01;
                    }
                    contexts.emplace_back(std::move(covers),context_mask,context_relev);
                } else if (first || covers.size() > 1) {
                    cit = coalesced.find(zxy);
                    if (cit == coalesced.end()) {
                        std::vector<Context> local_contexts;
                        local_contexts.emplace_back(std::move(covers),context_mask,context_relev);
                        coalesced.emplace(zxy, std::move(local_contexts));
                    } else {
                        cit->second.emplace_back(std::move(covers),context_mask,context_relev);
                    }
                }
            }

            i++;
        }

        // append coalesced to contexts by moving memory
        for (auto && matched : coalesced) {
            for (auto &&context : matched.second) {
                contexts.emplace_back(std::move(context));
            }
        }

        std::sort(contexts.begin(), contexts.end(), contextSortByRelev);
        coalesceFinalize(baton, std::move(contexts));
    } catch (std::exception const& ex) {
       baton->error = ex.what();
    }
}

Local<Object> coverToObject(Cover const& cover) {
    Local<Object> object = Nan::New<Object>();
    object->Set(Nan::New("x").ToLocalChecked(), Nan::New<Number>(cover.x));
    object->Set(Nan::New("y").ToLocalChecked(), Nan::New<Number>(cover.y));
    object->Set(Nan::New("relev").ToLocalChecked(), Nan::New<Number>(cover.relev));
    object->Set(Nan::New("score").ToLocalChecked(), Nan::New<Number>(cover.score));
    object->Set(Nan::New("id").ToLocalChecked(), Nan::New<Number>(cover.id));
    object->Set(Nan::New("idx").ToLocalChecked(), Nan::New<Number>(cover.idx));
    object->Set(Nan::New("tmpid").ToLocalChecked(), Nan::New<Number>(cover.tmpid));
    object->Set(Nan::New("distance").ToLocalChecked(), Nan::New<Number>(cover.distance));
    object->Set(Nan::New("scoredist").ToLocalChecked(), Nan::New<Number>(cover.scoredist));
    return object;
}
Local<Array> contextToArray(Context const& context) {
    std::size_t size = context.coverList.size();
    Local<Array> array = Nan::New<Array>(static_cast<int>(size));
    for (uint32_t i = 0; i < size; i++) {
        array->Set(i, coverToObject(context.coverList[i]));
    }
    array->Set(Nan::New("relev").ToLocalChecked(), Nan::New(context.relev));
    return array;
}
void coalesceAfter(uv_work_t* req) {
    Nan::HandleScope scope;
    CoalesceBaton *baton = static_cast<CoalesceBaton *>(req->data);

    for (auto & phrase_match : baton->stack) {
        phrase_match.cache->_unref();
    }

    if (!baton->error.empty()) {
        v8::Local<v8::Value> argv[1] = { Nan::Error(baton->error.c_str()) };
        Nan::MakeCallback(Nan::GetCurrentContext()->Global(), Nan::New(baton->callback), 1, argv);
    }
    else {
        std::vector<Context> const& features = baton->features;

        Local<Array> jsFeatures = Nan::New<Array>(static_cast<int>(features.size()));
        for (std::size_t i = 0; i < features.size(); i++) {
            jsFeatures->Set(i, contextToArray(features[i]));
        }

        Local<Value> argv[2] = { Nan::Null(), jsFeatures };
        Nan::MakeCallback(Nan::GetCurrentContext()->Global(), Nan::New(baton->callback), 2, argv);
    }

    baton->callback.Reset();
    delete baton;
}
NAN_METHOD(Cache::coalesce) {
    // PhrasematchStack (js => cpp)
    if (!info[0]->IsArray()) {
        return Nan::ThrowTypeError("Arg 1 must be a PhrasematchSubq array");
    }
    CoalesceBaton *baton = new CoalesceBaton();

    try {
        std::vector<PhrasematchSubq> stack;
        const Local<Array> array = Local<Array>::Cast(info[0]);
        for (uint32_t i = 0; i < array->Length(); i++) {
            Local<Value> val = array->Get(i);
            if (!val->IsObject()) {
                delete baton;
                return Nan::ThrowTypeError("All items in array must be valid PhrasematchSubq objects");
            }
            Local<Object> jsStack = val->ToObject();
            if (jsStack->IsNull() || jsStack->IsUndefined()) {
                delete baton;
                return Nan::ThrowTypeError("All items in array must be valid PhrasematchSubq objects");
            }
            PhrasematchSubq subq;

            int64_t _idx = jsStack->Get(Nan::New("idx").ToLocalChecked())->IntegerValue();
            if (_idx < 0 || _idx > std::numeric_limits<unsigned short>::max()) {
                delete baton;
                return Nan::ThrowTypeError("encountered idx value too large to fit in unsigned short");
            }
            subq.idx = static_cast<unsigned short>(_idx);

            int64_t _zoom = jsStack->Get(Nan::New("zoom").ToLocalChecked())->IntegerValue();
            if (_zoom < 0 || _zoom > std::numeric_limits<unsigned short>::max()) {
                delete baton;
                return Nan::ThrowTypeError("encountered zoom value too large to fit in unsigned short");
            }
            subq.zoom = static_cast<unsigned short>(_zoom);

            subq.weight = jsStack->Get(Nan::New("weight").ToLocalChecked())->NumberValue();
            subq.phrase = jsStack->Get(Nan::New("phrase").ToLocalChecked())->IntegerValue();
            subq.mask = static_cast<std::uint32_t>(jsStack->Get(Nan::New("mask").ToLocalChecked())->IntegerValue());

            // JS cache reference => cpp
            Local<Object> cache = Local<Object>::Cast(jsStack->Get(Nan::New("cache").ToLocalChecked()));
            Cache * cache_ptr = node::ObjectWrap::Unwrap<Cache>(cache);
            cache_ptr->_ref();
            subq.cache = cache_ptr;
            stack.push_back(subq);
        }
        baton->stack = stack;

        // Options object (js => cpp)
        if (!info[1]->IsObject()) {
            delete baton;
            return Nan::ThrowTypeError("Arg 2 must be an options object");
        }
        const Local<Object> options = Local<Object>::Cast(info[1]);
        if (options->Has(Nan::New("centerzxy").ToLocalChecked())) {
            baton->centerzxy = arrayToDoubleVector(Local<Array>::Cast(options->Get(Nan::New("centerzxy").ToLocalChecked())));
        }

        if (options->Has(Nan::New("bboxzxy").ToLocalChecked())) {
            baton->bboxzxy = arrayToVector(Local<Array>::Cast(options->Get(Nan::New("bboxzxy").ToLocalChecked())));
        }

        // callback
        if (!info[2]->IsFunction()) {
            delete baton;
            return Nan::ThrowTypeError("Arg 3 must be a callback function");
        }
        Local<Value> callback = info[2];
        baton->callback.Reset(callback.As<Function>());

        // queue work
        baton->request.data = baton;
        // optimization: for stacks of 1, use coalesceSingle
        if (stack.size() == 1) {
            uv_queue_work(uv_default_loop(), &baton->request, coalesceSingle, (uv_after_work_cb)coalesceAfter);
        } else {
            uv_queue_work(uv_default_loop(), &baton->request, coalesceMulti, (uv_after_work_cb)coalesceAfter);
        }
    } catch (std::exception const& ex) {
        delete baton;
        return Nan::ThrowTypeError(ex.what());
    }

    info.GetReturnValue().Set(Nan::Undefined());
    return;
}

extern "C" {
    static void start(Handle<Object> target) {
        Cache::Initialize(target);
    }
}

} // namespace carmen


NODE_MODULE(carmen, carmen::start)
