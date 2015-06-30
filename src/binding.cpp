
#include "binding.hpp"
#include <node_version.h>
#include <node_buffer.h>

#include "pbf.hpp"

#include <sstream>
#include <cmath>

#pragma GCC diagnostic push
#pragma GCC diagnostic warning "-Wpadded"

namespace carmen {

using namespace v8;

Persistent<FunctionTemplate> Cache::constructor;

std::string shard(uint64_t level, uint64_t id) {
    if (level == 0) return "0";
    unsigned int bits = 32 - (level * 4);
    unsigned int shard_id = std::floor(id / std::pow(2, bits));
    return std::to_string(shard_id);
}

Cache::intarray arrayToVector(Local<Array> const& array) {
    Cache::intarray vector;
    vector.reserve(array->Length());
    for (uint64_t i = 0; i < array->Length(); i++) {
        uint64_t num = array->Get(i)->NumberValue();
        vector.emplace_back(num);
    }
    return vector;
}

Local<Array> vectorToArray(Cache::intarray const& vector) {
    std::size_t size = vector.size();
    Local<Array> array = NanNew<Array>(static_cast<int>(size));
    for (uint64_t i = 0; i < size; i++) {
        array->Set(i, NanNew<Number>(vector[i]));
    }
    return array;
}

Local<Object> mapToObject(std::map<std::uint64_t,std::uint64_t> const& map) {
    Local<Object> object = NanNew<Object>();
    for (auto const& item : map) {
        object->Set(NanNew<Number>(item.first), NanNew<Number>(item.second));
    }
    return object;
}

std::map<std::uint64_t,std::uint64_t> objectToMap(Local<Object> const& object) {
    std::map<std::uint64_t,std::uint64_t> map;
    const Local<Array> keys = object->GetPropertyNames();
    const uint32_t length = keys->Length();
    for (uint32_t i = 0; i < length; i++) {
        uint64_t key = keys->Get(i)->NumberValue();
        uint64_t value = object->Get(key)->NumberValue();
        map.emplace(key, value);
    }
    return map;
}

Cache::intarray __get(Cache const* c, std::string const& type, std::string const& shard, uint64_t id) {
    std::string key = type + "-" + shard;
    Cache::memcache const& mem = c->cache_;
    Cache::memcache::const_iterator itr = mem.find(key);
    Cache::intarray array;
    if (itr == mem.end()) {
        Cache::lazycache const& lazy = c->lazy_;
        Cache::lazycache::const_iterator litr = lazy.find(key);
        if (litr == lazy.end()) {
            return array;
        }
        Cache::message_cache const& messages = c->msg_;
        Cache::message_cache::const_iterator mitr = messages.find(key);
        if (mitr == messages.end()) {
            throw std::runtime_error("misuse");
        }
        Cache::larraycache::const_iterator laitr = litr->second.find(id);
        if (laitr == litr->second.end()) {
            return array;
        } else {
            // NOTE: we cannot call array.reserve here since
            // the total length is not known
            unsigned start = (laitr->second & 0xffffffff);
            unsigned len = (laitr->second >> 32);
            std::string ref = mitr->second.substr(start,len);
            protobuf::message buffer(ref.data(), ref.size());
            while (buffer.next()) {
                if (buffer.tag == 1) {
                    buffer.skip();
                } else if (buffer.tag == 2) {
                    uint64_t array_length = buffer.varint();
                    protobuf::message pbfarray(buffer.getData(),static_cast<std::size_t>(array_length));
                    while (pbfarray.next()) {
                        array.emplace_back(pbfarray.value);
                    }
                    buffer.skipBytes(array_length);
                } else {
                    std::stringstream msg("");
                    msg << "cxx get: hit unknown protobuf type: '" << buffer.tag << "'";
                    throw std::runtime_error(msg.str());
                }
            }
            return array;
        }
    } else {
        Cache::arraycache::const_iterator aitr = itr->second.find(id);
        if (aitr == itr->second.end()) {
            return array;
        } else {
            return aitr->second;
        }
    }
}

bool __exists(Cache const* c, std::string const& type, std::string const& shard, uint64_t id) {
    std::string key = type + "-" + shard;
    Cache::memcache const& mem = c->cache_;
    Cache::memcache::const_iterator itr = mem.find(key);
    Cache::intarray array;
    if (itr == mem.end()) {
        Cache::lazycache const& lazy = c->lazy_;
        Cache::lazycache::const_iterator litr = lazy.find(key);
        if (litr == lazy.end()) {
            return false;
        }
        Cache::message_cache const& messages = c->msg_;
        Cache::message_cache::const_iterator mitr = messages.find(key);
        if (mitr == messages.end()) {
            throw std::runtime_error("misuse");
        }
        Cache::larraycache::const_iterator laitr = litr->second.find(id);
        if (laitr == litr->second.end()) {
            return false;
        }
        // NOTE: we cannot call array.reserve here since
        // the total length is not known
        unsigned start = (laitr->second & 0xffffffff);
        unsigned len = (laitr->second >> 32);
        std::string ref = mitr->second.substr(start,len);
        protobuf::message buffer(ref.data(), ref.size());
        while (buffer.next()) {
            if (buffer.tag == 1) {
                buffer.skip();
            } else if (buffer.tag == 2) {
                return true;
            } else {
                std::stringstream msg("");
                msg << "cxx get: hit unknown protobuf type: '" << buffer.tag << "'";
                throw std::runtime_error(msg.str());
            }
        }
        return false;
    } else {
        Cache::arraycache::const_iterator aitr = itr->second.find(id);
        return aitr != itr->second.end();
    }
}

void Cache::Initialize(Handle<Object> target) {
    NanScope();
    Local<FunctionTemplate> t = NanNew<FunctionTemplate>(Cache::New);
    t->InstanceTemplate()->SetInternalFieldCount(1);
    t->SetClassName(NanNew("Cache"));
    NODE_SET_PROTOTYPE_METHOD(t, "has", has);
    NODE_SET_PROTOTYPE_METHOD(t, "load", load);
    NODE_SET_PROTOTYPE_METHOD(t, "loadSync", loadSync);
    NODE_SET_PROTOTYPE_METHOD(t, "pack", pack);
    NODE_SET_PROTOTYPE_METHOD(t, "list", list);
    NODE_SET_PROTOTYPE_METHOD(t, "_set", _set);
    NODE_SET_PROTOTYPE_METHOD(t, "_get", _get);
    NODE_SET_PROTOTYPE_METHOD(t, "_exists", _exists);
    NODE_SET_PROTOTYPE_METHOD(t, "unload", unload);
    NODE_SET_METHOD(t, "coalesceZooms", coalesceZooms);
    NODE_SET_METHOD(t, "spatialMatch", spatialMatch);
    NODE_SET_METHOD(t, "coalesce", coalesce);
    target->Set(NanNew("Cache"),t->GetFunction());
    NanAssignPersistent(constructor, t);
}

Cache::Cache(std::string const& id, unsigned shardlevel)
  : ObjectWrap(),
    id_(id),
    shardlevel_(shardlevel),
    cache_(),
    lazy_(),
    msg_()
    { }

Cache::~Cache() { }

NAN_METHOD(Cache::pack)
{
    NanScope();
    if (args.Length() < 1) {
        return NanThrowTypeError("expected two args: 'type', 'shard'");
    }
    if (!args[0]->IsString()) {
        return NanThrowTypeError("first argument must be a String");
    }
    if (!args[1]->IsNumber()) {
        return NanThrowTypeError("second arg must be an Integer");
    }
    try {
        std::string type = *String::Utf8Value(args[0]->ToString());
        std::string shard = *String::Utf8Value(args[1]->ToString());
        std::string key = type + "-" + shard;
        Cache* c = node::ObjectWrap::Unwrap<Cache>(args.This());
        Cache::memcache const& mem = c->cache_;
        Cache::memcache::const_iterator itr = mem.find(key);
        carmen::proto::object message;
        if (itr != mem.end()) {
            for (auto const& item : itr->second) {
                ::carmen::proto::object_item * new_item = message.add_items(); 
                new_item->set_key(item.first);
                Cache::intarray const & varr = item.second;
                for (auto const& vitem : varr) {
                    new_item->add_val(static_cast<int64_t>(vitem));
                }
            }
        } else {
            Cache::lazycache const& lazy = c->lazy_;
            Cache::message_cache const& messages = c->msg_;
            Cache::lazycache::const_iterator litr = lazy.find(key);
            if (litr != lazy.end()) {
                Cache::message_cache::const_iterator mitr = messages.find(key);
                if (mitr == messages.end()) {
                    throw std::runtime_error("misuse");
                }
                for (auto const& item : litr->second) {
                    ::carmen::proto::object_item * new_item = message.add_items();
                    new_item->set_key(static_cast<int64_t>(item.first));
                    unsigned start = (item.second & 0xffffffff);
                    unsigned len = (item.second >> 32);
                    std::string ref = mitr->second.substr(start,len);
                    protobuf::message buffer(ref.data(), ref.size());
                    while (buffer.next()) {
                        if (buffer.tag == 1) {
                            buffer.skip();
                        } else if (buffer.tag == 2) {
                            uint64_t array_length = buffer.varint();
                            protobuf::message pbfarray(buffer.getData(),static_cast<std::size_t>(array_length));
                            while (pbfarray.next()) {
                                new_item->add_val(static_cast<int64_t>(pbfarray.value));
                            }
                            buffer.skipBytes(array_length);
                        } else {
                            std::stringstream msg("");
                            msg << "pack: hit unknown protobuf type: '" << buffer.tag << "'";
                            throw std::runtime_error(msg.str());
                        }
                    }
                }
            } else {
                return NanThrowTypeError("pack: cannot pack empty data");
            }
        }
        int size = message.ByteSize();
        if (size > 0)
        {
            std::size_t usize = static_cast<std::size_t>(size);
            Local<Object> buf = NanNewBufferHandle(usize);
            if (message.SerializeToArray(node::Buffer::Data(buf),size))
            {
                NanReturnValue(buf);
            }
        } else {
            return NanThrowTypeError("pack: invalid message ByteSize encountered");
        }
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
    NanReturnUndefined();
}

NAN_METHOD(Cache::list)
{
    NanScope();
    if (args.Length() < 1) {
        return NanThrowTypeError("expected at least one arg: 'type' and optional a 'shard'");
    }
    if (!args[0]->IsString()) {
        return NanThrowTypeError("first argument must be a String");
    }
    try {
        std::string type = *String::Utf8Value(args[0]->ToString());
        Cache* c = node::ObjectWrap::Unwrap<Cache>(args.This());
        Cache::memcache const& mem = c->cache_;
        Cache::lazycache const& lazy = c->lazy_;
        Local<Array> ids = NanNew<Array>();
        if (args.Length() == 1) {
            unsigned idx = 0;
            std::size_t type_size = type.size();
            for (auto const& item : mem) {
                std::size_t item_size = item.first.size();
                if (item_size > type_size && item.first.substr(0,type_size) == type) {
                    std::string shard = item.first.substr(type_size+1,item_size);
                    ids->Set(idx++,NanNew(NanNew(shard.c_str())->NumberValue()));
                }
            }
            for (auto const& item : lazy) {
                std::size_t item_size = item.first.size();
                if (item_size > type_size && item.first.substr(0,type_size) == type) {
                    std::string shard = item.first.substr(type_size+1,item_size);
                    ids->Set(idx++,NanNew(NanNew(shard.c_str())->NumberValue()));
                }
            }
            NanReturnValue(ids);
        } else if (args.Length() == 2) {
            std::string shard = *String::Utf8Value(args[1]->ToString());
            std::string key = type + "-" + shard;
            Cache::memcache::const_iterator itr = mem.find(key);
            unsigned idx = 0;
            if (itr != mem.end()) {
                for (auto const& item : itr->second) {
                    ids->Set(idx++,NanNew(item.first)->ToString());
                }
            }
            Cache::lazycache::const_iterator litr = lazy.find(key);
            if (litr != lazy.end()) {
                for (auto const& item : litr->second) {
                    ids->Set(idx++,NanNew<Number>(item.first)->ToString());
                }
            }
            NanReturnValue(ids);
        }
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
    NanReturnUndefined();
}

NAN_METHOD(Cache::_set)
{
    NanScope();
    if (args.Length() < 3) {
        return NanThrowTypeError("expected four args: 'type', 'shard', 'id', 'data'");
    }
    if (!args[0]->IsString()) {
        return NanThrowTypeError("first argument must be a String");
    }
    if (!args[1]->IsNumber()) {
        return NanThrowTypeError("second arg must be an Integer");
    }
    if (!args[2]->IsNumber()) {
        return NanThrowTypeError("third arg must be an Integer");
    }
    if (!args[3]->IsArray()) {
        return NanThrowTypeError("fourth arg must be an Array");
    }
    Local<Array> data = Local<Array>::Cast(args[3]);
    if (data->IsNull() || data->IsUndefined()) {
        return NanThrowTypeError("an array expected for fourth argument");
    }
    try {
        std::string type = *String::Utf8Value(args[0]->ToString());
        std::string shard = *String::Utf8Value(args[1]->ToString());
        std::string key = type + "-" + shard;
        Cache* c = node::ObjectWrap::Unwrap<Cache>(args.This());
        Cache::memcache & mem = c->cache_;
        Cache::memcache::const_iterator itr = mem.find(key);
        if (itr == mem.end()) {
            c->cache_.emplace(key,Cache::arraycache());
        }
        Cache::arraycache & arrc = c->cache_[key];
        Cache::arraycache::key_type key_id = static_cast<Cache::arraycache::key_type>(args[2]->IntegerValue());
        Cache::arraycache::iterator itr2 = arrc.find(key_id);
        if (itr2 == arrc.end()) {
            arrc.emplace(key_id,Cache::intarray());
        }
        Cache::intarray & vv = arrc[key_id];

        unsigned array_size = data->Length();
        if (args[4]->IsBoolean() && args[4]->BooleanValue()) {
            vv.reserve(vv.size() + array_size);
        } else {
            if (itr2 != arrc.end()) vv.clear();
            vv.reserve(array_size);
        }

        for (unsigned i=0;i<array_size;++i) {
            vv.emplace_back(data->Get(i)->NumberValue());
        }
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
    NanReturnUndefined();
}

void load_into_cache(Cache::larraycache & larrc,
                            const char * data,
                            size_t size) {
    protobuf::message message(data,size);
    while (message.next()) {
        if (message.tag == 1) {
            uint64_t len = message.varint();
            protobuf::message buffer(message.getData(), static_cast<std::size_t>(len));
            while (buffer.next()) {
                if (buffer.tag == 1) {
                    uint64_t key_id = buffer.varint();
                    size_t start = static_cast<size_t>(message.getData() - data);
                    // insert here because:
                    //  - libstdc++ does not support std::map::emplace <-- does now with gcc 4.8, but...
                    //  - we are using google::sparshash now, which does not support emplace (yet?)
                    //  - larrc.emplace(buffer.varint(),Cache::string_ref_type(message.getData(),len)) was not faster on OS X
                    Cache::offset_type offsets = (((Cache::offset_type)len << 32)) | (((Cache::offset_type)start) & 0xffffffff);
                    larrc.insert(std::make_pair(key_id,offsets));
                }
                // it is safe to break immediately because tag 1 should come first
                // it would also be safe to not use `while (buffer.next())` here, but we do it
                // because I've not seen a performance cost (dane/osx) and being explicit is good
                break;
            }
            message.skipBytes(len);
        } else {
            std::stringstream msg("");
            msg << "load: hit unknown protobuf type: '" << message.tag << "'";
            throw std::runtime_error(msg.str());
        }
    }
}

NAN_METHOD(Cache::loadSync)
{
    NanScope();
    if (args.Length() < 2) {
        return NanThrowTypeError("expected at three args: 'buffer', 'type', and 'shard'");
    }
    if (!args[0]->IsObject()) {
        return NanThrowTypeError("first argument must be a Buffer");
    }
    Local<Object> obj = args[0]->ToObject();
    if (obj->IsNull() || obj->IsUndefined()) {
        return NanThrowTypeError("a buffer expected for first argument");
    }
    if (!node::Buffer::HasInstance(obj)) {
        return NanThrowTypeError("first argument must be a Buffer");
    }
    if (!args[1]->IsString()) {
        return NanThrowTypeError("second arg 'type' must be a String");
    }
    if (!args[2]->IsNumber()) {
        return NanThrowTypeError("third arg 'shard' must be an Integer");
    }
    try {
        std::string type = *String::Utf8Value(args[1]->ToString());
        std::string shard = *String::Utf8Value(args[2]->ToString());
        std::string key = type + "-" + shard;
        Cache* c = node::ObjectWrap::Unwrap<Cache>(args.This());
        Cache::memcache & mem = c->cache_;
        Cache::memcache::iterator itr = mem.find(key);
        if (itr != mem.end()) {
            c->cache_.emplace(key,arraycache());
        }
        Cache::memcache::iterator itr2 = mem.find(key);
        if (itr2 != mem.end()) {
            mem.erase(itr2);
        }
        Cache::lazycache & lazy = c->lazy_;
        Cache::lazycache::iterator litr = lazy.find(key);
        Cache::message_cache & messages = c->msg_;
        if (litr == lazy.end()) {
            c->lazy_.emplace(key,Cache::larraycache());
            messages.emplace(key,std::string(node::Buffer::Data(obj),node::Buffer::Length(obj)));
        }
        load_into_cache(c->lazy_[key],node::Buffer::Data(obj),node::Buffer::Length(obj));
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
    NanReturnUndefined();
}

struct load_baton : carmen::noncopyable {
    uv_work_t request;
    Cache * c;
    NanCallback cb;
    Cache::larraycache arrc;
    std::string key;
    std::string data;
    std::string error_name;
    load_baton(std::string const& _key,
               const char * _data,
               size_t size,
               Local<Function> callbackHandle,
               Cache * _c) :
      c(_c),
      cb(callbackHandle),
      arrc(),
      key(_key),
      data(_data,size),
      error_name() {
        request.data = this;
        c->_ref();
      }
    ~load_baton() {
         c->_unref();
    }
};

void Cache::AsyncLoad(uv_work_t* req) {
    load_baton *closure = static_cast<load_baton *>(req->data);
    try {
        load_into_cache(closure->arrc,closure->data.data(),closure->data.size());
    }
    catch (std::exception const& ex)
    {
        closure->error_name = ex.what();
    }
}

void Cache::AfterLoad(uv_work_t* req) {
    NanScope();
    load_baton *closure = static_cast<load_baton *>(req->data);
    TryCatch try_catch;
    if (!closure->error_name.empty()) {
        Local<Value> argv[1] = { Exception::Error(NanNew(closure->error_name.c_str())) };
        closure->cb.Call(1, argv);
    } else {
        Cache::memcache::iterator itr2 = closure->c->cache_.find(closure->key);
        if (itr2 != closure->c->cache_.end()) {
            closure->c->cache_.erase(itr2);
        }
        closure->c->lazy_[closure->key] = std::move(closure->arrc);
        Local<Value> argv[1] = { NanNull() };
        closure->cb.Call(1, argv);
    }
    if (try_catch.HasCaught())
    {
        node::FatalException(try_catch);
    }
    delete closure;
}

NAN_METHOD(Cache::load)
{
    NanScope();
    Local<Value> callback = args[args.Length()-1];
    if (!callback->IsFunction()) {
        return loadSync(args);
    }
    if (args.Length() < 2) {
        return NanThrowTypeError("expected at least three args: 'buffer', 'type', 'shard', and optionally a 'callback'");
    }
    if (!args[0]->IsObject()) {
        return NanThrowTypeError("first argument must be a Buffer");
    }
    Local<Object> obj = args[0]->ToObject();
    if (obj->IsNull() || obj->IsUndefined()) {
        return NanThrowTypeError("a buffer expected for first argument");
    }
    if (!node::Buffer::HasInstance(obj)) {
        return NanThrowTypeError("first argument must be a Buffer");
    }
    if (!args[1]->IsString()) {
        return NanThrowTypeError("second arg 'type' must be a String");
    }
    if (!args[2]->IsNumber()) {
        return NanThrowTypeError("third arg 'shard' must be an Integer");
    }
    try {
        std::string type = *String::Utf8Value(args[1]->ToString());
        std::string shard = *String::Utf8Value(args[2]->ToString());
        std::string key = type + "-" + shard;
        Cache* c = node::ObjectWrap::Unwrap<Cache>(args.This());
        load_baton *closure = new load_baton(key,
                                             node::Buffer::Data(obj),
                                             node::Buffer::Length(obj),
                                             args[3].As<Function>(),
                                             c);
        uv_queue_work(uv_default_loop(), &closure->request, AsyncLoad, (uv_after_work_cb)AfterLoad);
        Cache::message_cache & messages = c->msg_;
        if (messages.find(key) == messages.end()) {
            messages.emplace(key,std::string(node::Buffer::Data(obj),node::Buffer::Length(obj)));
        }
        NanReturnUndefined();
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
}

NAN_METHOD(Cache::has)
{
    NanScope();
    if (args.Length() < 2) {
        return NanThrowTypeError("expected two args: 'type' and 'shard'");
    }
    if (!args[0]->IsString()) {
        return NanThrowTypeError("first arg must be a String");
    }
    if (!args[1]->IsNumber()) {
        return NanThrowTypeError("second arg must be an Integer");
    }
    try {
        std::string type = *String::Utf8Value(args[0]->ToString());
        std::string shard = *String::Utf8Value(args[1]->ToString());
        std::string key = type + "-" + shard;
        Cache* c = node::ObjectWrap::Unwrap<Cache>(args.This());
        Cache::memcache const& mem = c->cache_;
        Cache::memcache::const_iterator itr = mem.find(key);
        if (itr != mem.end()) {
            NanReturnValue(NanTrue());
        } else {
            Cache::message_cache const& messages = c->msg_;
            if (messages.find(key) != messages.end()) {
                NanReturnValue(NanTrue());
            }
            NanReturnValue(NanFalse());
        }
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
}

NAN_METHOD(Cache::_get)
{
    NanScope();
    if (args.Length() < 3) {
        return NanThrowTypeError("expected three args: type, shard, and id");
    }
    if (!args[0]->IsString()) {
        return NanThrowTypeError("first arg must be a String");
    }
    if (!args[1]->IsNumber()) {
        return NanThrowTypeError("second arg must be an Integer");
    }
    if (!args[2]->IsNumber()) {
        return NanThrowTypeError("third arg must be an Integer");
    }
    try {
        std::string type = *String::Utf8Value(args[0]->ToString());
        std::string shard = *String::Utf8Value(args[1]->ToString());
        uint64_t id = static_cast<uint64_t>(args[2]->IntegerValue());
        Cache* c = node::ObjectWrap::Unwrap<Cache>(args.This());
        Cache::intarray vector = __get(c, type, shard, id);
        if (!vector.empty()) {
            NanReturnValue(vectorToArray(vector));
        } else {
            NanReturnUndefined();
        }
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
}

NAN_METHOD(Cache::_exists)
{
    NanScope();
    if (args.Length() < 3) {
        return NanThrowTypeError("expected three args: type, shard, and id");
    }
    if (!args[0]->IsString()) {
        return NanThrowTypeError("first arg must be a String");
    }
    if (!args[1]->IsNumber()) {
        return NanThrowTypeError("second arg must be an Integer");
    }
    if (!args[2]->IsNumber()) {
        return NanThrowTypeError("third arg must be an Integer");
    }
    try {
        std::string type = *String::Utf8Value(args[0]->ToString());
        std::string shard = *String::Utf8Value(args[1]->ToString());
        uint64_t id = static_cast<uint64_t>(args[2]->IntegerValue());
        Cache* c = node::ObjectWrap::Unwrap<Cache>(args.This());
        bool exists = __exists(c, type, shard, id);
        NanReturnValue(NanNew<Boolean>(exists));
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
}

NAN_METHOD(Cache::unload)
{
    NanScope();
    if (args.Length() < 2) {
        return NanThrowTypeError("expected at least two args: 'type' and 'shard'");
    }
    if (!args[0]->IsString()) {
        return NanThrowTypeError("first arg must be a String");
    }
    if (!args[1]->IsNumber()) {
        return NanThrowTypeError("second arg must be an Integer");
    }
    bool hit = false;
    try {
        std::string type = *String::Utf8Value(args[0]->ToString());
        std::string shard = *String::Utf8Value(args[1]->ToString());
        std::string key = type + "-" + shard;
        Cache* c = node::ObjectWrap::Unwrap<Cache>(args.This());
        Cache::memcache & mem = c->cache_;
        Cache::memcache::iterator itr = mem.find(key);
        if (itr != mem.end()) {
            hit = true;
            mem.erase(itr);
        }
        Cache::lazycache & lazy = c->lazy_;
        Cache::lazycache::iterator litr = lazy.find(key);
        if (litr != lazy.end()) {
            hit = true;
            lazy.erase(litr);
        }
        Cache::message_cache & messages = c->msg_;
        Cache::message_cache::iterator mitr = messages.find(key);
        if (mitr != messages.end()) {
            hit = true;
            messages.erase(mitr);
        }
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
    NanReturnValue(NanNew<Boolean>(hit));
}

NAN_METHOD(Cache::New)
{
    NanScope();
    if (!args.IsConstructCall()) {
        return NanThrowTypeError("Cannot call constructor as function, you need to use 'new' keyword");
    }
    try {
        if (args.Length() < 2) {
            return NanThrowTypeError("expected 'id' and 'shardlevel' arguments");
        }
        if (!args[0]->IsString()) {
            return NanThrowTypeError("first argument 'id' must be a String");
        }
        if (!args[1]->IsNumber()) {
            return NanThrowTypeError("second argument 'shardlevel' must be a number");
        }
        std::string id = *String::Utf8Value(args[0]->ToString());
        unsigned shardlevel = static_cast<unsigned>(args[1]->IntegerValue());
        Cache* im = new Cache(id,shardlevel);
        im->Wrap(args.This());
        args.This()->Set(NanNew("id"),args[0]);
        args.This()->Set(NanNew("shardlevel"),args[1]);
        NanReturnValue(args.This());
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
    NanReturnUndefined();
}

//relev = 5 bits
//count = 3 bits
//reason = 12 bits
//* 1 bit gap
//id = 32 bits
const uint64_t POW2_52 = std::pow(2,52);
const uint64_t POW2_48 = std::pow(2,48);
const uint64_t POW2_45 = std::pow(2,45);
const uint64_t POW2_33 = std::pow(2,33);
const uint64_t POW2_32 = std::pow(2,32);
const uint64_t POW2_28 = std::pow(2,28);
const uint64_t POW2_25 = std::pow(2,25);
const uint64_t POW2_12 = std::pow(2,12);
const uint64_t POW2_8 = std::pow(2,8);
const uint64_t POW2_5 = std::pow(2,5);
const uint64_t POW2_4 = std::pow(2,4);
const uint64_t POW2_3 = std::pow(2,3);

class CoalesceZooms : carmen::noncopyable {
public:
    CoalesceZooms() :
     coalesced(),
     keys() {}
    std::map<uint64_t,Cache::intarray> coalesced;
    std::map<uint64_t,std::string> keys;
};

void _coalesceZooms(CoalesceZooms & ret, std::vector<Cache::intarray> & grids, Cache::intarray const& zooms) {
    // Filter zooms down to those with matches.
    Cache::intarray matchedZooms;
    std::size_t zooms_size = zooms.size();
    matchedZooms.reserve(zooms_size);
    for (unsigned short i = 0; i < zooms_size; i++) {
        if (!grids[i].empty()) {
            matchedZooms.emplace_back(zooms[i]);
        }
    }
    std::sort(matchedZooms.begin(), matchedZooms.end());
    matchedZooms.erase(std::unique(matchedZooms.begin(), matchedZooms.end()), matchedZooms.end());

    // Cache zoom levels to iterate over as coalesce occurs.
    std::vector<Cache::intarray> zoomcache(22);
    std::size_t matched_zoom_size = matchedZooms.size();
    for (unsigned short i = 0; i < matched_zoom_size; i++) {
        Cache::intarray sliced;
        sliced.reserve(i);
        for (unsigned short j = 0; j < i; j++) {
            sliced.emplace_back(matchedZooms[j]);
        }
        std::reverse(sliced.begin(), sliced.end());
        zoomcache[matchedZooms[i]] = sliced;
    }

    uint64_t xd = std::pow(2, 39);
    uint64_t yd = std::pow(2, 25);
    uint64_t mp2_14 = std::pow(2, 14);
    uint64_t mp2_28 = std::pow(2, 28);

    std::map<uint64_t,Cache::intarray> coalesced;
    std::map<uint64_t,std::string> keys;
    std::map<uint64_t,bool> done;

    std::map<uint64_t,Cache::intarray>::iterator cit;
    std::map<uint64_t,std::string>::iterator kit;
    std::map<uint64_t,bool>::iterator dit;
    std::map<uint64_t,Cache::intarray>::iterator parent_cit;
    std::map<uint64_t,std::string>::iterator parent_kit;

    std::size_t grids_size = grids.size();
    for (unsigned h = 0; h < grids_size; h++) {
        Cache::intarray const& grid = grids[h];
        uint64_t z = zooms[h];
        auto const& zoom_cache = zoomcache[z];
        std::size_t zoom_cache_size = zoom_cache.size();
        std::size_t grid_size = grid.size();
        for (unsigned i = 0; i < grid_size; i++) {
            uint32_t tmpid = (h * POW2_25) + (grid[i] % yd);
            uint64_t x = std::floor(grid[i]/xd);
            uint64_t y = std::floor(grid[i]%xd/yd);
            uint64_t zxy = (z * mp2_28) + (x * mp2_14) + y;

            cit = coalesced.find(zxy);
            if (cit == coalesced.end()) {
                Cache::intarray zxylist;
                zxylist.push_back(tmpid);
                coalesced.emplace(zxy, zxylist);
                keys.emplace(zxy, std::to_string(tmpid));
            } else {
                cit->second.push_back(tmpid);
                kit = keys.find(zxy);
                kit->second.append("-");
                kit->second.append(std::to_string(tmpid));
            }

            dit = done.find(zxy);
            if (dit == done.end()) {
                // for each parent zoom collect ids
                for (unsigned a = 0; a < zoom_cache_size; a++) {
                    unsigned p = zoom_cache[a];
                    unsigned s = 1 << (z-p);
                    uint64_t pxy = (p * mp2_28) + (std::floor(x/s) * mp2_14) + std::floor(y/s);
                    // Set a flag to ensure coalesce occurs only once per zxy.
                    parent_cit = coalesced.find(pxy);
                    if (parent_cit != coalesced.end()) {
                        cit = coalesced.find(zxy);
                        for (auto const& parent_array : parent_cit->second)
                        {
                            cit->second.emplace_back(parent_array);
                        }
                        parent_kit = keys.find(pxy);
                        kit = keys.find(zxy);
                        kit->second.append("-");
                        kit->second.append(parent_kit->second);
                        done.emplace(zxy, true);
                        break;
                    }
                }
            }
        }
    }
    ret.coalesced = std::move(coalesced);
    ret.keys = keys;
}

NAN_METHOD(Cache::coalesceZooms) {
    NanScope();
    if (!args[0]->IsArray()) {
        return NanThrowTypeError("first arg must be an array of grid cover arrays");
    }
    if (!args[1]->IsArray()) {
        return NanThrowTypeError("second arg must be an array of zoom integers");
    }

    Local<Array> gridsArray = Local<Array>::Cast(args[0]);
    std::vector<Cache::intarray> grids;
    grids.reserve(gridsArray->Length());
    for (uint64_t i = 0; i < gridsArray->Length(); i++) {
        Cache::intarray grid = arrayToVector(Local<Array>::Cast(gridsArray->Get(i)));
        grids.emplace_back(grid);
    }

    Cache::intarray zooms = arrayToVector(Local<Array>::Cast(args[1]));

    CoalesceZooms ret;
    _coalesceZooms(ret, grids, zooms);

    std::map<uint64_t,std::string>::iterator kit;

    Local<Object> object = NanNew<Object>();
    for (auto const& item : ret.coalesced) {
        kit = ret.keys.find(item.first);
        Local<Array> array = vectorToArray(item.second);
        array->Set(NanNew("key"), NanNew(kit->second));
        object->Set(NanNew<Number>(item.first), array);
    }

    NanReturnValue(object);
}

class SetRelev : carmen::noncopyable {
public:
    double relev;
    uint64_t id;
    uint32_t tmpid;
    unsigned short count;
    unsigned short reason;
    unsigned short idx;
    bool check;
    SetRelev(double _relev,
             uint64_t _id,
             uint32_t _tmpid,
             unsigned short _count,
             unsigned short _reason,
             unsigned short _idx,
             bool _check) :
      relev(_relev),
      id(_id),
      tmpid(_tmpid),
      count(_count),
      reason(_reason),
      idx(_idx),
      check(_check) {}

    SetRelev(SetRelev && rhs) noexcept :
      relev(std::move(rhs.relev)),
      id(std::move(rhs.id)),
      tmpid(std::move(rhs.tmpid)),
      count(std::move(rhs.count)),
      reason(std::move(rhs.reason)),
      idx(std::move(rhs.idx)),
      check(std::move(rhs.check)) {}

    SetRelev& operator=(SetRelev && rhs) // move assign
    {
        std::swap(relev, rhs.relev);
        std::swap(id, rhs.id);
        std::swap(tmpid, rhs.tmpid);
        std::swap(count, rhs.count);
        std::swap(reason, rhs.reason);
        std::swap(idx, rhs.idx);
        std::swap(check, rhs.check);
        return *this;
    }

};

uint64_t setRelevToNumber(SetRelev const& setRelev) {
    unsigned short relev = std::floor(setRelev.relev * (POW2_5-1));

    // count + reason are values that may overflow the currently tight
    // bit constraints given. For now clip to 8/12 bits respectively --
    // requires a better solution long term.
    unsigned short count = setRelev.count < POW2_3 ? setRelev.count : POW2_3-1;
    unsigned short reason = setRelev.reason < POW2_12 ? setRelev.reason : POW2_12-1;

    if (relev >= POW2_5) throw std::runtime_error("misuse: setRelev.relev > 5 bits");
    if (setRelev.idx >= POW2_8)  throw std::runtime_error("misuse: setRelev.idx > 8 bits");
    if (setRelev.id >= POW2_25)  throw std::runtime_error("misuse: setRelev.id > 25 bits");
    uint64_t num =
        (relev * POW2_48) +
        (count * POW2_45) +
        (reason * POW2_33) +
        (setRelev.idx * POW2_25) +
        (setRelev.id);
    return num;
}

inline SetRelev numberToSetRelev(uint64_t num) {
    uint64_t id = num % POW2_25;
    unsigned short idx = (num >> 25) % POW2_8;
    uint32_t tmpid = (idx * POW2_25) + id;
    unsigned short reason = (num >> 33) % POW2_12;
    unsigned short count = (num >> 45) % POW2_3;
    double relev = ((num >> 48) % POW2_5) / ((double)POW2_5-1);
    return SetRelev(relev,id,tmpid,count,reason,idx,true);
}

double _setRelevance(unsigned short queryLength, std::vector<SetRelev> & sets, std::vector<unsigned short> & groups) {
    double total = queryLength;
    double max_relevance = 0;
    unsigned short max_checkmask = 0;
    std::size_t sets_size = sets.size();

    // For a given set: a, b, c, d, iterate over:
    // a, b, c, d
    //    b, c, d
    //       c, d
    //          d
    for (unsigned short a = 0; a < sets_size; a++) {
        double relevance = 0;
        double gappy = 0;
        double stacky = 0;
        unsigned short checkmask = 0;
        unsigned short querymask = 0;
        unsigned short tally = 0;
        signed short lastgroup = -1;
        signed short lastreason = -1;
        signed short laststart = 0;

        // Mark parts of the set that will not be considered due
        // to `a` offset in the checkmask.
        for (unsigned short b = 0; b < a; b++) {
            checkmask += 1<<b;
        }

        // For each set, score its correspondence with the query
        for (unsigned short i = a; i < sets_size; i++) {
            auto & set = sets[i];

            // Each db may contribute a distinct matching reason to the final
            // relev. If this entry is for a db that has already contributed
            // but without the same reason mark it as false.
            if (lastgroup == groups[set.idx]) {
                if (lastreason != set.reason) {
                    checkmask += 1<<i;
                }
                continue;
            }

            bool backy = false;
            unsigned short usage = 0;
            unsigned short count = set.count;

            for (unsigned j = 0; j < queryLength; j++) {
                if (
                    // make sure this term has not already been counted for
                    // relevance. Uses a bitmask to mark positions counted.
                    ((querymask & (1<<j)) == 0) &&
                    // if this term matches the reason bitmask for relevance
                    ((set.reason & (1<<j)) != 0)
                ) {
                    ++usage;
                    ++tally;

                    // store the position of the first bit of the querymask
                    // for the current matching term. The next matching term
                    // may match parts prior to this in the query, but adds
                    // to the backy penalty.
                    //
                    // stack: a b c
                    // query: a b c === relev 1
                    //
                    // stack: a b c
                    // query: c a b === relev 0.99
                    if (j < laststart) backy = true;
                    laststart = j;

                    // 'check off' this term of the query so that it isn't
                    // double-counted against a different `sets` reason.
                    querymask += 1<<j;
                    // once a set's term count has been exhausted short circuit.
                    // this prevents a province match from grabbing both instances
                    // of 'new' and 'york' in a 'new york new york' query.
                    if (!--count) break;
                }
            }

            // If this relevant criteria matched any terms in the query,
            // increment the total relevance score.
            if (usage > 0 && count == 0) {
                if (backy) {
                    relevance += (set.relev * (usage / total) * 0.5);
                } else {
                    relevance += (set.relev * (usage / total));
                }
                if (lastgroup > -1) stacky = 1;
                if (lastgroup >= 0) gappy += (std::abs(groups[set.idx] - lastgroup) - 1);
                lastgroup = groups[set.idx];
                lastreason = set.reason;
            } else if (lastreason != set.reason) {
                checkmask += 1<<i;
            }
        }

        // Bonus when multiple features have stacked: +0.01
        relevance -= 0.01;
        relevance += 0.01 * stacky;
        // Penalize stacking bonus slightly based on whether stacking matches
        // contained "gaps" in continuity between index levels.
        relevance -= 0.001 * gappy;
        relevance = relevance > 0 ? relevance : 0;

        if (relevance > max_relevance) {
            max_relevance = relevance;
            max_checkmask = checkmask;
        }
    }

    for (unsigned short i = 0; i < sets_size; i++) {
        if (max_checkmask & (1<<i)) {
            auto & set = sets[i];
            set.check = false;
        }
    }

    return max_relevance;
}

struct SpatialMatchBaton : carmen::noncopyable {
    uv_work_t request;
    // params
    v8::Persistent<v8::Function> callback;
    std::map<uint64_t,uint64_t> featnums;
    std::vector<Cache::intarray> grids;
    std::vector<unsigned short> groups;
    Cache::intarray zooms;
    // return
    std::map<uint64_t,uint64_t> sets;
    std::vector<uint64_t> results;
    std::map<uint64_t,Cache::intarray> coalesced;
    unsigned short queryLength;
};

bool sortRelevReason(SetRelev const& a, SetRelev const& b) {
    if (a.idx > b.idx) return true;
    else if (a.idx < b.idx) return false;
    else if (a.relev > b.relev) return true;
    else if (a.relev < b.relev) return false;
    else if (a.reason > b.reason) return true;
    else if (a.reason < b.reason) return false;
    return a.id < b.id;
}
bool sortByRelev(SetRelev const& a, SetRelev const& b) {
    if (a.relev > b.relev) return true;
    if (a.relev < b.relev) return false;
    if (a.idx < b.idx) return true;
    if (a.idx > b.idx) return false;
    return a.id < b.id;
}
void _spatialMatch(uv_work_t* req) {
    SpatialMatchBaton *baton = static_cast<SpatialMatchBaton *>(req->data);

    // convert featnums back to SetRelev
    std::map<uint64_t,uint64_t> const& featnums = baton->featnums;
    std::map<uint64_t,uint64_t>::const_iterator fnit;
    std::map<uint64_t,SetRelev> features;
    std::map<uint64_t,SetRelev>::iterator fit;
    for (auto const& item : featnums) {
        features.emplace(item.first, numberToSetRelev(item.second));
    }

    unsigned short queryLength = baton->queryLength;
    std::vector<Cache::intarray> & grids = baton->grids;
    Cache::intarray const& zooms = baton->zooms;
    std::vector<unsigned short> & groups = baton->groups;

    CoalesceZooms ret;
    _coalesceZooms(ret, grids, zooms);

    std::map<uint64_t,Cache::intarray> const& coalesced = ret.coalesced;
    std::map<uint64_t,Cache::intarray>::const_iterator cit;
    std::map<uint64_t,std::string> const& keys = ret.keys;
    std::map<uint64_t,std::string>::const_iterator kit;
    std::map<std::string,bool> done;
    std::map<std::string,bool>::iterator dit;

    std::map<uint64_t,uint64_t> sets;
    std::map<uint64_t,uint64_t>::iterator sit;
    std::map<uint64_t,SetRelev> rowMemo;
    std::map<uint64_t,SetRelev>::iterator rit;

    for (auto const& item : coalesced) {
        kit = keys.find(item.first);
        std::string key = kit->second;
        dit = done.find(key);
        if (dit != done.end()) {
            continue;
        } else {
            done.emplace(key, true);
        }

        std::vector<SetRelev> rows;
        std::size_t coalesced_size = item.second.size();
        rows.reserve(coalesced_size);
        for (unsigned short i = 0; i < coalesced_size; i++) {
            fit = features.find(item.second[i]);
            if (fit->second.check == false) {
                continue;
            } else {
                rows.emplace_back(std::move(fit->second));
            }
        }
        std::sort(rows.begin(), rows.end(), sortRelevReason);
        double relev = _setRelevance(queryLength, rows, groups);
        std::size_t rows_size = rows.size();

        signed int lastgroup = -1;

        for (unsigned short i = 0; i < rows_size; i++) {
            auto & row = rows[i];
            // Add setRelev to sets.
            sit = sets.find(row.tmpid);
            if (sit == sets.end()) {
                sets.emplace(row.tmpid, setRelevToNumber(row));
            }

            // Push results from the top index group.
            if (lastgroup == -1 || lastgroup == groups[row.idx]) {
                // Clone setRelev from row.
                SetRelev setRelev = std::move(row);
                setRelev.relev = relev;

                lastgroup = groups[setRelev.idx];
                rit = rowMemo.find(setRelev.tmpid);
                if (rit != rowMemo.end()) {
                    if (rit->second.relev > relev) {
                        continue;
                    }
                    rit->second = std::move(setRelev);
                } else {
                    rowMemo.emplace(setRelev.tmpid, std::move(setRelev));
                }
            }
        }
    }

    std::vector<SetRelev> sorted;
    for (auto & row : rowMemo) {
        sorted.emplace_back(std::move(row.second));
    }
    std::sort(sorted.begin(), sorted.end(), sortByRelev);

    double lastRelev = 0;
    std::vector<uint64_t> results;
    for (auto const& s : sorted) {
        if (lastRelev == 0 || lastRelev - s.relev < 0.25) {
            lastRelev = s.relev;
            results.emplace_back(setRelevToNumber(s));
        }
    }

    baton->sets = std::move(sets);
    baton->results = std::move(results);
    baton->coalesced = std::move(ret.coalesced);
}
void spatialMatchAfter(uv_work_t* req) {
    NanScope();
    SpatialMatchBaton *baton = static_cast<SpatialMatchBaton *>(req->data);
    std::map<uint64_t,uint64_t> const& sets = baton->sets;
    std::vector<uint64_t> const& results = baton->results;
    std::map<uint64_t,Cache::intarray> const& coalesced = baton->coalesced;

    Local<Object> ret = NanNew<Object>();

    // sets to relevnum
    Local<Object> setsObject = NanNew<Object>();
    std::map<uint64_t,uint64_t>::iterator sit;
    for (auto const& set : sets) {
        setsObject->Set(NanNew<Number>(set.first), NanNew<Number>(set.second));
    }
    ret->Set(NanNew("sets"), setsObject);

    // results to array
    std::size_t size = results.size();
    Local<Array> resultsArray = NanNew<Array>(static_cast<int>(size));
    for (uint64_t i = 0; i < size; i++) {
        resultsArray->Set(i, NanNew<Number>(results[i]));
    }
    ret->Set(NanNew("results"), resultsArray);

    // coalesced to object
    Local<Object> coalescedObject = NanNew<Object>();
    for (auto const& item : coalesced) {
        Local<Array> array = vectorToArray(item.second);
        coalescedObject->Set(NanNew<Number>(item.first), array);
    }
    ret->Set(NanNew("coalesced"), coalescedObject);

    Local<Value> argv[2] = { NanNull(), ret };
    NanMakeCallback(NanGetCurrentContext()->Global(), NanNew(baton->callback), 2, argv);
    NanDisposePersistent(baton->callback);
    delete baton;
}
NAN_METHOD(Cache::spatialMatch) {
    NanScope();
    if (!args[0]->IsNumber()) {
        return NanThrowTypeError("first arg must be a queryLength number");
    }
    if (!args[1]->IsObject()) {
        return NanThrowTypeError("second arg must be an object with feature numbers");
    }
    if (!args[2]->IsArray()) {
        return NanThrowTypeError("third arg must be an array of grid cover arrays");
    }
    if (!args[3]->IsArray()) {
        return NanThrowTypeError("fourth arg must be an array of zoom integers");
    }
    if (!args[4]->IsArray()) {
        return NanThrowTypeError("fifth arg must be an array of group integers");
    }
    if (!args[5]->IsFunction()) {
        return NanThrowTypeError("sixth arg must be a callback function");
    }

    SpatialMatchBaton *baton = new SpatialMatchBaton();

    // queryLength
    unsigned short queryLength = args[0]->NumberValue();

    // featnums
    Local<Object> object = Local<Object>::Cast(args[1]);
    const Local<Array> keys = object->GetPropertyNames();
    const uint32_t length = keys->Length();
    for (uint32_t i = 0; i < length; i++) {
        uint64_t key = keys->Get(i)->NumberValue();
        uint64_t featnum = object->Get(key)->NumberValue();
        baton->featnums.emplace(key, featnum);
    }

    // grids
    Local<Array> array = Local<Array>::Cast(args[2]);
    baton->grids.reserve(array->Length());
    for (uint64_t i = 0; i < array->Length(); i++) {
        Cache::intarray grid = arrayToVector(Local<Array>::Cast(array->Get(i)));
        baton->grids.push_back(std::move(grid));
    }

    // zooms
    baton->zooms = arrayToVector(Local<Array>::Cast(args[3]));

    // groups
    Local<Array> groupsArray = Local<Array>::Cast(args[4]);
    baton->groups.reserve(groupsArray->Length());
    for (unsigned short i = 0; i < groupsArray->Length(); i++) {
        unsigned short num = groupsArray->Get(i)->NumberValue();
        baton->groups.emplace_back(num);
    }

    // callback
    Local<Value> callback = args[5];
    baton->queryLength = queryLength;
    baton->request.data = baton;
    NanAssignPersistent(baton->callback, callback.As<Function>());
    uv_queue_work(uv_default_loop(), &baton->request, _spatialMatch, (uv_after_work_cb)spatialMatchAfter);
    NanReturnUndefined();
}

struct PhrasematchSubq {
    carmen::Cache *cache;
    uint64_t phrase;
    unsigned short idx;
    unsigned short zoom;
    double weight;
};

struct Cover {
    uint32_t x;
    uint32_t y;
    double relev;
    unsigned short score;
    uint32_t id;
    uint32_t tmpid;
    unsigned short idx;
};

struct CoalesceBaton : carmen::noncopyable {
    uv_work_t request;
    // params
    std::map<uint64_t,carmen::Cache> caches;
    std::vector<PhrasematchSubq> stack;
    Cache::intarray centerzxy;
    v8::Persistent<v8::Function> callback;
    // return
    std::map<uint64_t,Cover> sets;
    std::vector<Cover> features;
};

void coalesceUV(uv_work_t* req) {
    CoalesceBaton *baton = static_cast<CoalesceBaton *>(req->data);
    std::vector<PhrasematchSubq> stack = std::move(baton->stack);

    // Cache zoom levels to iterate over as coalesce occurs.
    Cache::intarray zoom;
    std::vector<bool> zoomUniq;
    std::vector<Cache::intarray> zoomCache(22);
    std::size_t l;
    l = stack.size();
    for (unsigned short i = 0; i < l; i++) {
        if (zoomUniq[stack[l].zoom]) continue;
        zoomUniq[stack[l].zoom] = true;
        zoom.emplace_back(stack[l].zoom);
    }
    l = zoom.size();
    for (unsigned short i = 0; i < l; i++) {
        Cache::intarray sliced;
        sliced.reserve(i);
        for (unsigned short j = 0; j < i; j++) {
            sliced.emplace_back(zoom[j]);
        }
        std::reverse(sliced.begin(), sliced.end());
        zoomCache[zoom[i]] = sliced;
    }
}
void coalesceAfter(uv_work_t* req) {
    NanScope();
    CoalesceBaton *baton = static_cast<CoalesceBaton *>(req->data);
    std::map<uint64_t,Cover> const& sets = baton->sets;
    std::vector<Cover> const& features = baton->features;

    // sets to JS obj
    // features to JS array
    // Local<Value> argv[2] = { NanNull(), ret };
    Local<Object> ret = NanNew<Object>();

    Local<Value> argv[2] = { NanNull(), ret };
    NanMakeCallback(NanGetCurrentContext()->Global(), NanNew(baton->callback), 2, argv);
    NanDisposePersistent(baton->callback);
    delete baton;
}
NAN_METHOD(Cache::coalesce) {
    NanScope();

    // PhrasematchStack (js => cpp)
    if (!args[0]->IsArray()) {
        return NanThrowTypeError("Arg 1 must be a PhrasematchSubq array");
    }
    CoalesceBaton *baton = new CoalesceBaton();
    std::vector<PhrasematchSubq> stack;
    const Local<Array> array = Local<Array>::Cast(args[0]);
    for (uint64_t i = 0; i < array->Length(); i++) {
        Local<Object> jsStack = Local<Object>::Cast(array->Get(i));
        PhrasematchSubq subq;
        subq.idx = jsStack->Get(NanNew("idx"))->NumberValue();
        subq.zoom = jsStack->Get(NanNew("zoom"))->NumberValue();
        subq.weight = jsStack->Get(NanNew("weight"))->NumberValue();
        subq.phrase = jsStack->Get(NanNew("phrase"))->NumberValue();

        // JS cache reference => cpp
        Local<Object> cache = Local<Object>::Cast(jsStack->Get(NanNew("cache")));
        subq.cache = node::ObjectWrap::Unwrap<Cache>(cache);

        stack.push_back(subq);
    }

    // Options object (js => cpp)
    if (!args[1]->IsObject()) {
        return NanThrowTypeError("Arg 2 must be an options object");
    }
    const Local<Object> options = Local<Object>::Cast(args[1]);
    if (options->Has(NanNew("centerzxy"))) {
        baton->centerzxy = arrayToVector(Local<Array>::Cast(options->Get(NanNew("centerzxy"))));
    }

    // callback
    if (!args[2]->IsFunction()) {
        return NanThrowTypeError("Arg 3 must be a callback function");
    }
    Local<Value> callback = args[2];
    NanAssignPersistent(baton->callback, callback.As<Function>());

    // queue work
    baton->request.data = baton;
    uv_queue_work(uv_default_loop(), &baton->request, coalesceUV, (uv_after_work_cb)coalesceAfter);
    NanReturnUndefined();
}

extern "C" {
    static void start(Handle<Object> target) {
        Cache::Initialize(target);
    }
}

} // namespace carmen


#pragma GCC diagnostic pop

NODE_MODULE(carmen, carmen::start)
