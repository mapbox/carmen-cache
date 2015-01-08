
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
    NODE_SET_PROTOTYPE_METHOD(t, "unload", unload);
    NODE_SET_PROTOTYPE_METHOD(t, "phrasematchDegens", phrasematchDegens);
    NODE_SET_PROTOTYPE_METHOD(t, "phrasematchPhraseRelev", phrasematchPhraseRelev);
    NODE_SET_METHOD(t, "coalesceZooms", coalesceZooms);
    NODE_SET_METHOD(t, "setRelevance", setRelevance);
    NODE_SET_METHOD(t, "spatialMatch", spatialMatch);
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
#if NODE_VERSION_AT_LEAST(0, 11, 0)
            Local<Object> retbuf = node::Buffer::New(usize);
            if (message.SerializeToArray(node::Buffer::Data(retbuf),size))
            {
                NanReturnValue(retbuf);
            }
#else
            node::Buffer *retbuf = node::Buffer::New(usize);
            if (message.SerializeToArray(node::Buffer::Data(retbuf),size))
            {
                NanReturnValue(retbuf->handle_);
            }
#endif
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
        if (itr2 != arrc.end()) {
            vv.clear();
        }
        unsigned array_size = data->Length();
        vv.reserve(array_size);
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

// cache.phrasematchDegens(termidx, degens)
struct phrasematchDegensBaton : carmen::noncopyable {
    v8::Persistent<v8::Function> callback;
    uv_work_t request;
    std::vector<std::vector<std::uint64_t>> results;
    std::vector<std::uint64_t> terms;
    std::map<std::uint64_t,std::uint64_t> queryidx;
    std::map<std::uint64_t,std::uint64_t> querymask;
    std::map<std::uint64_t,std::uint64_t> querydist;
};

bool sortDegens(uint64_t a, uint64_t b) {
    uint32_t ad = a % 16;
    uint32_t bd = b % 16;
    if (ad < bd) { return true; }
    if (ad > bd) { return false; }
    return a < b;
}
// In the threadpool (no V8)
void _phrasematchDegens(uv_work_t* req) {
    phrasematchDegensBaton *baton = static_cast<phrasematchDegensBaton *>(req->data);

    std::vector<std::uint64_t> terms;
    std::map<std::uint64_t,std::uint64_t> queryidx;
    std::map<std::uint64_t,std::uint64_t> querymask;
    std::map<std::uint64_t,std::uint64_t> querydist;
    std::size_t results_size = baton->results.size();
    for (uint64_t idx = 0; idx < results_size; idx++) {
        std::vector<std::uint64_t> & degens = baton->results[idx];
        std::sort(degens.begin(), degens.end(), sortDegens);
        std::size_t degens_size = degens.size();
        for (std::size_t i = 0; i < degens_size && i < 10; i++) {
            uint64_t term = degens[i] >> 4 << 4;
            terms.emplace_back(term);

            std::map<std::uint64_t,std::uint64_t>::iterator it;

            it = queryidx.find(term);
            if (it == queryidx.end()) {
                queryidx.emplace(term, idx);
            }

            it = querymask.find(term);
            if (it == querymask.end()) {
                querymask.emplace(term, 0 + (1<<idx));
            } else {
                it->second = it->second + (1<<idx);
            }

            it = querydist.find(term);
            if (it == querydist.end()) {
                querydist.emplace(term, term % 16);
            }
        }
    }

    baton->terms = std::move(terms);
    baton->queryidx = std::move(queryidx);
    baton->querymask = std::move(querymask);
    baton->querydist = std::move(querydist);
}
void phrasematchDegensAfter(uv_work_t* req) {
    NanScope();
    phrasematchDegensBaton *baton = static_cast<phrasematchDegensBaton *>(req->data);

    Local<Object> ret = NanNew<Object>();
    ret->Set(NanNew("terms"), vectorToArray(baton->terms));
    ret->Set(NanNew("queryidx"), mapToObject(baton->queryidx));
    ret->Set(NanNew("querymask"), mapToObject(baton->querymask));
    ret->Set(NanNew("querydist"), mapToObject(baton->querydist));

    Local<Value> argv[2] = { NanNull(), ret };
    NanMakeCallback(NanGetCurrentContext()->Global(), NanNew(baton->callback), 2, argv);
    NanDisposePersistent(baton->callback);
    delete baton;
}
// In main event loop (with V8)
NAN_METHOD(Cache::phrasematchDegens)
{
    NanScope();
    if (!args[0]->IsArray()) {
        return NanThrowTypeError("first arg must be a results array");
    }
    if (!args[1]->IsFunction()) {
        return NanThrowTypeError("second arg must be a callback");
    }

    phrasematchDegensBaton *baton = new phrasematchDegensBaton();
 
    // convert v8 results array into nested std::map
    Local<Array> resultsArray = Local<Array>::Cast(args[0]);
    baton->results.reserve(resultsArray->Length());
    for (uint64_t i = 0; i < resultsArray->Length(); i++) {
        std::vector<std::uint64_t> degens = arrayToVector(Local<Array>::Cast(resultsArray->Get(i)));
        baton->results.push_back(std::move(degens));
    }

    // callback
    Local<Value> callback = args[1];
    baton->request.data = baton;
    NanAssignPersistent(baton->callback, callback.As<Function>());
    uv_queue_work(uv_default_loop(), &baton->request, _phrasematchDegens, (uv_after_work_cb)phrasematchDegensAfter);
    NanReturnUndefined();
}

class PhraseRelev : carmen::noncopyable{
public:
    double relev;
    unsigned short count;
    unsigned short reason;
    uint64_t id;
    PhraseRelev(uint64_t _id,
                unsigned short _count,
                double _relev,
                unsigned short _reason) noexcept :
      relev(_relev),
      count(_count),
      reason(_reason),
      id(_id) {}

    PhraseRelev(PhraseRelev && rhs) noexcept :
      relev(std::move(rhs.relev)),
      count(std::move(rhs.count)),
      reason(std::move(rhs.reason)),
      id(std::move(rhs.id)) {}

    PhraseRelev& operator=(PhraseRelev && rhs) // move assign
    {
        std::swap(relev, rhs.relev);
        std::swap(count, rhs.count);
        std::swap(reason, rhs.reason);
        std::swap(id, rhs.id);
        return *this;
    }
};

//relev = 5 bits
//count = 3 bits
//reason = 12 bits
//* 1 bit gap
//id = 32 bits
const uint64_t POW2_48 = std::pow(2,48);
const uint64_t POW2_45 = std::pow(2,45);
const uint64_t POW2_33 = std::pow(2,33);
const uint64_t POW2_32 = std::pow(2,32);
const uint64_t POW2_25 = std::pow(2,25);
const uint64_t POW2_12 = std::pow(2,12);
const uint64_t POW2_8 = std::pow(2,8);
const uint64_t POW2_5 = std::pow(2,5);
const uint64_t POW2_3 = std::pow(2,3);
uint64_t phraseRelevToNumber(PhraseRelev const& pr) {
    uint64_t num;
    unsigned short relev;
    relev = std::floor(pr.relev * (POW2_5-1));

    if (relev >= POW2_5) throw std::runtime_error("misuse: pr.relev > 5 bits");
    if (pr.count >= POW2_3)  throw std::runtime_error("misuse: pr.count > 3 bits");
    if (pr.reason >= POW2_12)  throw std::runtime_error("misuse: pr.reason > 12 bits");
    if (pr.id >= POW2_32)  throw std::runtime_error("misuse: pr.id > 32 bits");
    num =
        (relev * POW2_48) +
        (pr.count * POW2_45) +
        (pr.reason * POW2_33) +
        (pr.id);
    return num;
}

inline PhraseRelev numberToPhraseRelev(uint64_t num) {
    uint64_t id = num % POW2_32;
    unsigned short reason = (num >> 33) % POW2_12;
    unsigned short count = (num >> 45) % POW2_3;
    double relev = ((num >> 48) % POW2_5) / ((double)POW2_5-1);
    return PhraseRelev(id,count,relev,reason);
}

struct phrasematchPhraseRelevBaton : carmen::noncopyable {
    v8::Persistent<v8::Function> callback;
    uv_work_t request;
    Cache* cache;
    uint64_t shardlevel;
    std::string error;
    std::vector<PhraseRelev> relevantPhrases;
    std::vector<std::uint64_t> phrases;
    std::map<std::uint64_t,std::uint64_t> queryidx;
    std::map<std::uint64_t,std::uint64_t> querymask;
    std::map<std::uint64_t,std::uint64_t> querydist;
};

bool sortPhraseRelev(PhraseRelev const& a, PhraseRelev const& b) {
    return a.id < b.id;
}
bool uniqPhraseRelev(PhraseRelev const& a, PhraseRelev const& b) {
    return a.id == b.id;
}
void _phrasematchPhraseRelev(uv_work_t* req) {
    phrasematchPhraseRelevBaton *baton = static_cast<phrasematchPhraseRelevBaton *>(req->data);

    std::string type = "phrase";
    double max_relev = 0;
    double min_relev = 1;
    std::vector<PhraseRelev> allPhrases;
    std::vector<PhraseRelev> relevantPhrases;
    std::size_t phrases_size = baton->phrases.size();
    for (uint64_t a = 0; a < phrases_size; a++) {
        uint64_t id = baton->phrases[a];
        Cache::intarray phrase = __get(baton->cache, "phrase", shard(baton->shardlevel, id), id);
        if (phrase.empty()) {
            baton->error = "Failed to get phrase";
            return;
        }

        unsigned short size = phrase.size();

        // Get total relev score of phrase.
        unsigned short total = 0;
        for (unsigned short i = 0; i < size; i++) {
            total += phrase[i] % 16;
        }

        double relev = 0;
        unsigned short count = 0;
        unsigned short reason = 0;
        unsigned short chardist = 0;
        signed short lastidx = -1;

        // relev each feature:
        // - across all feature synonyms, find the max relev of the sum
        //   of each synonym's terms based on each term's frequency of
        //   occurrence in the dataset.
        // - for the max relev also store the 'reason' -- the index of
        //   each query token that contributed to its relev.
        for (unsigned short i = 0; i < size; i++) {
            uint64_t term = phrase[i] >> 4 << 4;

            std::map<std::uint64_t,std::uint64_t>::iterator it;
            it = baton->querymask.find(term);

            // Short circuit
            if (it == baton->querymask.end()) {
                if (relev != 0) {
                    break;
                } else {
                    continue;
                }
            }

            it = baton->queryidx.find(term);
            unsigned short termidx = it->second;
            it = baton->querymask.find(term);
            unsigned short termmask = it->second;
            it = baton->querydist.find(term);
            unsigned short termdist = it->second;
            if (relev == 0 || termidx == lastidx + 1) {
                relev += phrase[i] % 16;
                reason = reason | termmask;
                chardist += termdist;
                lastidx = termidx;
                count++;
            }
        }

        // get relev back to float-land.
        relev = relev / total;
        relev = (relev > 0.99 ? 1 : relev) - (chardist * 0.01);

        if (relev > max_relev) {
            max_relev = relev;
        }
        if (relev < min_relev) {
            min_relev = relev;
        }

        // relev represents a score based on comparative term weight
        // significance alone. If it passes this threshold check it is
        // adjusted based on degenerate term character distance (e.g.
        // degens of higher distance reduce relev score).
        // printf( "%f \n", relev);
        if (relev >= 0.5) {
            allPhrases.emplace_back(id,count,relev,reason);
            if (relev > 0.75) {
                relevantPhrases.emplace_back(id,count,relev,reason);
            }
        }
    }

    // Reduces the relevance bar to 0.50 since all results have identical relevance values
    if (min_relev == max_relev) {
        relevantPhrases = std::move(allPhrases);
    }

    std::sort(relevantPhrases.begin(), relevantPhrases.end(), sortPhraseRelev);
    relevantPhrases.erase(std::unique(relevantPhrases.begin(), relevantPhrases.end(), uniqPhraseRelev), relevantPhrases.end());
    baton->relevantPhrases = std::move(relevantPhrases);
}
void phrasematchPhraseRelevAfter(uv_work_t* req) {
    NanScope();
    phrasematchPhraseRelevBaton *baton = static_cast<phrasematchPhraseRelevBaton *>(req->data);

    if (!baton->error.empty()) {
        Local<Value> argv[1] = { NanError(baton->error.c_str()) };
        NanMakeCallback(NanGetCurrentContext()->Global(), NanNew(baton->callback), 1, argv);
    } else {
        unsigned short relevsize = baton->relevantPhrases.size();
        Local<Array> result = NanNew<Array>(static_cast<int>(relevsize));
        Local<Object> relevs = NanNew<Object>();
        for (uint32_t i = 0; i < relevsize; i++) {
            auto const& phrase = baton->relevantPhrases[i];
            result->Set(i, NanNew<Number>(phrase.id));
            relevs->Set(NanNew<Number>(phrase.id), NanNew<Number>(phraseRelevToNumber(phrase)));
        }
        Local<Object> ret = NanNew<Object>();
        ret->Set(NanNew("result"), result);
        ret->Set(NanNew("relevs"), relevs);
        Local<Value> argv[2] = { NanNull(), ret };
        NanMakeCallback(NanGetCurrentContext()->Global(), NanNew(baton->callback), 2, argv);
    }
    NanDisposePersistent(baton->callback);
    delete baton;
}
// In main event loop (with V8)
NAN_METHOD(Cache::phrasematchPhraseRelev)
{
    NanScope();
    if (!args[0]->IsArray()) {
        return NanThrowTypeError("first arg must be a results array");
    }
    if (!args[1]->IsObject()) {
        return NanThrowTypeError("second arg must be a queryidx object");
    }
    if (!args[2]->IsObject()) {
        return NanThrowTypeError("third arg must be a querymask object");
    }
    if (!args[3]->IsObject()) {
        return NanThrowTypeError("fourth arg must be a querydist object");
    }
    if (!args[4]->IsFunction()) {
        return NanThrowTypeError("fifth arg must be a callback");
    }

    uint64_t shardlevel = args.This()->Get(NanNew("shardlevel"))->NumberValue();
    Cache::intarray phrases = arrayToVector(Local<Array>::Cast(args[0]));
    std::map<std::uint64_t,std::uint64_t> queryidx = objectToMap(Local<Object>::Cast(args[1]));
    std::map<std::uint64_t,std::uint64_t> querymask = objectToMap(Local<Object>::Cast(args[2]));
    std::map<std::uint64_t,std::uint64_t> querydist = objectToMap(Local<Object>::Cast(args[3]));
    Cache* cache = node::ObjectWrap::Unwrap<Cache>(args.This());

    // callback
    Local<Value> callback = args[4];
    phrasematchPhraseRelevBaton *baton = new phrasematchPhraseRelevBaton();
    baton->shardlevel = shardlevel;
    baton->cache = cache;
    baton->phrases = std::move(phrases);
    baton->queryidx = std::move(queryidx);
    baton->querymask = std::move(querymask);
    baton->querydist = std::move(querydist);
    baton->request.data = baton;
    NanAssignPersistent(baton->callback, callback.As<Function>());
    uv_queue_work(uv_default_loop(), &baton->request, _phrasematchPhraseRelev, (uv_after_work_cb)phrasematchPhraseRelevAfter);
    NanReturnUndefined();
}

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
            uint64_t tmpid = h * 1e8 + (grid[i] % yd);
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
    uint64_t tmpid;
    unsigned short count;
    unsigned short reason;
    unsigned short idx;
    bool check;
    SetRelev(double _relev,
             uint64_t _id,
             uint64_t _tmpid,
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

    if (relev >= POW2_5) throw std::runtime_error("misuse: setRelev.relev > 5 bits");
    if (setRelev.count >= POW2_3)  throw std::runtime_error("misuse: setRelev.count > 3 bits");
    if (setRelev.reason >= POW2_12)  throw std::runtime_error("misuse: setRelev.reason > 12 bits");
    if (setRelev.idx >= POW2_8)  throw std::runtime_error("misuse: setRelev.idx > 8 bits");
    if (setRelev.id >= POW2_25)  throw std::runtime_error("misuse: setRelev.id > 25 bits");
    uint64_t num =
        (relev * POW2_48) +
        (setRelev.count * POW2_45) +
        (setRelev.reason * POW2_33) +
        (setRelev.idx * POW2_25) +
        (setRelev.id);
    return num;
}

inline SetRelev numberToSetRelev(uint64_t num) {
    uint64_t id = num % POW2_25;
    unsigned short idx = (num >> 25) % POW2_8;
    uint64_t tmpid = (idx * 1e8) + id;
    unsigned short reason = (num >> 33) % POW2_12;
    unsigned short count = (num >> 45) % POW2_3;
    double relev = ((num >> 48) % POW2_5) / ((double)POW2_5-1);
    return SetRelev(relev,id,tmpid,count,reason,idx,true);
}

double _setRelevance(unsigned short queryLength, std::vector<SetRelev> & sets) {
    double relevance = 0;
    double total = queryLength;
    double gappy = 0;
    unsigned short querymask = 0;
    unsigned short tally = 0;
    signed short lastdb = -1;

    std::map<unsigned short,unsigned short> reason2db;
    std::map<unsigned short,unsigned short>::iterator rit;

    // For each set, score its correspondence with the query
    std::size_t sets_size = sets.size();
    for (unsigned short i = 0; i < sets_size; i++) {
        auto & set = sets[i];
        rit = reason2db.find(set.reason);

        // Each db may contribute a distinct matching reason to the final
        // relev. If this entry is for a db that has already contributed
        // but without the same reason mark it as false.
        if (lastdb == set.idx && (rit == reason2db.end() || rit->second != set.idx)) {
            set.check = false;
            continue;
        }

        unsigned short usage = 0;
        unsigned short count = set.count;

        for (unsigned j = 0; j < queryLength; j++) {
            if (
                // make sure this term has not already been counted for
                // relevance. Uses a bitmask to mark positions counted.
                !(querymask & (1<<j)) &&
                // if this term matches the reason bitmask for relevance
                (1 << j & set.reason)
            ) {
                ++usage;
                ++tally;
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
        if (usage > 0) {
            relevance += (set.relev * (usage / total));
            reason2db.emplace(set.reason, set.idx);
            if (lastdb >= 0) gappy += (std::abs(set.idx - lastdb) - 1);
            lastdb = set.idx;
            if (tally == queryLength) break;
        } else {
            set.check = false;
        }
    }

    // Penalize relevance slightly based on whether query matches contained
    // "gaps" in continuity between index levels.
    relevance -= 0.01 * gappy;

    return relevance;
}
NAN_METHOD(Cache::setRelevance) {
    NanScope();
    if (!args[0]->IsNumber()) {
        return NanThrowTypeError("first arg must be a queryLength number");
    }
    if (!args[1]->IsArray()) {
        return NanThrowTypeError("second arg must be a sets array");
    }

    uint64_t queryLength = args[0]->NumberValue();
    std::vector<SetRelev> sets;

    Local<Array> array = Local<Array>::Cast(args[1]);
    sets.reserve(array->Length());
    for (unsigned short i = 0; i < array->Length(); i++) {
        uint64_t num = array->Get(i)->NumberValue();
        sets.emplace_back(numberToSetRelev(num));
    }

    double relevance = _setRelevance(queryLength, sets);

    std::size_t size = sets.size();
    Local<Array> setsArray = NanNew<Array>();
    unsigned short j = 0;
    for (unsigned short i = 0; i < size; i++) {
        auto const& set = sets[i];
        if (set.check == true) {
            uint64_t num = setRelevToNumber(set);
            setsArray->Set(j, NanNew<Number>(num));
            j++;
        }
    }
    Local<Object> ret = NanNew<Object>();
    ret->Set(NanNew("relevance"), NanNew<Number>(relevance));
    ret->Set(NanNew("sets"), setsArray);
    NanReturnValue(ret);
}

struct SpatialMatchBaton : carmen::noncopyable {
    uv_work_t request;
    // params
    v8::Persistent<v8::Function> callback;
    std::map<uint64_t,uint64_t> featnums;
    std::vector<Cache::intarray> grids;
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
        signed int pushed = -1;
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
        double relev = _setRelevance(queryLength, rows);
        std::size_t rows_size = rows.size();
        for (unsigned short i = 0; i < rows_size; i++) {
            auto & row = rows[i];
            // Add setRelev to sets.
            sit = sets.find(row.tmpid);
            if (sit == sets.end()) {
                sets.emplace(row.tmpid, setRelevToNumber(row));
            }

            // Don't use results after the topmost index in the stack.
            if (pushed != -1 && pushed != row.idx) {
                continue;
            }

            // Clone setRelev from row.
            SetRelev setRelev = std::move(row);
            setRelev.relev = relev;

            pushed = setRelev.idx;
            rit = rowMemo.find(setRelev.tmpid);
            if (rit != rowMemo.end()) {
                if (rit->second.relev > relev) {
                    continue;
                }
                // @TODO apply addrmod
                // if (rit->second.relev > relev + addrmod[row.dbid])
                rit->second = std::move(setRelev);
            } else {
                rowMemo.emplace(setRelev.tmpid, std::move(setRelev));
                // @TODO apply addrmod
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
        if (lastRelev == 0 || lastRelev - s.relev < 0.1) {
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
    if (!args[4]->IsFunction()) {
        return NanThrowTypeError("fifth arg must be a callback function");
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

    // callback
    Local<Value> callback = args[4];
    baton->queryLength = queryLength;
    baton->request.data = baton;
    NanAssignPersistent(baton->callback, callback.As<Function>());
    uv_queue_work(uv_default_loop(), &baton->request, _spatialMatch, (uv_after_work_cb)spatialMatchAfter);
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
