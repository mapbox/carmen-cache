
#include "binding.hpp"
#include <node_version.h>
#include <node_buffer.h>

#include "pbf.hpp"

#include <sstream>

namespace carmen {

using namespace v8;

Persistent<FunctionTemplate> Cache::constructor;

std::string shard(uint64_t level, uint64_t id) {
    if (level == 0) return "0";
    unsigned int bits = 32 - (level * 4);
    unsigned int shard_id = std::floor(id / std::pow(2, bits));
    return std::to_string(shard_id);
}

Cache::intarray arrayToVector(Local<Array> array) {
    Cache::intarray vector;
    for (uint64_t i = 0; i < array->Length(); i++) {
        uint64_t num = array->Get(i)->NumberValue();
        vector.push_back(num);
    }
    return vector;
}

Local<Array> vectorToArray(Cache::intarray vector) {
    std::size_t size = vector.size();
    Local<Array> array = NanNew<Array>(static_cast<int>(size));
    for (uint64_t i = 0; i < size; i++) {
        array->Set(i, NanNew<Number>(vector[i]));
    }
    return array;
}

Local<Object> mapToObject(std::map<std::uint64_t,std::uint64_t> map) {
    Local<Object> object = NanNew<Object>();
    typedef std::map<std::uint64_t,std::uint64_t>::iterator it_type;
    for (it_type it = map.begin(); it != map.end(); it++) {
        object->Set(NanNew<Number>(it->first), NanNew<Number>(it->second));
    }
    return object;
}

std::map<std::uint64_t,std::uint64_t> objectToMap(Local<Object> object) {
    std::map<std::uint64_t,std::uint64_t> map;
    const Local<Array> keys = object->GetPropertyNames();
    const uint32_t length = keys->Length();
    for (uint32_t i = 0; i < length; i++) {
        uint64_t key = keys->Get(i)->NumberValue();
        uint64_t value = object->Get(key)->NumberValue();
        map.insert(std::make_pair(key, value));
    }
    return map;
}

Cache::intarray __get(Cache* c, std::string type, std::string shard, uint64_t id) {
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
            Cache::intarray const& array = aitr->second;
            return array;
        }
    }
}


void Cache::Initialize(Handle<Object> target) {
    NanScope();
    Local<FunctionTemplate> t = FunctionTemplate::New(Cache::New);
    t->InstanceTemplate()->SetInternalFieldCount(1);
    t->SetClassName(String::NewSymbol("Cache"));
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
    target->Set(String::NewSymbol("Cache"),t->GetFunction());
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
            for (auto item : itr->second) {
                ::carmen::proto::object_item * new_item = message.add_items(); 
                new_item->set_key(item.first);
                Cache::intarray const & varr = item.second;
                std::size_t varr_size = varr.size();
                for (std::size_t i=0;i<varr_size;++i) {
                    new_item->add_val(static_cast<int64_t>(varr[i]));
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
                for (auto item : litr->second) {
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
    NanReturnValue(Undefined());
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
        Local<Array> ids = Array::New();
        if (args.Length() == 1) {
            unsigned idx = 0;
            for (auto item : mem) {
                if (item.first.size() > type.size() && item.first.substr(0,type.size()) == type) {
                    std::string shard = item.first.substr(type.size()+1,item.first.size());
                    ids->Set(idx++,Number::New(String::New(shard.c_str())->NumberValue()));
                }
            }
            for (auto item : lazy) {
                if (item.first.size() > type.size() && item.first.substr(0,type.size()) == type) {
                    std::string shard = item.first.substr(type.size()+1,item.first.size());
                    ids->Set(idx++,Number::New(String::New(shard.c_str())->NumberValue()));
                }
            }
            NanReturnValue(ids);
        } else if (args.Length() == 2) {
            std::string shard = *String::Utf8Value(args[1]->ToString());
            std::string key = type + "-" + shard;
            Cache::memcache::const_iterator itr = mem.find(key);
            unsigned idx = 0;
            if (itr != mem.end()) {
                for (auto item : itr->second) {
                    ids->Set(idx++,Number::New(item.first)->ToString());
                }
            }
            Cache::lazycache::const_iterator litr = lazy.find(key);
            if (litr != lazy.end()) {
                for (auto item : litr->second) {
                    ids->Set(idx++,Number::New(item.first)->ToString());
                }
            }
            NanReturnValue(ids);
        }
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
    NanReturnValue(Undefined());
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
            c->cache_.insert(std::make_pair(key,Cache::arraycache()));
        }
        Cache::arraycache & arrc = c->cache_[key];
        Cache::arraycache::key_type key_id = static_cast<Cache::arraycache::key_type>(args[2]->IntegerValue());
        Cache::arraycache::iterator itr2 = arrc.find(key_id);
        if (itr2 == arrc.end()) {
            arrc.insert(std::make_pair(key_id,Cache::intarray()));
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
    NanReturnValue(Undefined());
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
                    //  - libstdc++ does not support std::map::emplace
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
            c->cache_.insert(std::make_pair(key,arraycache()));
        }
        Cache::memcache::iterator itr2 = mem.find(key);
        if (itr2 != mem.end()) {
            mem.erase(itr2);
        }
        Cache::lazycache & lazy = c->lazy_;
        Cache::lazycache::iterator litr = lazy.find(key);
        Cache::message_cache & messages = c->msg_;
        if (litr == lazy.end()) {
            c->lazy_.insert(std::make_pair(key,Cache::larraycache()));
            messages.insert(std::make_pair(key,std::string(node::Buffer::Data(obj),node::Buffer::Length(obj))));
        }
        load_into_cache(c->lazy_[key],node::Buffer::Data(obj),node::Buffer::Length(obj));
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
    NanReturnValue(Undefined());
}

struct load_baton {
    uv_work_t request;
    Cache * c;
    NanCallback cb;
    Cache::larraycache arrc;
    std::string key;
    std::string data;
    bool error;
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
      error(false),
      error_name() {
        request.data = this;
        c->_ref();
      }
    ~load_baton() {
         c->_unref();
         //closure->cb.Dispose();
    }
};

void Cache::AsyncLoad(uv_work_t* req) {
    load_baton *closure = static_cast<load_baton *>(req->data);
    try {
        load_into_cache(closure->arrc,closure->data.data(),closure->data.size());
    }
    catch (std::exception const& ex)
    {
        closure->error = true;
        closure->error_name = ex.what();
    }
}

void Cache::AfterLoad(uv_work_t* req) {
    NanScope();
    load_baton *closure = static_cast<load_baton *>(req->data);
    TryCatch try_catch;
    if (closure->error) {
        Local<Value> argv[1] = { Exception::Error(String::New(closure->error_name.c_str())) };
        closure->cb.Call(1, argv);
    } else {
        Cache::memcache::iterator itr2 = closure->c->cache_.find(closure->key);
        if (itr2 != closure->c->cache_.end()) {
            closure->c->cache_.erase(itr2);
        }
        closure->c->lazy_[closure->key] = std::move(closure->arrc);
        Local<Value> argv[1] = { Local<Value>::New(Null()) };
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
            messages.insert(std::make_pair(key,std::string(node::Buffer::Data(obj),node::Buffer::Length(obj))));
        }
        NanReturnValue(Undefined());
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
            NanReturnValue(True());
        } else {
            Cache::message_cache const& messages = c->msg_;
            if (messages.find(key) != messages.end()) {
                NanReturnValue(True());
            }
            NanReturnValue(False());
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
        if (vector.size() > 0) {
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
    NanReturnValue(Boolean::New(hit));
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
        args.This()->Set(String::NewSymbol("id"),args[0]);
        args.This()->Set(String::NewSymbol("shardlevel"),args[1]);
        NanReturnValue(args.This());
    } catch (std::exception const& ex) {
        return NanThrowTypeError(ex.what());
    }
    NanReturnValue(Undefined());
}

// cache.phrasematchDegens(termidx, degens)
struct phrasematchDegensBaton {
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
    for (uint64_t idx = 0; idx < baton->results.size(); idx++) {
        std::vector<std::uint64_t> degens = baton->results[idx];
        std::sort(degens.begin(), degens.end(), sortDegens);
        for (std::size_t i = 0; i < degens.size() && i < 10; i++) {
            uint64_t term = degens[i] >> 4 << 4;
            terms.push_back(term);

            std::map<std::uint64_t,std::uint64_t>::iterator it;

            it = queryidx.find(term);
            if (it == queryidx.end()) {
                queryidx.insert(std::make_pair(term, idx));
            }

            it = querymask.find(term);
            if (it == querymask.end()) {
                querymask.insert(std::make_pair(term, 0 + (1<<idx)));
            } else {
                it->second = it->second + (1<<idx);
            }

            it = querydist.find(term);
            if (it == querydist.end()) {
                querydist.insert(std::make_pair(term, term % 16));
            }
        }
    }

    baton->terms = terms;
    baton->queryidx = queryidx;
    baton->querymask = querymask;
    baton->querydist = querydist;
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

    // convert v8 results array into nested std::map
    Local<Array> resultsArray = Local<Array>::Cast(args[0]);
    std::vector<std::vector<std::uint64_t>> results;
    for (uint64_t i = 0; i < resultsArray->Length(); i++) {
        std::vector<std::uint64_t> degens = arrayToVector(Local<Array>::Cast(resultsArray->Get(i)));
        results.push_back(degens);
    }

    // callback
    Local<Value> callback = args[1];
    phrasematchDegensBaton *baton = new phrasematchDegensBaton();
    baton->results = results;
    baton->request.data = baton;
    NanAssignPersistent(baton->callback, callback.As<Function>());
    uv_queue_work(uv_default_loop(), &baton->request, _phrasematchDegens, (uv_after_work_cb)phrasematchDegensAfter);
    NanReturnUndefined();
}

struct phraseRelev {
    uint64_t id;
    double relev;
    double tmprelev;
    unsigned short count;
    unsigned short reason;
};
struct phrasematchPhraseRelevBaton {
    v8::Persistent<v8::Function> callback;
    uv_work_t request;
    Cache* cache;
    uint64_t shardlevel;
    std::string error;
    std::vector<phraseRelev> relevantPhrases;
    std::vector<std::uint64_t> phrases;
    std::map<std::uint64_t,std::uint64_t> queryidx;
    std::map<std::uint64_t,std::uint64_t> querymask;
    std::map<std::uint64_t,std::uint64_t> querydist;
};
bool sortPhraseRelev(phraseRelev a, phraseRelev b) {
    return a.id < b.id;
}
bool uniqPhraseRelev(phraseRelev a, phraseRelev b) {
    return a.id == b.id;
}
void _phrasematchPhraseRelev(uv_work_t* req) {
    phrasematchPhraseRelevBaton *baton = static_cast<phrasematchPhraseRelevBaton *>(req->data);

    std::string type = "phrase";
    double max_relev = 0;
    double min_relev = 1;
    std::vector<phraseRelev> allPhrases;
    std::vector<phraseRelev> relevantPhrases;

    for (uint64_t a = 0; a < baton->phrases.size(); a++) {
        uint64_t id = baton->phrases[a];
        Cache::intarray phrase = __get(baton->cache, "phrase", shard(baton->shardlevel, id), id);
        unsigned short size = phrase.size();
        if (size == 0) {
            baton->error = "Failed to get phrase";
            return;
        }

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
            phraseRelev pr;
            pr.id = id;
            pr.count = count;
            pr.relev = relev;
            pr.reason = reason;
            pr.tmprelev = (relev * 1e6) + count;
            allPhrases.push_back(pr);
            if (relev > 0.75) {
                relevantPhrases.push_back(pr);
            }
        }
    }

    // Reduces the relevance bar to 0.50 since all results have identical relevance values
    if (min_relev == max_relev) {
        relevantPhrases = allPhrases;
    }

    std::sort(relevantPhrases.begin(), relevantPhrases.end(), sortPhraseRelev);
    std::unique(relevantPhrases.begin(), relevantPhrases.end(), uniqPhraseRelev);
    baton->relevantPhrases = relevantPhrases;
}
void phrasematchPhraseRelevAfter(uv_work_t* req) {
    NanScope();
    phrasematchPhraseRelevBaton *baton = static_cast<phrasematchPhraseRelevBaton *>(req->data);

    if (baton->error.size() > 0) {
        Local<Value> argv[1] = { NanError(baton->error.c_str()) };
        NanMakeCallback(NanGetCurrentContext()->Global(), NanNew(baton->callback), 1, argv);
    } else {
        unsigned short relevsize = baton->relevantPhrases.size();
        Local<Array> result = NanNew<Array>(static_cast<int>(relevsize));
        Local<Object> relevs = NanNew<Object>();
        for (uint32_t i = 0; i < relevsize; i++) {
            Local<Object> phraseRelevObject = NanNew<Object>();
            phraseRelevObject->Set(NanNew("count"), NanNew<Number>(baton->relevantPhrases[i].count));
            phraseRelevObject->Set(NanNew("reason"), NanNew<Number>(baton->relevantPhrases[i].reason));
            phraseRelevObject->Set(NanNew("relev"), NanNew<Number>(baton->relevantPhrases[i].relev));
            phraseRelevObject->Set(NanNew("tmprelev"), NanNew<Number>(baton->relevantPhrases[i].tmprelev));
            result->Set(i, NanNew<Number>(baton->relevantPhrases[i].id));
            relevs->Set(NanNew<Number>(baton->relevantPhrases[i].id), phraseRelevObject);
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
    baton->phrases = phrases;
    baton->queryidx = queryidx;
    baton->querymask = querymask;
    baton->querydist = querydist;
    baton->request.data = baton;
    NanAssignPersistent(baton->callback, callback.As<Function>());
    uv_queue_work(uv_default_loop(), &baton->request, _phrasematchPhraseRelev, (uv_after_work_cb)phrasematchPhraseRelevAfter);
    NanReturnUndefined();
}

extern "C" {
    static void start(Handle<Object> target) {
        Cache::Initialize(target);
    }
}

} // namespace carmen

NODE_MODULE(carmen, carmen::start)
