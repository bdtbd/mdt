#include <unistd.h>
#include <stdarg.h>
#include <iostream>
#include <sstream>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include "proto/agent.pb.h"
#include <gflags/gflags.h>

#include "ftrace/trace.h"
#include "rpc/rpc_client.h"

DECLARE_string(flagfile);
DECLARE_uint64(mdt_max_text_annotation_size);
DECLARE_string(mdt_server_addr);
DECLARE_uint64(mdt_log_level);
DECLARE_string(mdt_flagfile);

namespace mdt {

pthread_key_t TraceModule::thread_key; // thread_load variable
Mutex TraceModule::kmutex;
std::map<uint64_t, Trace*> TraceModule::ktrace_map;
std::map<uint64_t, Trace*> TraceModule::kspan_map; // gobal span map
::mdt::RpcClient* TraceModule::rpc_client;
bool TraceModule::TraceInited = false;
pthread_t TraceModule::async_log_tid;
std::string TraceModule::FlagFile;

///////////////////////
///// TraceModule /////
///////////////////////
void TraceModule::SetGoogleFlag(const std::string& flagfile) {
    int ac = 1;
    char** av = new char*[2];
    av[0] = (char*)"dummy";
    av[1] = NULL;
    std::string local_flagfile = FLAGS_flagfile;
    FLAGS_flagfile = flagfile;
    ::google::ParseCommandLineFlags(&ac, &av, true);
    delete av;
    FLAGS_flagfile = local_flagfile;
    return;
}

/*
void* TraceModule::AsyncLogThread(void* arg) {
    while (1) {
        // update param every 5 sec
        if (TraceModule::FlagFile.size() > 0 && (access(TraceModule::FlagFile.c_str(), F_OK) == 0)) {
            TraceModule::SetGoogleFlag(TraceModule::FlagFile);
        } else if (FLAGS_mdt_flagfile.size() > 0 && (access(FLAGS_mdt_flagfile.c_str(), F_OK) == 0)) {
            TraceModule::SetGoogleFlag(FLAGS_mdt_flagfile);
        } else {
            // use default configure
        }
        usleep(10000000); // sleep 5 sec
    }
    return NULL;
}
*/

void NilCallback(void* arg) {}
static pthread_once_t trace_once = PTHREAD_ONCE_INIT;
static void TraceModuleInitOnce() {
    pthread_key_create(&TraceModule::thread_key, NilCallback);
    // local rpc client
    TraceModule::rpc_client = new RpcClient();
    TraceModule::TraceInited = true;

    if (TraceModule::FlagFile.size() > 0 && (access(TraceModule::FlagFile.c_str(), F_OK) == 0)) {
        TraceModule::SetGoogleFlag(TraceModule::FlagFile);
    } else if (FLAGS_mdt_flagfile.size() > 0 && (access(FLAGS_mdt_flagfile.c_str(), F_OK) == 0)) {
        TraceModule::SetGoogleFlag(FLAGS_mdt_flagfile);
    } else {
        // use default configure
    }
}

// trace module init once
void TraceModule::InitTraceModule(const std::string& flagfile) {
    if (TraceModule::TraceInited == false) {
        TraceModule::FlagFile = flagfile;
    }
    pthread_once(&trace_once, TraceModuleInitOnce);
    return;
}

// galaxy interface
inline uint32_t DecodeBigEndain32(const char* ptr) {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])) << 24));
}

inline uint64_t DecodeBigEndain(const char* ptr) {
    uint64_t lo = DecodeBigEndain32(ptr + 4);
    uint64_t hi = DecodeBigEndain32(ptr);
    return (hi << 32) | lo;
}

inline void EncodeBigEndian(char* buf, uint64_t value) {
    buf[0] = (value >> 56) & 0xff;
    buf[1] = (value >> 48) & 0xff;
    buf[2] = (value >> 40) & 0xff;
    buf[3] = (value >> 32) & 0xff;
    buf[4] = (value >> 24) & 0xff;
    buf[5] = (value >> 16) & 0xff;
    buf[6] = (value >> 8) & 0xff;
    buf[7] = value & 0xff;
}

inline std::string Uint64ToString(uint64_t val) {
    char buf[8];
    char* p = buf;
    EncodeBigEndian(p, val);
    std::string str_val(buf, 8);
    return str_val;
}

std::string TraceModule::GetFieldValue(::google::protobuf::Message* message,
                                       const ::google::protobuf::FieldDescriptor* field) {
    const ::google::protobuf::Reflection* reflection = message->GetReflection();
    std::ostringstream os;
    std::string s;

    if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_UINT64) {
        uint64_t val = (uint64_t)reflection->GetUInt64(*message, field);
        //return Uint64ToString(val);
        os << val;
        s = os.str();
        return s;
    } else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_INT64) {
        uint64_t val = (uint64_t)reflection->GetInt64(*message, field);
        //std::cout << "GetInt64: " << val << std::endl;
        //return Uint64ToString(val);
        os << val;
        s = os.str();
        return s;
    } else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_INT32) {
        uint64_t val = (uint64_t)reflection->GetInt32(*message, field);
        //std::cout << "GetInt32: " << val << std::endl;
        //return Uint64ToString(val);
        os << val;
        s = os.str();
        return s;
    } else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_UINT32) {
        uint64_t val = (uint64_t)reflection->GetUInt32(*message, field);
        //return Uint64ToString(val);
        os << val;
        s = os.str();
        return s;
    } else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_STRING) {
        //std::cout << "GetString: " << reflection->GetString(*message, field) << std::endl;
        return reflection->GetString(*message, field);
    } else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_BYTES) {
        return reflection->GetString(*message, field);
    } else {
        std::cout << "not support type " << field->type() << std::endl;
    }
    return "";
}

// TODO: do io in lock, but Open not frequent
void TraceModule::OpenProtoBufLog(const std::string& dbname, const std::string& tablename) {
    return;
}

void TraceModule::CloseProtoBufLog(const std::string& dbname, const std::string& tablename) {
    return;
}

void StoreCallback(const mdt::LogAgentService::RpcStoreRequest* req,
                   mdt::LogAgentService::RpcStoreResponse* resp,
                   bool failed, int error,
                   mdt::LogAgentService::LogAgentService_Stub* service) {
    delete req;
    delete service;
    delete resp;
}

inline int64_t get_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

// log pb:
//  1. export pb to local service
//  2. support string, bytes, int32, int64, uint32, uint64 to build index
//  3. TODO: support enum index, repeate type index
//  4. dbname = package name
//  5. tablename = message name
void TraceModule::LogProtoBuf(const std::string& primary_key_name, ::google::protobuf::Message* message) {
    TraceModule::InitTraceModule("");

    const ::google::protobuf::Descriptor* descriptor = message->GetDescriptor();
    const std::string& tablename = descriptor->name();
    const std::string& fullname = descriptor->full_name();
    std::string dbname(fullname, 0, fullname.size() - tablename.size() - 1);

    mdt::LogAgentService::LogAgentService_Stub* service;
    TraceModule::rpc_client->GetMethodList(FLAGS_mdt_server_addr, &service);
    mdt::LogAgentService::RpcStoreRequest* req = new mdt::LogAgentService::RpcStoreRequest;
    mdt::LogAgentService::RpcStoreResponse* resp = new mdt::LogAgentService::RpcStoreResponse;
    boost::function<void (const mdt::LogAgentService::RpcStoreRequest*,
                          mdt::LogAgentService::RpcStoreResponse*,
                          bool, int)> callback = boost::bind(&mdt::StoreCallback, _1, _2, _3, _4, service);

    // prepare request
    req->set_db_name(dbname);
    req->set_table_name(tablename);
    for (int i = 0; i < descriptor->field_count(); i++) {
        const ::google::protobuf::FieldDescriptor* field = descriptor->field(i);
        const ::google::protobuf::Reflection* reflection = message->GetReflection();
        if (field == NULL ||
            (field->label() == ::google::protobuf::FieldDescriptor::LABEL_REPEATED) ||
            !reflection->HasField(*message, field)) continue;
        // set primary key
        if (primary_key_name == field->name()) {
            req->set_primary_key(TraceModule::GetFieldValue(message, field));
            req->set_timestamp(get_micros());
            if (message->SerializeToString(req->mutable_data())) {

            } else {
                //std::cout << "serialstring fail\n";
            }
        } else {
            ::mdt::LogAgentService::RpcStoreIndex* idx = req->add_index_list();
            idx->set_index_table(field->name());
            idx->set_key(TraceModule::GetFieldValue(message, field));
        }
    }

    TraceModule::rpc_client->AsyncCall(service,
                                &mdt::LogAgentService::LogAgentService_Stub::Store,
                                req, resp, callback);
    return;
}

/////////////////////////////////////////
/////   internal interface          /////
/////////////////////////////////////////
void TraceModule::SetTrace(uint64_t key, Trace* trace) {
    MutexLock mu(&TraceModule::kmutex);
    trace->ref.Inc();
    TraceModule::ktrace_map.insert(std::pair<uint64_t, Trace*>(key, trace));
    return;
}
// trace.ref++
Trace* TraceModule::ClearAndGetTrace(uint64_t key) {
    Trace* trace = NULL;
    MutexLock mu(&TraceModule::kmutex);
    std::map<uint64_t, Trace*>::iterator it = TraceModule::ktrace_map.find(key);
    if (it != TraceModule::ktrace_map.end()) {
        trace = it->second;
        TraceModule::ktrace_map.erase(it);
        trace->ref.Dec(); // erase from ktrace_map, so dec ref
    }
    if (trace == NULL) {
        std::cout << "trace module, attach trace(key = " << key << "), not found\n";
    } else {
        // new thread will ref this trace obj, so add ref
        trace->ref.Inc();
    }
    return trace;
}

// trace.ref++
Trace* TraceModule::GetTraceBySpanId(uint64_t span_id) {
    Trace* trace = NULL;
    MutexLock mu(&TraceModule::kmutex);
    std::map<uint64_t, Trace*>::iterator it = TraceModule::kspan_map.find(span_id);
    if (it != TraceModule::kspan_map.end()) {
        trace = it->second;
        trace->ref.Inc();
    }
    return trace;
}

// trace.ref++
void TraceModule::SetTraceBySpanId(uint64_t span_id, Trace* trace) {
    MutexLock mu(&TraceModule::kmutex);
    trace->ref.Inc();
    TraceModule::kspan_map.insert(std::pair<uint64_t, Trace*>(span_id, trace));
    return;
}

// trace.ref--, last one flush log
void TraceModule::ClearTraceBySpanId(uint64_t span_id) {
    Trace* trace = NULL;
    MutexLock mu(&TraceModule::kmutex);
    std::map<uint64_t, Trace*>::iterator it = TraceModule::kspan_map.find(span_id);
    if (it != TraceModule::kspan_map.end()) {
        trace = it->second;
        if (trace->ref.Dec() == 0) {
            // last one remove from map
            TraceModule::kspan_map.erase(it);
            TraceModule::kmutex.Unlock();
            delete trace;
            TraceModule::kmutex.Lock();
        }
    }
    return;
}

uint64_t TraceModule::GenerateUUID() {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    uint64_t uuid_val = 0;
    for (int i = 0; i < 8; i++) {
        uint64_t tmp = (uint64_t)uuid.data[i];
        uuid_val |= tmp << (i * 8);
    }
    return uuid_val;
}

///////////////////////////
/////   Trace impl    /////
///////////////////////////
void Trace::OpenTrace(uint64_t trace_id,
                      uint64_t parent_span_id,
                      uint64_t span_id,
                      const std::string& db_name,
                      const std::string& table_name) {
    TraceModule::InitTraceModule("");

    bool new_span = false;
    Trace* trace = NULL;
    if (trace_id == 0) {
        trace_id = TraceModule::GenerateUUID();
        parent_span_id = 0;
        span_id = TraceModule::GenerateUUID();
        new_span = true;
    } else if (span_id == 0) {
        span_id = TraceModule::GenerateUUID();
        new_span = true;
    } else {
        trace = TraceModule::GetTraceBySpanId(span_id);
        if (trace == NULL) {
            std::cout << "trace id " << trace_id << ", span id " << span_id << ", not exist, New it\n";
            new_span = true;
        }
    }
    if (new_span) {
        trace = new Trace(trace_id, parent_span_id, span_id, db_name, table_name);
        TraceModule::SetTraceBySpanId(span_id, trace);
    }

    // store in thread local
    trace->PushThreadValue();
    return;
}

void Trace::ReleaseTrace() {
    TraceModule::InitTraceModule("");

    Trace* trace = TopThreadValue();
    if (trace == NULL) {
        return;
    }
    trace->PopThreadValue();
    uint64_t tid, pid, id;
    std::string db_name, table_name;
    if (trace->GetTraceIdentify(&tid, &pid, &id, &db_name, &table_name) == 0) {
        TraceModule::ClearTraceBySpanId(id);
    }
    return;
}

// BEGIN jump to next thread
void Trace::AttachTrace(uint64_t attach_id) {
    TraceModule::InitTraceModule("");

    Trace* trace = TopThreadValue();
    if (trace == NULL) {
        return;
    }
    TraceModule::SetTrace(attach_id, trace);
}

// FINISH jump to next thread
void Trace::DetachAndOpenTrace(uint64_t attach_id) {
    TraceModule::InitTraceModule("");

    Trace* trace = TraceModule::ClearAndGetTrace(attach_id);
    if (trace == NULL) {
        return;
    }
    // store in thread local
    trace->PushThreadValue();
}

void Trace::Log(int level, const char* fmt, ...) {
    TraceModule::InitTraceModule("");

    if (FLAGS_mdt_log_level < (uint64_t)level) {
        return;
    }
    char anno_buf[FLAGS_mdt_max_text_annotation_size];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(anno_buf, sizeof(anno_buf), fmt, ap);
    va_end(ap);

    Trace* trace = Trace::TopThreadValue();
    if (trace == NULL) {
        return;
    }
    trace->AddTextAnnotation((char *)anno_buf);
    return;
}

void Trace::KvLog(int level, const char* value, const char* fmt, ...) {
    TraceModule::InitTraceModule("");

    if (FLAGS_mdt_log_level < (uint64_t)level) {
        return;
    }
    char anno_buf[FLAGS_mdt_max_text_annotation_size];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(anno_buf, sizeof(anno_buf), fmt, ap);
    va_end(ap);

    Trace* trace = Trace::TopThreadValue();
    if (trace == NULL) {
        return;
    }
    trace->AddKvAnnotation(anno_buf, value);
}

///////////////////////////////////
/////   internal interface    /////
///////////////////////////////////
void FlushLogCallback(const mdt::LogAgentService::RpcStoreSpanRequest* req,
                      mdt::LogAgentService::RpcStoreSpanResponse* resp,
                      bool failed, int error,
                      mdt::LogAgentService::LogAgentService_Stub* service) {
    delete req;
    delete service;
    delete resp;
}

void Trace::FlushLog() {
#if 0
    mu_.Lock();
    // TODO: flush annotations
    std::cout << "\n[stub]flush annotations =====>\n";
    TEST_PrintLog();
    span_.clear_annotations();
    mu_.Unlock();
#endif
    mdt::LogAgentService::LogAgentService_Stub* service;
    TraceModule::rpc_client->GetMethodList(FLAGS_mdt_server_addr, &service);
    mdt::LogAgentService::RpcStoreSpanRequest* req = new mdt::LogAgentService::RpcStoreSpanRequest;
    mdt::LogAgentService::RpcStoreSpanResponse* resp = new mdt::LogAgentService::RpcStoreSpanResponse;
    req->mutable_span()->CopyFrom(span_);
    boost::function<void (const mdt::LogAgentService::RpcStoreSpanRequest*,
                          mdt::LogAgentService::RpcStoreSpanResponse*,
                          bool, int)> callback = boost::bind(&mdt::FlushLogCallback, _1, _2, _3, _4, service);
    TraceModule::rpc_client->AsyncCall(service,
                                &mdt::LogAgentService::LogAgentService_Stub::RpcStoreSpan,
                                req, resp, callback);
    std::cout << span_.DebugString() << std::endl;
}

void Trace::AddTextAnnotation(const std::string& text) {
    mu_.Lock();
    ::mdt::LogAgentService::FunctionAnnotation* anno = span_.add_annotations();
    anno->set_timestamp(timer::get_micros());
    anno->set_text_context(text);
    mu_.Unlock();
}

void Trace::AddKvAnnotation(const std::string& key, const std::string& value) {
    mu_.Lock();
    ::mdt::LogAgentService::FunctionAnnotation* anno = span_.add_annotations();
    anno->set_timestamp(timer::get_micros());
    anno->mutable_kv_context()->set_key(key);
    anno->mutable_kv_context()->set_value(value);
    mu_.Unlock();
}

Trace::Trace(uint64_t trace_id, uint64_t parent_span_id, uint64_t span_id,
             const std::string& db_name, const std::string& table_name) {
    span_.mutable_id()->set_trace_id(trace_id);
    span_.mutable_id()->set_parent_span_id(parent_span_id);
    span_.mutable_id()->set_span_id(span_id);
    span_.set_db_name(db_name);
    span_.set_table_name(table_name);
    ref.Set(0);
}

Trace::~Trace() {
    FlushLog();
}

Trace* Trace::TopThreadValue() {
    TraceModule::InitTraceModule("");

    Trace* trace = (Trace*)pthread_getspecific(TraceModule::thread_key);
    if (trace == NULL) {
        //std::cout << "WARNING: trace in thread local is nil\n";
    }
    return trace;
}

// step into thread
void Trace::PushThreadValue() {
    uint64_t tid = (uint64_t)pthread_self();
    void* prev_trace = pthread_getspecific(TraceModule::thread_key);
    pthread_setspecific(TraceModule::thread_key, (void*)this);

    //std::cout << "<tid> " << tid << ", <trace> " << (uint64_t)this << std::endl;

    mu_.Lock();
    std::map<uint64_t, std::stack<void*> >::iterator it = thread_value_.find(tid);
    if (it != thread_value_.end()) {
        std::stack<void*>& st = it->second;
        st.push(prev_trace);
    } else if (prev_trace) {
        std::stack<void*> new_st;
        new_st.push(prev_trace);
        thread_value_.insert(std::pair<uint64_t, std::stack<void*> >(tid, new_st));
    }
    mu_.Unlock();
}

// leave from thread
void Trace::PopThreadValue() {
    uint64_t tid = (uint64_t)pthread_self();
    void* prev_trace = NULL;

    mu_.Lock();
    std::map<uint64_t, std::stack<void*> >::iterator it = thread_value_.find(tid);
    if (it != thread_value_.end()) {
        std::stack<void*>& st = it->second;
        if (st.size() != 0) {
            prev_trace = st.top();
            st.pop();
        }
    }
    mu_.Unlock();
    pthread_setspecific(TraceModule::thread_key, prev_trace);
}

int Trace::GetTraceIdentify(uint64_t* tid, uint64_t* pid, uint64_t* id,
                     std::string* db_name, std::string* table_name) {
    *tid = span_.id().trace_id();
    *pid = span_.id().parent_span_id();
    *id = span_.id().span_id();
    *db_name = span_.db_name();
    *table_name = span_.table_name();
    return 0;
}

void Trace::MergeTrace(::mdt::LogAgentService::FunctionSpan span) {
    mu_.Lock();
    if ((span_.id().trace_id() == span.id().trace_id()) &&
       (span_.id().parent_span_id() == span.id().parent_span_id()) &&
       (span_.id().span_id() == span.id().span_id())) {
        for (int i = 0; i < span.annotations_size(); i++) {
            ::mdt::LogAgentService::FunctionAnnotation* anno = span_.add_annotations();
            anno->CopyFrom(span.annotations(i));
        }
    }
    mu_.Unlock();
    return;
}

void Trace::TEST_PrintLog() {
    std::cout << "SPAN PRINTER: trace id " << span_.id().trace_id()
        << ", parent_span_id " << span_.id().parent_span_id()
        << ", span_id " << span_.id().span_id() << "\n";
    for (int i = 0; i < span_.annotations_size(); i++) {
        if (span_.annotations(i).has_text_context()) {
            std::cout << "\t\tTS: " << span_.annotations(i).timestamp()
                << ", TEXT: " << span_.annotations(i).text_context() << std::endl;
        }
        if (span_.annotations(i).has_kv_context()) {
            std::cout << "\t\tTS: " << span_.annotations(i).timestamp()
                << ", KV: " << span_.annotations(i).kv_context().key()
                << " = " << span_.annotations(i).kv_context().value() << std::endl;
        }
    }
}

}
