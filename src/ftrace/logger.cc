#include <stdarg.h>
#include <sstream>

#include <gflags/gflags.h>
#include "ftrace/logger.h"
#include "ftrace/trace.h"
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <sofa/pbrpc/pbrpc.h>

DECLARE_uint64(mdt_max_text_annotation_size);

namespace mdt {

void InitTraceModule(const std::string& flagfile) {
    TraceModule::InitTraceModule(flagfile);
}

void OpenTrace(uint64_t trace_id,
               uint64_t parent_span_id,
               uint64_t span_id,
               const std::string& db_name,
               const std::string& table_name) {
    Trace::OpenTrace(trace_id, parent_span_id, span_id, db_name, table_name);
}

void AttachTrace(uint64_t attach_id) {
    Trace::AttachTrace(attach_id);
}

void DetachAndOpenTrace(uint64_t attach_id) {
    Trace::DetachAndOpenTrace(attach_id);
}

void ReleaseTrace() {
    Trace::ReleaseTrace();
}

TraceGuard::TraceGuard(uint64_t trace_id,
                       uint64_t parent_span_id,
                       uint64_t span_id,
                       const std::string& db_name,
                       const std::string& table_name) {
    Trace::OpenTrace(trace_id, parent_span_id, span_id, db_name, table_name);
}

TraceGuard::TraceGuard(uint64_t attach_id) {
    Trace::DetachAndOpenTrace(attach_id);
}

TraceGuard::~TraceGuard() {
    Trace::ReleaseTrace();
}
 // use for rpc
int GetTraceIdentify(uint64_t* tid, uint64_t* pid, uint64_t* id,
                     std::string* db_name, std::string* table_name) {
    Trace* trace = Trace::TopThreadValue();
    if (trace == NULL) {
        return -1;
    }
    trace->GetTraceIdentify(tid, pid, id, db_name, table_name);
    return 0;
}

void Log(int level, const char* fmt, ...) {
    char anno_buf[FLAGS_mdt_max_text_annotation_size];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(anno_buf, sizeof(anno_buf), fmt, ap);
    va_end(ap);

    Trace::Log(level, anno_buf);
    return;
}

void KvLog(int level, const char* value, const char* fmt, ...) {
    char anno_buf[FLAGS_mdt_max_text_annotation_size];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(anno_buf, sizeof(anno_buf), fmt, ap);
    va_end(ap);

    Trace::KvLog(level, value, anno_buf);
    return;
}

////////////////////////////////////////////////
///////    LOG interface for galaxy   //////////
////////////////////////////////////////////////
// log pb:
//  1. export pb to local service
//  2. support string, bytes, int32, int64, uint32, uint64 to build index
//  3. TODO: support enum index, repeate type index
//  4. dbname = package name
//  5. tablename = message name
void LogProtoBuf(const std::string& primary_key_name, ::google::protobuf::Message* message) {
    TraceModule::LogProtoBuf(primary_key_name, message);
    return;
}

}

