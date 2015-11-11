#include <stdarg.h>
#include <sstream>

#include "sdk/sdk.h"
#include "sdk/table.h"
#include "sdk/db.h"
#include <gflags/gflags.h>
#include "ftrace/logger.h"
#include "ftrace/trace.h"
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>

DECLARE_uint64(max_text_annotation_size);

namespace mdt {

void InitTraceModule(const std::string& flagfile) {
    TraceModule::InitTraceModule(flagfile);
}

void OpenTrace(uint64_t trace_id,
               uint64_t parent_span_id,
               uint64_t span_id,
               const std::string& name,
               const std::string& trace_name) {
    Trace::OpenTrace(trace_id, parent_span_id, span_id, name, trace_name);
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

TraceIdentify GetTraceIdentify() {
    TraceIdentify id;
    Trace* trace = Trace::TopThreadValue();
    if (trace == NULL) {
        return id;
    }
    id.CopyFrom(trace->GetTraceID());
    return id;
}

void Log(int level, const char* fmt, ...) {
    char anno_buf[FLAGS_max_text_annotation_size];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(anno_buf, sizeof(anno_buf), fmt, ap);
    va_end(ap);

    Trace::Log(level, anno_buf);
    return;
}

void KvLog(int level, const char* value, const char* fmt, ...) {
    char anno_buf[FLAGS_max_text_annotation_size];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(anno_buf, sizeof(anno_buf), fmt, ap);
    va_end(ap);

    Trace::KvLog(level, value, anno_buf);
    return;
}

// LOG interface for galaxy
void OpenProtoBufLog(const std::string& dbname, const std::string& tablename) {
    TraceModule::OpenProtoBufLog(dbname, tablename);
}

void CloseProtoBufLog(const std::string& dbname, const std::string& tablename) {
    TraceModule::CloseProtoBufLog(dbname, tablename);
}

void DummyPutCallback(mdt::Table* table, mdt::StoreRequest* request,
                        mdt::StoreResponse* response,
                        void* callback_param) {
    delete request;
    delete response;
}
void LogProtoBuf(const std::string& primary_key_name, ::google::protobuf::Message* message) {
    const ::google::protobuf::Descriptor* descriptor = message->GetDescriptor();
    const std::string& dbname = descriptor->full_name();
    const std::string& tablename = descriptor->name();
    ::mdt::Table* table = TraceModule::GetProtoBufTable(dbname, tablename);
    if (table == NULL) return ;

    // store pb
    bool log_invalid = false;
    ::mdt::StoreRequest* req = new ::mdt::StoreRequest;
    ::mdt::StoreResponse* resp = new ::mdt::StoreResponse;
    for (int i = 0; i < descriptor->field_count(); i++) {
        const ::google::protobuf::FieldDescriptor* field = descriptor->field(i);
        if (field == NULL) continue;
        if (primary_key_name == field->name()) {
            req->primary_key = TraceModule::GetFieldValue(message, field);
            req->timestamp = ::mdt::timer::get_micros();
            std::ostringstream ostr;
            if (message->SerializeToOstream(&ostr)) {
                req->data = ostr.str();
                log_invalid = true;
            }
        } else {
            ::mdt::Index idx;
            idx.index_name = field->name();
            idx.index_key = TraceModule::GetFieldValue(message, field);
            if (idx.index_key.size())
                req->index_list.push_back(idx);
        }
    }

    ::mdt::StoreCallback callback = DummyPutCallback;
    if (!log_invalid) {
        table->Put(req, resp, callback, NULL);
    } else {
        callback(table, req, resp, NULL);
    }
    return;
}

}

