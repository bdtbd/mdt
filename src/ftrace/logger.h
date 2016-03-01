#ifndef FTRACE_LOGGER_H_
#define FTRACE_LOGGER_H_

#include <iostream>
#include <stdint.h>
#include <google/protobuf/message.h>

namespace mdt {
// common interface
// trace guard
class TraceGuard {
public:
    explicit TraceGuard(const std::string& db_name, const std::string& table_name);
    explicit TraceGuard(uint64_t attach_id);
    explicit TraceGuard(uint64_t trace_id,
                        uint64_t parent_span_id,
                        uint64_t span_id,
                        const std::string& db_name,
                        const std::string& table_name);
    ~TraceGuard();
private:
    TraceGuard(const TraceGuard&);
    void operator=(const TraceGuard&);
};

void InitTraceModule(const std::string& flagfile);

void OpenTrace(uint64_t trace_id,
               uint64_t parent_span_id,
               uint64_t span_id,
               const std::string& db_name,
               const std::string& table_name);

void AttachTrace(uint64_t attach_id); // use for trace_context transform between threads

void DetachAndOpenTrace(uint64_t attach_id);

void ReleaseTrace();

int GetTraceIdentify(uint64_t* tid, uint64_t* pid, uint64_t* id,
                     std::string* db_name, std::string* table_name);

void Log(int level, const char* fmt, ...);

void KvLog(int level, const char* value, const char* fmt, ...);

// log protobuf interface
void LogProtoBuf(const std::string& primary_key_name, ::google::protobuf::Message* message);

}
#endif
