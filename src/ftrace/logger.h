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
    enum RpcEventTyep {
        CS = 1,
        SR = 2,
        SS = 3,
        CR = 4,
    };
    explicit TraceGuard(int level, int event, ::google::protobuf::Message* req, ::google::protobuf::Message* resp);

    ~TraceGuard();
private:
    bool need_release;
    TraceGuard(const TraceGuard&);
    void operator=(const TraceGuard&);
};

void InitTraceModule(const std::string& flagfile);

void AttachTrace(uint64_t attach_id); // use for trace_context transform between threads

void Log(int level, const char* fmt, ...);

// log protobuf interface
void LogProtoBuf(const std::string& primary_key_name, ::google::protobuf::Message* message);

}
#endif
