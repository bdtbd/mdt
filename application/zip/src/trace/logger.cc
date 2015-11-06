#include "trace/logger.h"
#include "trace/trace.h"

namespace mdt {
void InitTraceModule(const std::string& flagfile) {
    TraceModule::InitTraceModule(flagfile);
}

void OpenTrace(uint64_t trace_id = 0,
               uint64_t parent_span_id = 0,
               uint64_t span_id = 0,
               std::string& name) {
    Trace::OpenTrace(trace_id, parent_span_id, span_id, name);
}

void AttachTrace(uint64_t attach_id) {
    Trace::AttachTrace(attach_id);
}

void DetachTrace(uint64_t attach_id) {
    Trace::DetachTrace(attach_id);
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

}

