#ifndef ZIP_SRC_TRACE_LOGGER_H_
#define ZIP_SRC_TRACE_LOGGER_H_

namespace mdt {
void InitTraceModule(const std::string& flagfile);
void OpenTrace(uint64_t trace_id = 0,
               uint64_t parent_span_id = 0,
               uint64_t span_id = 0,
               std::string& name);
void AttachTrace(uint64_t attach_id); // use for trace_context transform between threads
void DetachTrace(uint64_t attach_id);
void ReleaseTrace();

#define LOG(level, fmt, args...) do { \
        Trace::Log(level, fmt, ##args); \
    } while(0)
#define LOG(level, value, fmt, args...) do { \
        Trace::Log(level, value, fmt, ##args); \
    } while(0)
TraceIdentify GetTraceIdentify();
}
#endif
