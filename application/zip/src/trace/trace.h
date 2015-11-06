#ifndef ZIP_SRC_TRACE_TRACE_H_
#define ZIP_SRC_TRACE_TRACE_H_

namespace mdt {

class Trace {
public:
    //////////////////////////////
    /////   User interface   /////
    //////////////////////////////
    static void OpenTrace(uint64_t trace_id = 0,
                          uint64_t parent_span_id = 0,
                          uint64_t span_id = 0,
                          std::string& name);
    static void AttachTrace(uint64_t attach_id); // use for trace_context transform between threads
    static void DetachTrace(uint64_t attach_id);
    static void ReleaseTrace();

    static void Log(int level, const char* fmt, ...);
    static void Log(int level, const char* value, const char* fmt, ...);

    ////////////////////////////////
    /////  internal interface  /////
    ////////////////////////////////
    Trace(uint64_t trace_id, uint64_t parent_span_id, uint64_t span_id,
          const std::string& name, const std::string& trace_name);
    ~Trace();
    // rpc trace relatively
    TraceIdentify GetTraceID();

    static Trace* TopThreadValue(); // Get from thread_local
    void PushThreadValue();
    void PopThreadValue();

    void MergeTrace(FunctionSpan span);

    void TEST_PrintLog();

    Counter ref; // protect by TraceModule::kmutex, because Trace should be thread safe

private:

    Mutex mu_;
    FunctionSpan span_;
    std::map<uint64_t, std::stack<void*> > thread_value_; // <thread id, stack<Trace ptr> >
};

struct TraceModule {
    //////////////////////////////
    /////   User interface   /////
    //////////////////////////////
    static void InitTraceModule(const std::string& flagfile);

    ////////////////////////////////
    /////  internal interface  /////
    ////////////////////////////////
    static uint64_t GenerateUUID();
    static pthread_key_t thread_key; // thread_load variable
    // trace bewteen threads
    static Trace* ClearTrace(uint64_t key); // Get and remove from trace_map_
    static void SetTrace(uint64_t key, Trace* trace); // Put into trace_map_

    static Trace* GetTraceBySpanId(uint64_t span_id);
    static void SetTraceBySpanId(uint64_t span_id, Trace* trace);
    static void ClearTraceBySpanId(uint64_t span_id);

    static Mutex kmutex;
    static std::map<uint64_t, Trace*> ktrace_map;
    static std::map<uint64_t, Trace*> kspan_map; // gobal span map
};

}

#endif
