#include <unistd.h>
#include <iostream>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "trace/trace.h"

DECLARE_string(flagfile);
DECLARE_uint64(max_text_annotation_size);
DECLARE_uint64(log_level);

namespace mdt {
///////////////////////
///// TraceModule /////
///////////////////////
void* NilCallback(void* arg) {
    return NULL;
}
void TraceModule::InitTraceModule(const std::string& flagfile) {
    // Get configure
    if (flagfile.size() == 0 || access(flagfile.c_str(), F_OK)) {
        std::cout << "ZIPLOG: use default configure\n";
    } else {
        int ac = 1;
        char* av[2] = {"dummy", NULL};
        std::string local_flagfile = FLAGS_flagfile;
        FLAGS_flagfile = flagfile;
        ::google::ParseCommandLineFlags(&ac, &av, true);
        FLAGS_flagfile = local_flagfile;
        std::cout << "ZIPLOG: use " << flagfile << " to configure lib\n";
    }
    pthread_key_create(&thread_key, NilCallback);
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
    std::map<uint64_t, Trace*>::iterator it = TraceModule::kspan_map.find(key);
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
    std::map<uint64_t, Trace*>::iterator it = TraceModule::kspan_map.find(key);
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
                      const std::string& name,
                      const std::string& trace_name) {
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
            std::cout << "trace id " << trace_id << ", span id " << span_id << ", not exist, New it";
            new_span = true;
        }
    }
    if (new_span) {
        trace = new Trace(trace_id, parent_span_id, span_id, name, trace_name);
        TraceModule::SetTraceBySpanId(span_id, trace);
    }

    // store in thread local
    trace->PushThreadValue();
    return;
}

void Trace::ReleaseTrace() {
    Trace* trace = TopThreadValue();
    if (trace == NULL) {
        return;
    }
    trace->PopThreadValue();
    TraceModule::ClearTraceBySpanId(trace->GetTraceID().span_id());
    return;
}

// BEGIN jump to next thread
void Trace::AttachTrace(uint64_t attach_id) {
    Trace* trace = TopThreadValue();
    if (trace == NULL) {
        return;
    }
    TraceModule::SetTrace(attach_id, trace);
}

// FINISH jump to next thread
void Trace::DetachTrace(uint64_t attach_id) {
    Trace* trace = TraceModule::ClearAndGetTrace(attach_id);
    if (trace == NULL) {
        return;
    }
    // store in thread local
    trace->PushThreadValue();
}

void Trace::Log(int level, const char* fmt, ...) {
    if (FLAGS_log_level > level) {
        return;
    }
    char anno_buf[FLAGS_max_text_annotation_size];
    va_list ap;
    va_start(ap, fmt);
    int size = vsnprintf(anno_buf, sizeof(anno_buf), fmt, ap);
    va_end(ap);

    Trace* trace = Trace::TopThreadValue();
    if (trace == NULL) {
        return;
    }
    trace->AddTextAnnotation((char *)anno_buf);
    return;
}

void Trace::Log(int level, const char* value, const char* fmt, ...) {
    if (FLAGS_log_level > level) {
        return;
    }
    char anno_buf[FLAGS_max_text_annotation_size];
    va_list ap;
    va_start(ap, fmt);
    int size = vsnprintf(anno_buf, sizeof(anno_buf), fmt, ap);
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
void Trace::FlushLog() {
    mu_.Lock();
    // TODO: flush annotations
    std::cout << "[stub]flush annotations =====>\n";
    TETS_PrintLog();
    span_.annotations().Clear();
    mu_.Unlock();
}

void Trace::AddTextAnnotation(const std::string& text) {
    mu_.Lock();
    FunctionAnnotation* anno = span_.add_annotations();
    anno->set_timestamp(timer::get_micros());
    anno->set_text_context(text);
    mu_.Unlock();
}

void Trace::AddKvAnnotation(const std::string& key, const std::string& value) {
    mu_.Lock();
    FunctionAnnotation* anno = span_.add_annotations();
    anno->set_timestamp(timer::get_micros());
    anno->mutable_kv_context()->set_key(key);
    anno->mutable_kv_context()->set_value(value);
    mu_.Unlock();
}

Trace::Trace(uint64_t trace_id, uint64_t parent_span_id, uint64_t span_id,
             const std::string& name, const std::string& trace_name) {
    span_.id.set_trace_id(trace_id);
    span_.id.set_parent_span_id(parent_span_id);
    span_.id.set_span_id(span_id);
    span_.set_span_name(name);
    span_.set_trace_name(trace_name);
    ref_.Set(0);
}

Trace::~Trace() {
    FlushLog();
}

Trace* Trace::TopThreadValue() {
    Trace* trace = (Trace*)pthread_getspecific(TraceModule::thread_key);
    if (trace == NULL) {
        std::cout << "WARNING: trace in thread local is nil\n";
    }
    return trace;
}

// step into thread
void Trace::PushThreadValue() {
    uint64_t tid = (uint64_t)pthread_self();
    void* prev_trace = pthread_getspecific(TraceModule::thread_key);
    pthread_setspecific(TraceModule::thread_key, (void*)this);

    mu_.Lock();
    std::map<uint64_t, std::stack<void*> >::iterator it = thread_value_.find(tid);
    if (it != thread_value_.end()) {
        std::stack<void*>& st = it->second;
        st.push(prev_trace);
    } else {
        std::stack<void*> st;
        st.push(pre_trace);
        thread_value_.insert(std::pair<uint64_t, std::stack<void*> >(tid, st));
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
    pthread_setspecific(TraceModule::thread_key, prev_trace_);
}

TraceIdentify Trace::GetTraceID() {
    return span_.id;
}

void Trace::MergeTrace(FunctionSpan span) {
    mu_.Lock();
    if ((id_.trace_id() == span.trace_id()) &&
       (id_.parent_span_id() == span.parent_span_id()) &&
       (id_.span_id() == span.span_id())) {
        for (int i = 0; i < span.annotations_size(); i++) {
            FunctionAnnotation* anno = id_.add_annotations();
            anno->CopyFrom(span.annotations(i));
        }
    }
    mu_.Unlock();
    return;
}

void Trace::TEST_PrintLog() {
    std::cout << "SPAN PRINTER: trace id " << span_.id.trace_id()
        << ", parent_span_id " << span_.id.parent_span_id()
        << ", span_id " << span_.id.span_id() << "\n\t\t";
    for (int i = 0; i < span_.annotations_size(); i++) {
        if (span_.annotations(i).has_text_context()) {
            std::cout << "TS: " << span_.annotations(i).timestamp()
                << ", TEXT: " << span_.annotations(i).text_context();
        }
        if (span_.annotations(i).has_kv_context()) {
            std::cout << "TS: " << span_.annotations(i).timestamp()
                << ", KV: " << span_.annotations(i).kv_context().key()
                << " = " << span_.annotations(i).kv_context().value();
        }
    }
}

}
