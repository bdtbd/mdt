#include <gflags/gflags.h>

DEFINE_string(mdt_flagfile, "../conf/trace.flag", "mdt flag file, use for dynamic update param");
DEFINE_uint64(mdt_max_text_annotation_size, 4096, "max single text annotation size");
DEFINE_uint64(mdt_log_level, 10000, "log level");
DEFINE_string(mdt_server_addr, "0.0.0.0:12390", "mdt server addr");
DEFINE_bool(mdt_trace_debug_enable, false, "dump log into stdout");
