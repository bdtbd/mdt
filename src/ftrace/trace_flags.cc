#include <gflags/gflags.h>

DEFINE_uint64(max_text_annotation_size, 10240, "max single text annotation size");
DEFINE_uint64(log_level, 20, "log level");

DEFINE_string(mdt_server_addr, "0.0.0.0:12390", "mdt server addr");
