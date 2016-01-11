#include <gflags/gflags.h>

DEFINE_int32(se_num_threads, 10, "num of thread handle search req");
DEFINE_string(se_service_port, "22221", "listen port for query service");
DEFINE_string(scheduler_addr, "0.0.0.0:11111", "scheduler service addr");
DEFINE_bool(mdt_flagfile_set, false, "force user set mdt.flag");
DEFINE_string(mdt_flagfile, "../conf/trace.flag", "search service flagfile");
