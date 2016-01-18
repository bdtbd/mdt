#include <gflags/gflags.h>

DEFINE_string(scheduler_service_port, "11111", "scheduler service port");
DEFINE_int32(agent_timeout, 60, "agent info will be delete after x second");
DEFINE_int32(agent_qps_quota, 10000, "max qps agent can be use per second");
DEFINE_int32(agent_bandwidth_quota, 20000000, "max bandwidth agent can be use per second");

DEFINE_int32(collector_timeout, 60000000, "collector info will be delete after x us");
DEFINE_int32(collector_max_error, 10, "max error can occur in collector");



