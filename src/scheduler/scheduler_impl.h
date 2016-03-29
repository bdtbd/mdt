#ifndef SCHEDULER_SCHEDULER_IMPL_H_
#define SCHEDULER_SCHEDULER_IMPL_H_

#include <google/protobuf/service.h>
#include <pthread.h>
#include <map>
#include <sofa/pbrpc/pbrpc.h>
#include "proto/scheduler.pb.h"
#include "utils/thread_pool.h"
#include "utils/counter.h"
#include "rpc/rpc_client.h"
#include "proto/agent.pb.h"
#include <boost/shared_ptr.hpp>
#include <galaxy.h>

namespace mdt {
namespace scheduler {

enum AgentState {
    AGENT_ACTIVE = 1,
    AGENT_INACTIVE = 2,
};

struct AgentInfo {
    //std::string agent_addr;
    // report by agent
    int64_t qps_use;
    int64_t qps_quota;
    int64_t bandwidth_use;
    int64_t bandwidth_quota;

    int64_t max_packet_size;
    int64_t min_packet_size;
    int64_t average_packet_size;

    int64_t error_nr;

    // manage by scheduler
    int64_t ctime;
    std::string collector_addr;
    AgentState state;
    Counter counter;
};

enum CollectorState {
    COLLECTOR_ACTIVE = 1,
    COLLECTOR_INACTIVE = 2,
};

struct CollectorInfo {
    //std::string collector_addr;
    // info report by collector
    int64_t qps;
    int64_t max_packet_size;
    int64_t min_packet_size;
    int64_t average_packet_size;

    int64_t store_pending;
    int64_t store_sched_ts;
    int64_t store_task_ts;
    int64_t store_task_num;

    // state info manage by scheduler
    int64_t nr_agents;
    int64_t ctime;
    int64_t error_nr;
    CollectorState state; // 1 = active, 2 = inactive
};

enum TraceState {
    ENABLE_TRACE = 1,
    DISABLE_TRACE = 2,
};

struct TraceInfo {
    std::string job_name;
    ::baidu::galaxy::Galaxy* galaxy;
    uint64_t flag;
    Counter ref;
    mdt::LogSchedulerService::RpcTraceGalaxyAppRequest configure;

    TraceInfo() : galaxy(NULL) {}
    ~TraceInfo() { }
};

class SchedulerImpl : public mdt::LogSchedulerService::LogSchedulerService {
public:
    SchedulerImpl();
    ~SchedulerImpl();

    // rpc service
    void Echo(::google::protobuf::RpcController* controller,
         const mdt::LogSchedulerService::EchoRequest* request,
         mdt::LogSchedulerService::EchoResponse* response,
         ::google::protobuf::Closure* done);

    void RegisterNode(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::RegisterNodeRequest* request,
                 mdt::LogSchedulerService::RegisterNodeResponse* response,
                 ::google::protobuf::Closure* done);
    void BgHandleCollectorInfo();
    void BgHandleAgentInfo();

    void GetNodeList(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::GetNodeListRequest* request,
                 mdt::LogSchedulerService::GetNodeListResponse* response,
                 ::google::protobuf::Closure* done);

    void RpcAddAgentWatchPath(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::RpcAddAgentWatchPathRequest* request,
                 mdt::LogSchedulerService::RpcAddAgentWatchPathResponse* response,
                 ::google::protobuf::Closure* done);

    void RpcAddWatchModuleStream(::google::protobuf::RpcController* controller,
                                 const mdt::LogSchedulerService::RpcAddWatchModuleStreamRequest* request,
                                 mdt::LogSchedulerService::RpcAddWatchModuleStreamResponse* response,
                                 ::google::protobuf::Closure* done);
    void RpcShowAgentInfo(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcShowAgentInfoRequest* request,
                          mdt::LogSchedulerService::RpcShowAgentInfoResponse* response,
                          ::google::protobuf::Closure* done);

    void RpcShowCollectorInfo(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcShowCollectorInfoRequest* request,
                          mdt::LogSchedulerService::RpcShowCollectorInfoResponse* response,
                          ::google::protobuf::Closure* done);
    void RpcTraceGalaxyApp(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcTraceGalaxyAppRequest* request,
                          mdt::LogSchedulerService::RpcTraceGalaxyAppResponse* response,
                          ::google::protobuf::Closure* done);

private:
    void AsyncTraceGalaxyAppCallback(const mdt::LogAgentService::RpcTraceGalaxyAppRequest* req,
                mdt::LogAgentService::RpcTraceGalaxyAppResponse* resp,
                bool failed, int error,
                mdt::LogAgentService::LogAgentService_Stub* service,
                boost::shared_ptr<TraceInfo> trace_info);
    void DoRpcTraceGalaxyApp(boost::shared_ptr<TraceInfo> trace_info);

    void DoRegisterNode(::google::protobuf::RpcController* controller,
                                       const mdt::LogSchedulerService::RegisterNodeRequest* request,
                                       mdt::LogSchedulerService::RegisterNodeResponse* response,
                                       ::google::protobuf::Closure* done);

    void DoUpdateAgentInfo(::google::protobuf::RpcController* controller,
                           const mdt::LogSchedulerService::GetNodeListRequest* request,
                           mdt::LogSchedulerService::GetNodeListResponse* response,
                           ::google::protobuf::Closure* done);
    void SelectAndUpdateCollector(AgentInfo info, std::string* select_server_addr);

    void DoRpcAddAgentWatchPath(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::RpcAddAgentWatchPathRequest* request,
                 mdt::LogSchedulerService::RpcAddAgentWatchPathResponse* response,
                 ::google::protobuf::Closure* done);

    void DoRpcAddWatchModuleStream(::google::protobuf::RpcController* controller,
                                   const mdt::LogSchedulerService::RpcAddWatchModuleStreamRequest* request,
                                   mdt::LogSchedulerService::RpcAddWatchModuleStreamResponse* response,
                                   ::google::protobuf::Closure* done);

    void DoRpcShowAgentInfo(::google::protobuf::RpcController* controller,
                      const mdt::LogSchedulerService::RpcShowAgentInfoRequest* request,
                      mdt::LogSchedulerService::RpcShowAgentInfoResponse* response,
                      ::google::protobuf::Closure* done);

    void DoRpcShowCollectorInfo(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcShowCollectorInfoRequest* request,
                          mdt::LogSchedulerService::RpcShowCollectorInfoResponse* response,
                          ::google::protobuf::Closure* done);
private:
    RpcClient* rpc_client_;

    ThreadPool agent_thread;
    pthread_spinlock_t agent_lock_;
    std::map<std::string, AgentInfo> agent_map_;
    ThreadPool agent_thread_;

    ThreadPool collector_thread_;
    pthread_spinlock_t collector_lock_;
    std::map<std::string, CollectorInfo> collector_map_;

    ThreadPool ctrl_thread_;

    pthread_t collector_tid_;
    volatile bool collector_thread_stop_;

    pthread_t agent_tid_;
    volatile bool agent_thread_stop_;

    // use for galaxy configure app trace path
    ThreadPool galaxy_trace_pool_;
    pthread_spinlock_t galaxy_trace_lock_;
    std::map<std::string, boost::shared_ptr<TraceInfo> > galaxy_trace_rule_;
};

}
}

#endif
