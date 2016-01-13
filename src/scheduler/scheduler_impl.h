#ifndef SCHEDULER_SCHEDULER_IMPL_H_
#define SCHEDULER_SCHEDULER_IMPL_H_

#include <google/protobuf/service.h>
#include <pthread.h>
#include <map>
#include <sofa/pbrpc/pbrpc.h>
#include "proto/scheduler.pb.h"

namespace mdt {
namespace scheduler {

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

    void GetNodeList(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::GetNodeListRequest* request,
                 mdt::LogSchedulerService::GetNodeListResponse* response,
                 ::google::protobuf::Closure* done);

private:
    pthread_spinlock_t lock_;
    uint64_t seq_no_;
    std::map<std::string, uint64_t> server_list_; // log dir notify
};

}
}

#endif
