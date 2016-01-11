#include "scheduler/scheduler_impl.h"
#include <glog/logging.h>
#include <iostream>

namespace mdt {
namespace scheduler {

SchedulerImpl::SchedulerImpl()
    : seq_no_(0) {
    pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
}

SchedulerImpl::~SchedulerImpl() {
}

void SchedulerImpl::Echo(::google::protobuf::RpcController* controller,
                         const mdt::LogSchedulerService::EchoRequest* request,
                         mdt::LogSchedulerService::EchoResponse* response,
                         ::google::protobuf::Closure* done) {
    LOG(INFO) << "Echo: " << request->message();
    done->Run();
}

// new node's score = 0
void SchedulerImpl::RegisterNode(::google::protobuf::RpcController* controller,
                                 const mdt::LogSchedulerService::RegisterNodeRequest* request,
                                 mdt::LogSchedulerService::RegisterNodeResponse* response,
                                 ::google::protobuf::Closure* done) {
    pthread_spin_lock(&lock_);
    std::map<std::string, uint64_t>::iterator it = server_list_.find(request->server_addr());
    if (it == server_list_.end()) {
        server_list_[request->server_addr()] = 0;
    }
    pthread_spin_unlock(&lock_);
    done->Run();
}

void SchedulerImpl::GetNodeList(::google::protobuf::RpcController* controller,
                                const mdt::LogSchedulerService::GetNodeListRequest* request,
                                mdt::LogSchedulerService::GetNodeListResponse* response,
                                ::google::protobuf::Closure* done) {
    std::map<std::string, uint64_t> server_list;
    pthread_spin_lock(&lock_);
    server_list = server_list_;
    pthread_spin_unlock(&lock_);

    uint64_t smallest_score = std::numeric_limits<unsigned long>::max();
    std::string smallest_server_addr;
    std::map<std::string, uint64_t>::iterator it = server_list.begin();
    for (; it != server_list.end(); ++it) {
        std::string* server_addr = response->add_server_list();
        *server_addr = it->first;
        if (it->second < smallest_score) {
            smallest_score = it->second;
            smallest_server_addr = it->first;
        }
    }
    if (request->current_server_addr() != "nil") {
        response->set_primary_server_addr(request->current_server_addr());
    } else {
        // rebalance workload
        response->set_primary_server_addr(smallest_server_addr);

        pthread_spin_lock(&lock_);
        server_list_[smallest_server_addr] += 1;
        pthread_spin_unlock(&lock_);
    }
    done->Run();
}

}
}

