#include "scheduler/scheduler_impl.h"
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <iostream>
#include "util/timer.h"
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include "proto/agent.pb.h"

DECLARE_string(scheduler_service_port);
DECLARE_int32(agent_timeout);
DECLARE_int32(agent_qps_quota);
DECLARE_int32(agent_bandwidth_quota);
DECLARE_int32(collector_timeout);
DECLARE_int32(collector_max_error);

namespace mdt {
namespace scheduler {

void* BgHandleCollectorInfoWrapper(void* arg) {
    SchedulerImpl* scheduler = (SchedulerImpl*)arg;
    scheduler->BgHandleCollectorInfo();
    return NULL;
}

SchedulerImpl::SchedulerImpl()
    : agent_thread_(40),
    collector_thread_(4) {

    rpc_client_ = new RpcClient();

    //pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
    pthread_spin_init(&agent_lock_, PTHREAD_PROCESS_PRIVATE);
    pthread_spin_init(&collector_lock_, PTHREAD_PROCESS_PRIVATE);

    collector_thread_stop_ = false;
    pthread_create(&collector_tid_, NULL, BgHandleCollectorInfoWrapper, this);
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
    ThreadPool::Task task = boost::bind(&SchedulerImpl::DoRegisterNode, this, controller, request, response, done);
    collector_thread_.AddTask(task);
    return;
}

void SchedulerImpl::DoRegisterNode(::google::protobuf::RpcController* controller,
                                 const mdt::LogSchedulerService::RegisterNodeRequest* request,
                                 mdt::LogSchedulerService::RegisterNodeResponse* response,
                                 ::google::protobuf::Closure* done) {
    pthread_spin_lock(&collector_lock_);
    std::map<std::string, CollectorInfo>::iterator it = collector_map_.find(request->server_addr());
    if (it == collector_map_.end()) {
        // new collector, insert into manage list
        CollectorInfo info;
        info.qps = request->info().qps();
        info.max_packet_size = request->info().max_packet_size();
        info.min_packet_size = request->info().min_packet_size();
        info.average_packet_size = request->info().average_packet_size();

        info.nr_agents = 0;
        info.error_nr = 0;
        info.state = COLLECTOR_ACTIVE;
        info.ctime = mdt::timer::get_micros();

        collector_map_.insert(std::pair<std::string, CollectorInfo>(request->server_addr(), info));
        VLOG(30) << "new node register, " << request->server_addr();
    } else {
        // update collector info
        CollectorInfo& info = it->second;
        info.qps = request->info().qps();
        info.max_packet_size = request->info().max_packet_size();
        info.min_packet_size = request->info().min_packet_size();
        info.average_packet_size = request->info().average_packet_size();

        info.error_nr = 0;
        info.state = COLLECTOR_ACTIVE;
        info.ctime = mdt::timer::get_micros();
    }
    pthread_spin_unlock(&collector_lock_);

    response->set_error_code(0);
    done->Run();
}

// try to set collector to inactive state
void SchedulerImpl::BgHandleCollectorInfo() {
    while (1) {
        if (collector_thread_stop_) {
            break;
        }

        // TODO: some bug casue collector no found
        int64_t start_ts = mdt::timer::get_micros();
        pthread_spin_lock(&collector_lock_);
        std::map<std::string, CollectorInfo>::iterator collector_it = collector_map_.begin();
        for (; collector_it != collector_map_.end(); ++collector_it) {
            CollectorInfo& info = collector_it->second;
            int64_t ts = mdt::timer::get_micros();
            if ((info.state == COLLECTOR_ACTIVE) &&
                    ((info.ctime + FLAGS_collector_timeout < ts) ||
                     (info.error_nr > FLAGS_collector_max_error))) {
                info.state = COLLECTOR_INACTIVE;
            }
        }
        pthread_spin_unlock(&collector_lock_);
        int64_t end_ts = mdt::timer::get_micros();
        VLOG(30) << "bg handle cost time " << end_ts - start_ts;
        sleep(5);
    }
}

void SchedulerImpl::DoUpdateAgentInfo(::google::protobuf::RpcController* controller,
                                      const mdt::LogSchedulerService::GetNodeListRequest* request,
                                      mdt::LogSchedulerService::GetNodeListResponse* response,
                                      ::google::protobuf::Closure* done) {
    std::string select_server_addr;

    VLOG(30) << "agent " << request->agent_addr() << ", update info";
    pthread_spin_lock(&agent_lock_);
    std::map<std::string, AgentInfo>::iterator it = agent_map_.find(request->agent_addr());
    if (it == agent_map_.end()) {
        AgentInfo info;
        info.qps_quota = FLAGS_agent_qps_quota;
        info.qps_use = 0;
        info.bandwidth_quota = FLAGS_agent_bandwidth_quota;
        info.bandwidth_use = 0;

        info.max_packet_size = 0;
        info.min_packet_size = 0;
        info.average_packet_size = 0;
        info.error_nr = 0;

        info.ctime = mdt::timer::get_micros();
        info.state = AGENT_ACTIVE;
        info.collector_addr = request->current_server_addr();
        info.counter.Set(1);
        pthread_spin_unlock(&agent_lock_);

        // select collector for agent
        SelectAndUpdateCollector(info, &select_server_addr);

        pthread_spin_lock(&agent_lock_);
        info.counter.Dec();
        info.collector_addr = select_server_addr;
        agent_map_.insert(std::pair<std::string, AgentInfo>(request->agent_addr(), info));
    } else {
        AgentInfo& info = it->second;
        info.qps_quota = FLAGS_agent_qps_quota;
        info.qps_use = request->info().qps_use();
        info.bandwidth_quota = FLAGS_agent_bandwidth_quota;
        info.bandwidth_use = request->info().bandwidth_use();

        info.max_packet_size = request->info().max_packet_size();
        info.min_packet_size = request->info().min_packet_size();
        info.average_packet_size = request->info().average_packet_size();
        info.error_nr = request->info().error_nr();

        info.ctime = mdt::timer::get_micros();
        info.state = AGENT_ACTIVE;
        info.collector_addr = request->current_server_addr();
        info.counter.Inc();
        pthread_spin_unlock(&agent_lock_);

        // select collector for agent
        SelectAndUpdateCollector(info, &select_server_addr);

        pthread_spin_lock(&agent_lock_);
        info.counter.Dec();
        info.collector_addr = select_server_addr;
    }
    pthread_spin_unlock(&agent_lock_);

    response->set_primary_server_addr(select_server_addr);
    done->Run();
}

void SchedulerImpl::SelectAndUpdateCollector(AgentInfo info, std::string* select_server_addr) {
    int64_t min_nr_agent = INT64_MAX;
    VLOG(30) << "current agent's collector addr " << info.collector_addr << ", error_nr " << info.error_nr;
    *select_server_addr = info.collector_addr;

    std::map<std::string, CollectorInfo>::iterator collector_it, min_it;
    pthread_spin_lock(&collector_lock_);
    if (info.collector_addr.size() && info.collector_addr != "nil") {
        collector_it = collector_map_.find(info.collector_addr);
        if (collector_it != collector_map_.end()) {
            CollectorInfo& collector_info = collector_it->second;
            collector_info.error_nr += info.error_nr;
            int64_t ts = mdt::timer::get_micros();
            if ((collector_info.state == COLLECTOR_ACTIVE) &&
               ((collector_info.ctime + FLAGS_collector_timeout < ts) ||
               (collector_info.error_nr <= FLAGS_collector_max_error))) {
                *select_server_addr = collector_it->first;

                pthread_spin_unlock(&collector_lock_);
                return;
            }
        }
    }

    // select another collector
    collector_it = collector_map_.begin();
    for (; collector_it != collector_map_.end(); ++collector_it) {
        CollectorInfo& collector_info = collector_it->second;
        int64_t ts = mdt::timer::get_micros();
        VLOG(30) << "select new collector, ctime " << collector_info.ctime << ", ts " << ts
            << ", addr " << collector_it->first
            << ", nr_agents " << collector_info.nr_agents;
        if ((collector_info.state == COLLECTOR_ACTIVE) &&
                ((collector_info.ctime + FLAGS_collector_timeout < ts) ||
                 (collector_info.error_nr <= FLAGS_collector_max_error))) {
            if (min_nr_agent > collector_info.nr_agents) {
                min_nr_agent = collector_info.nr_agents;
                min_it = collector_it;
                *select_server_addr = collector_it->first;
            }
        }
    }
    if (min_nr_agent != INT64_MAX) {
        CollectorInfo& min_info = min_it->second;
        min_info.nr_agents++;
    }
    pthread_spin_unlock(&collector_lock_);
}

void SchedulerImpl::GetNodeList(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::GetNodeListRequest* request,
                 mdt::LogSchedulerService::GetNodeListResponse* response,
                 ::google::protobuf::Closure* done) {
    ThreadPool::Task task = boost::bind(&SchedulerImpl::DoUpdateAgentInfo, this, controller, request, response, done);
    agent_thread_.AddTask(task);
    return;
}

void SchedulerImpl::DoRpcAddAgentWatchPath(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::RpcAddAgentWatchPathRequest* request,
                 mdt::LogSchedulerService::RpcAddAgentWatchPathResponse* response,
                 ::google::protobuf::Closure* done) {
    mdt::LogAgentService::LogAgentService_Stub* service;
    rpc_client_->GetMethodList(request->agent_addr(), &service);
    mdt::LogAgentService::RpcAddWatchPathRequest* req = new mdt::LogAgentService::RpcAddWatchPathRequest();
    mdt::LogAgentService::RpcAddWatchPathResponse* resp = new mdt::LogAgentService::RpcAddWatchPathResponse();
    req->set_watch_path(request->watch_path());

    rpc_client_->SyncCall(service, &mdt::LogAgentService::LogAgentService_Stub::RpcAddWatchPath, req, resp);
    if (resp->status() == mdt::LogAgentService::kRpcOk) {
        response->set_status(mdt::LogSchedulerService::kRpcOk);
    } else {
        response->set_status(mdt::LogSchedulerService::kRpcError);
    }

    delete req;
    delete resp;
    delete service;

    done->Run();
}

void SchedulerImpl::RpcAddAgentWatchPath(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::RpcAddAgentWatchPathRequest* request,
                 mdt::LogSchedulerService::RpcAddAgentWatchPathResponse* response,
                 ::google::protobuf::Closure* done) {
    ThreadPool::Task task = boost::bind(&SchedulerImpl::DoRpcAddAgentWatchPath, this, controller, request, response, done);
    agent_thread_.AddTask(task);
    return;
}

void SchedulerImpl::DoRpcAddWatchModuleStream(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::RpcAddWatchModuleStreamRequest* request,
                 mdt::LogSchedulerService::RpcAddWatchModuleStreamResponse* response,
                 ::google::protobuf::Closure* done) {
    mdt::LogAgentService::LogAgentService_Stub* service;
    rpc_client_->GetMethodList(request->agent_addr(), &service);
    mdt::LogAgentService::RpcAddWatchModuleStreamRequest* req = new mdt::LogAgentService::RpcAddWatchModuleStreamRequest();
    mdt::LogAgentService::RpcAddWatchModuleStreamResponse* resp = new mdt::LogAgentService::RpcAddWatchModuleStreamResponse();
    req->set_production_name(request->production_name());
    req->set_log_name(request->log_name());

    rpc_client_->SyncCall(service, &mdt::LogAgentService::LogAgentService_Stub::RpcAddWatchModuleStream, req, resp);
    if (resp->status() == mdt::LogAgentService::kRpcOk) {
        response->set_status(mdt::LogSchedulerService::kRpcOk);
    } else {
        response->set_status(mdt::LogSchedulerService::kRpcError);
    }

    delete req;
    delete resp;
    delete service;

    done->Run();
}

void SchedulerImpl::RpcAddWatchModuleStream(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::RpcAddWatchModuleStreamRequest* request,
                 mdt::LogSchedulerService::RpcAddWatchModuleStreamResponse* response,
                 ::google::protobuf::Closure* done) {
    ThreadPool::Task task = boost::bind(&SchedulerImpl::DoRpcAddWatchModuleStream, this, controller, request, response, done);
    agent_thread_.AddTask(task);
    return;
}

}
}

