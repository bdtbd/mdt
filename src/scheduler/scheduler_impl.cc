#include "scheduler/scheduler_impl.h"
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <iostream>
#include "utils/timer.h"
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include "proto/agent.pb.h"

#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <galaxy.h>
#include "proto/galaxy_galaxy.pb.h"
#include <netdb.h>
#include <stdio.h>
#include <sys/socket.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>


DECLARE_string(scheduler_service_port);
DECLARE_int32(agent_timeout);
DECLARE_int32(agent_qps_quota);
DECLARE_int32(agent_bandwidth_quota);
DECLARE_int32(collector_timeout);
DECLARE_int32(collector_max_error);
DECLARE_int64(scheduler_galaxy_app_trace_period);

namespace mdt {
namespace scheduler {

void* BgHandleCollectorInfoWrapper(void* arg) {
    SchedulerImpl* scheduler = (SchedulerImpl*)arg;
    scheduler->BgHandleCollectorInfo();
    return NULL;
}

void* BgHandleAgentInfoWrapper(void* arg) {
    SchedulerImpl* scheduler = (SchedulerImpl*)arg;
    scheduler->BgHandleAgentInfo();
    return NULL;
}

SchedulerImpl::SchedulerImpl()
    : agent_thread_(50),
    collector_thread_(4),
    ctrl_thread_(10),
    galaxy_trace_pool_(30) {

    rpc_client_ = new RpcClient();

    //pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
    pthread_spin_init(&agent_lock_, PTHREAD_PROCESS_PRIVATE);
    pthread_spin_init(&collector_lock_, PTHREAD_PROCESS_PRIVATE);
    pthread_spin_init(&galaxy_trace_lock_, PTHREAD_PROCESS_PRIVATE);

    agent_thread_stop_ = false;
    pthread_create(&collector_tid_, NULL, BgHandleAgentInfoWrapper, this);
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
    done->Run(); }

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
    int64_t begin_ts = mdt::timer::get_micros();
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
        VLOG(50) << "new node register, " << request->server_addr();
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

    int64_t end_ts = mdt::timer::get_micros();
    VLOG(50) << "register collector, cost time " << end_ts - begin_ts;
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
        VLOG(30) << "bg handle collector cost time " << end_ts - start_ts;
        sleep(2);
    }
}

void SchedulerImpl::BgHandleAgentInfo() {
    while (1) {
        if (agent_thread_stop_) {
            break;
        }

        std::map<std::string, int64_t> collector_set;
        int64_t start_ts = mdt::timer::get_micros();
        pthread_spin_lock(&agent_lock_);
        std::map<std::string, AgentInfo>::iterator it = agent_map_.begin();
        for (; it != agent_map_.end(); ++it) {
            AgentInfo& info = it->second;
            int64_t ts = mdt::timer::get_micros();
            if ((info.state == AGENT_ACTIVE) &&
                (info.ctime + FLAGS_agent_timeout < ts)) {
                info.state = AGENT_INACTIVE;
                if (collector_set.find(info.collector_addr) == collector_set.end()) {
                    collector_set.insert(std::pair<std::string, int64_t>(info.collector_addr, 1));
                } else {
                    (collector_set[info.collector_addr])++;
                }
            }
        }
        pthread_spin_unlock(&agent_lock_);

        // update collector info
        std::map<std::string, int64_t>::iterator set_it = collector_set.begin();
        for (; set_it != collector_set.end(); ++set_it) {
            pthread_spin_lock(&collector_lock_);
            std::map<std::string, CollectorInfo>::iterator collector_map_it = collector_map_.find(set_it->first);
            if (collector_map_it != collector_map_.end()) {
                CollectorInfo& c_info = collector_map_it->second;
                if (c_info.nr_agents < set_it->second) {
                    c_info.nr_agents = 0;
                } else {
                    c_info.nr_agents -= set_it->second;
                }
            }
            pthread_spin_unlock(&collector_lock_);
        }

        int64_t end_ts = mdt::timer::get_micros();
        VLOG(30) << "bg handle agent cost time " << end_ts - start_ts;
        sleep(2);
    }
}

void SchedulerImpl::DoUpdateAgentInfo(::google::protobuf::RpcController* controller,
                                      const mdt::LogSchedulerService::GetNodeListRequest* request,
                                      mdt::LogSchedulerService::GetNodeListResponse* response,
                                      ::google::protobuf::Closure* done) {
    std::string select_server_addr;

    VLOG(50) << "agent " << request->agent_addr() << ", update info";
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
    VLOG(50) << "current agent's collector addr " << info.collector_addr << ", error_nr " << info.error_nr;
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

/////////////////////////////////////
//  galaxy app trace
/////////////////////////////////////
void SchedulerImpl::AsyncTraceGalaxyAppCallback(const mdt::LogAgentService::RpcTraceGalaxyAppRequest* req,
                mdt::LogAgentService::RpcTraceGalaxyAppResponse* resp,
                bool failed, int error,
                mdt::LogAgentService::LogAgentService_Stub* service,
                boost::shared_ptr<TraceInfo> trace_info) {
    // last one reschedule trace push
    if (trace_info->ref.Dec() == 0) {
        if (trace_info->flag == ENABLE_TRACE) {
            ThreadPool::Task task = boost::bind(&SchedulerImpl::DoRpcTraceGalaxyApp, this, trace_info);
            galaxy_trace_pool_.DelayTask(FLAGS_scheduler_galaxy_app_trace_period, task);
        } else {
            pthread_spin_lock(&galaxy_trace_lock_);
            galaxy_trace_rule_.erase(trace_info->job_name);
            pthread_spin_unlock(&galaxy_trace_lock_);
        }
    }

    delete req;
    delete resp;
    delete service;
}

void SchedulerImpl::DoRpcTraceGalaxyApp(boost::shared_ptr<TraceInfo> trace_info) {
    LOG(INFO) << ", Thread pool[PushTrace] " << galaxy_trace_pool_.ProfilingLog()
        << ", pending req(PushTrace) " << galaxy_trace_pool_.PendingNum();

    // connect galaxy master
    if (trace_info->galaxy == NULL) {
        std::string master_key = trace_info->configure.nexus_root_path() + trace_info->configure.master_path();
        ::baidu::galaxy::Galaxy* galaxy = ::baidu::galaxy::Galaxy::ConnectGalaxy(trace_info->configure.nexus_servers(), master_key);
        if (galaxy == NULL) {
            LOG(WARNING) << "galaxy connnect error, " << master_key << ", nexus servers " << trace_info->configure.nexus_servers();
            if (trace_info->flag == ENABLE_TRACE) {
                ThreadPool::Task task = boost::bind(&SchedulerImpl::DoRpcTraceGalaxyApp, this, trace_info);
                galaxy_trace_pool_.DelayTask(FLAGS_scheduler_galaxy_app_trace_period, task);
            } else {
                pthread_spin_lock(&galaxy_trace_lock_);
                galaxy_trace_rule_.erase(trace_info->job_name);
                pthread_spin_unlock(&galaxy_trace_lock_);
            }
            return;
        }
        trace_info->galaxy = galaxy;
    }

    std::vector<::baidu::galaxy::PodInformation> pods;
    if (!trace_info->galaxy->GetPodsByName(trace_info->job_name, &pods)) {
        LOG(WARNING) << "galaxy get Pods error, " << trace_info->job_name;
        if (trace_info->flag == ENABLE_TRACE) {
            ThreadPool::Task task = boost::bind(&SchedulerImpl::DoRpcTraceGalaxyApp, this, trace_info);
            galaxy_trace_pool_.DelayTask(FLAGS_scheduler_galaxy_app_trace_period, task);
        } else {
            pthread_spin_lock(&galaxy_trace_lock_);
            galaxy_trace_rule_.erase(trace_info->job_name);
            pthread_spin_unlock(&galaxy_trace_lock_);
        }
        return;
    }

    trace_info->ref.Inc();
    for (uint32_t i = 0; i < pods.size(); i++) {
        ::baidu::galaxy::PodInformation& pod = pods[i];
        VLOG(30) << "podid " << pod.podid << ", endpoint " << pod.endpoint << ", jobid " << pod.jobid;

        // translate ip to hostname
        std::vector<std::string> addr_vec;
        boost::split(addr_vec, pod.endpoint, boost::is_any_of(":"));
        struct in_addr net_addr;
        inet_aton(addr_vec[0].c_str(), &net_addr);
        struct hostent* ent = gethostbyaddr(&net_addr, sizeof(net_addr), AF_INET);
        if (ent == NULL) {
            continue;
        }
        std::string hostname(ent->h_name);
        hostname.append(":");
        VLOG(30) << " endpoint hostname " << hostname;

        // match agent addr
        std::string hname;
        pthread_spin_lock(&agent_lock_);
        std::map<std::string, AgentInfo>::iterator uper = agent_map_.upper_bound(hostname);
        for (; uper != agent_map_.end(); ++uper) {
            hname = uper->first;
            if (hname.find(hostname) != std::string::npos) {
                break;
            }
            hname.clear();
        }
        pthread_spin_unlock(&agent_lock_);
        if (hname.size() == 0) {
            continue;
        }


        trace_info->ref.Inc();
        mdt::LogAgentService::LogAgentService_Stub* service;
        rpc_client_->GetMethodList(hname, &service);
        mdt::LogAgentService::RpcTraceGalaxyAppRequest* req = new mdt::LogAgentService::RpcTraceGalaxyAppRequest();
        mdt::LogAgentService::RpcTraceGalaxyAppResponse* resp = new mdt::LogAgentService::RpcTraceGalaxyAppResponse();
        req->set_pod_id(pod.podid);
        req->set_work_dir(trace_info->configure.work_dir());
        req->set_user_log_dir(trace_info->configure.user_log_dir());
        req->set_db_name(trace_info->configure.db_name());
        req->set_table_name(trace_info->configure.table_name());
        req->set_parse_path_fn(trace_info->configure.parse_path_fn());
        VLOG(30) << "begin push trace info: " << hname << ", req " << req->DebugString();

        boost::function<void (const mdt::LogAgentService::RpcTraceGalaxyAppRequest*,
                              mdt::LogAgentService::RpcTraceGalaxyAppResponse*,
                              bool, int)> callback =
            boost::bind(&SchedulerImpl::AsyncTraceGalaxyAppCallback,
                        this, _1, _2, _3, _4, service, trace_info);
        rpc_client_->AsyncCall(service,
                              &mdt::LogAgentService::LogAgentService_Stub::RpcTraceGalaxyApp,
                              req, resp, callback);
    }

    // last one reschedule trace push
    if (trace_info->ref.Dec() == 0) {
        if (trace_info->flag == ENABLE_TRACE) {
            ThreadPool::Task task = boost::bind(&SchedulerImpl::DoRpcTraceGalaxyApp, this, trace_info);
            galaxy_trace_pool_.DelayTask(FLAGS_scheduler_galaxy_app_trace_period, task);
        } else {
            pthread_spin_lock(&galaxy_trace_lock_);
            galaxy_trace_rule_.erase(trace_info->job_name);
            pthread_spin_unlock(&galaxy_trace_lock_);
        }
    }
}

void SchedulerImpl::RpcTraceGalaxyApp(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::RpcTraceGalaxyAppRequest* request,
                 mdt::LogSchedulerService::RpcTraceGalaxyAppResponse* response,
                 ::google::protobuf::Closure* done) {
    boost::shared_ptr<TraceInfo> trace_info(new TraceInfo());
    bool need_queue_task = false;
    pthread_spin_lock(&galaxy_trace_lock_);
    if (galaxy_trace_rule_.find(request->job_name()) == galaxy_trace_rule_.end()) {
        trace_info->configure.CopyFrom(*request);
        trace_info->flag = ENABLE_TRACE;
        trace_info->job_name = request->job_name();
        galaxy_trace_rule_[request->job_name()] = trace_info;
        need_queue_task = true;
    }
    pthread_spin_unlock(&galaxy_trace_lock_);

    if (need_queue_task) {
        VLOG(50) << "queue trace app request: " << trace_info->job_name << ", configure: " << trace_info->configure.DebugString();
        ThreadPool::Task task = boost::bind(&SchedulerImpl::DoRpcTraceGalaxyApp, this, trace_info);
        galaxy_trace_pool_.AddTask(task);
    }
    done->Run();
    return;
}

// query agent info
void SchedulerImpl::RpcShowAgentInfo(::google::protobuf::RpcController* controller,
                      const mdt::LogSchedulerService::RpcShowAgentInfoRequest* request,
                      mdt::LogSchedulerService::RpcShowAgentInfoResponse* response,
                      ::google::protobuf::Closure* done) {
    int64_t begin_ts = mdt::timer::get_micros();
    ThreadPool::Task task = boost::bind(&SchedulerImpl::DoRpcShowAgentInfo, this, controller, request, response, done);
    ctrl_thread_.AddTask(task);
    int64_t end_ts = mdt::timer::get_micros();
    VLOG(20) << "ShowAgentInfo, add task, cost time " << end_ts - begin_ts;
}

void SchedulerImpl::DoRpcShowAgentInfo(::google::protobuf::RpcController* controller,
                      const mdt::LogSchedulerService::RpcShowAgentInfoRequest* request,
                      mdt::LogSchedulerService::RpcShowAgentInfoResponse* response,
                      ::google::protobuf::Closure* done) {
    int64_t begin_ts = mdt::timer::get_micros();
    pthread_spin_lock(&agent_lock_);
    std::map<std::string, AgentInfo>::iterator it = agent_map_.begin();
    for (; it != agent_map_.end(); ++it) {
        AgentInfo& info = it->second;
        if (info.state != AGENT_ACTIVE) {
            continue;
        }

        mdt::LogSchedulerService::AgentInformation* agent_info = response->add_info();
        agent_info->set_agent_addr(it->first);
        agent_info->set_ctime(info.ctime);
        agent_info->set_collector_addr(info.collector_addr);
        agent_info->set_agent_state(info.state);

        mdt::LogSchedulerService::AgentInfo* information = agent_info->mutable_agent_info();
        information->set_qps_quota(info.qps_quota);
        information->set_qps_use(info.qps_use);
        information->set_bandwidth_quota(info.bandwidth_quota);
        information->set_bandwidth_use(info.bandwidth_use);
        information->set_max_packet_size(info.max_packet_size);
        information->set_min_packet_size(info.min_packet_size);
        information->set_average_packet_size(info.average_packet_size);
        information->set_error_nr(info.error_nr);
    }
    pthread_spin_unlock(&agent_lock_);
    done->Run();
    int64_t end_ts = mdt::timer::get_micros();
    VLOG(20) << "ShowAgentInfo, response agent info, cost time " << end_ts - begin_ts;
}

// query collector info
void SchedulerImpl::RpcShowCollectorInfo(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcShowCollectorInfoRequest* request,
                          mdt::LogSchedulerService::RpcShowCollectorInfoResponse* response,
                          ::google::protobuf::Closure* done) {
    int64_t begin_ts = mdt::timer::get_micros();
    ThreadPool::Task task = boost::bind(&SchedulerImpl::DoRpcShowCollectorInfo, this, controller, request, response, done);
    ctrl_thread_.AddTask(task);
    int64_t end_ts = mdt::timer::get_micros();
    VLOG(20) << "ShowCollectorInfo, add task, cost time " << end_ts - begin_ts;
}

void SchedulerImpl::DoRpcShowCollectorInfo(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcShowCollectorInfoRequest* request,
                          mdt::LogSchedulerService::RpcShowCollectorInfoResponse* response,
                          ::google::protobuf::Closure* done) {
    int64_t begin_ts = mdt::timer::get_micros();
    pthread_spin_lock(&collector_lock_);
    std::map<std::string, CollectorInfo>::iterator it = collector_map_.begin();
    for (; it != collector_map_.end(); ++it) {
        CollectorInfo& info = it->second;
        if (info.state != COLLECTOR_ACTIVE) {
            continue;
        }
        mdt::LogSchedulerService::CollectorInformation* collector_info = response->add_info();
        collector_info->set_collector_addr(it->first);
        collector_info->set_nr_agents(info.nr_agents);
        collector_info->set_ctime(info.ctime);
        collector_info->set_collector_state(info.state);
        collector_info->set_error_nr(info.error_nr);

        mdt::LogSchedulerService::CollectorInfo* information = collector_info->mutable_collector_info();
        information->set_qps(info.qps);
        information->set_max_packet_size(info.max_packet_size);
        information->set_min_packet_size(info.min_packet_size);
        information->set_average_packet_size(info.average_packet_size);
    }
    pthread_spin_unlock(&collector_lock_);
    done->Run();

    int64_t end_ts = mdt::timer::get_micros();
    VLOG(20) << "query collector info, cost time " << end_ts - begin_ts;
}

}
}

