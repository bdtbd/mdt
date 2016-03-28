#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <map>
#include <iostream>
#include <string>
#include <glog/logging.h>
#include <sofa/pbrpc/pbrpc.h>
#include <sys/inotify.h>
#include <unistd.h>
#include "agent/agent_impl.h"
#include "agent/options.h"
#include "proto/agent.pb.h"
#include "agent/log_stream.h"
#include <errno.h>
#include <sys/select.h>
#include "proto/scheduler.pb.h"
#include <dirent.h>
#include <sys/types.h>

DECLARE_string(scheduler_addr);
DECLARE_string(db_dir);
DECLARE_string(watch_log_dir);
DECLARE_string(module_name_list);
DECLARE_string(agent_service_port);

extern mdt::agent::EventMask event_masks[21];

namespace mdt {
namespace agent {

void* SchedulerThread(void* arg) {
    AgentImpl* agent = (AgentImpl*)arg;
    agent->GetServerAddr();
    return NULL;
}

AgentImpl::AgentImpl() {
    pthread_spin_init(&lock_, PTHREAD_PROCESS_SHARED);
    pthread_spin_init(&server_lock_, PTHREAD_PROCESS_SHARED);
    rpc_client_ = new RpcClient();

    info_.qps_use= 0;
    info_.qps_quota= 0;
    info_.bandwidth_use= 0;
    info_.bandwidth_quota= 0;
    info_.max_packet_size= 0;
    info_.min_packet_size= 0;
    info_.average_packet_size= 0;
    info_.error_nr = 0;
    info_.collector_addr = "nil";
    //server_addr_ = "nil";

    stop_scheduler_thread_ = false;
    pthread_create(&scheduler_tid_, NULL, SchedulerThread, this);
}

AgentImpl::~AgentImpl() {

}

void AgentImpl::GetServerAddrCallback(const mdt::LogSchedulerService::GetNodeListRequest* req,
                                      mdt::LogSchedulerService::GetNodeListResponse* resp,
                                      bool failed, int error,
                                      mdt::LogSchedulerService::LogSchedulerService_Stub* service) {
    if (!failed) {
        pthread_spin_lock(&server_lock_);
        info_.collector_addr = resp->primary_server_addr();
        VLOG(50) << "agent, collector addr " << info_.collector_addr;

        info_.qps_use= 0;
        info_.qps_quota= 0;
        info_.bandwidth_use= 0;
        info_.bandwidth_quota= 0;
        info_.max_packet_size= 0;
        info_.min_packet_size= 0;
        info_.average_packet_size= 0;
        info_.error_nr = 0;

        pthread_spin_unlock(&server_lock_);
    }
    delete req;
    delete resp;
    delete service;
    server_addr_event_.Set();
}

void AgentImpl::GetServerAddr() {
    char hostname[255];
    if (0 != gethostname(hostname, 256)) {
        LOG(FATAL) << "fail to report message";
    }
    std::string hostname_str = hostname;

    while (1) {
        if (stop_scheduler_thread_) {
            return;
        }
        std::string agent_addr = hostname_str + ":" + FLAGS_agent_service_port;
        std::string scheduler_addr = FLAGS_scheduler_addr;

        mdt::LogSchedulerService::LogSchedulerService_Stub* service;
        rpc_client_->GetMethodList(scheduler_addr, &service);
        mdt::LogSchedulerService::GetNodeListRequest* req = new mdt::LogSchedulerService::GetNodeListRequest();
        mdt::LogSchedulerService::GetNodeListResponse* resp = new mdt::LogSchedulerService::GetNodeListResponse();
        // set up agent info request
        pthread_spin_lock(&server_lock_);
        req->set_agent_addr(agent_addr);
        req->set_current_server_addr(info_.collector_addr);

        mdt::LogSchedulerService::AgentInfo* info = req->mutable_info();
        info->set_qps_quota(info_.qps_quota);
        info->set_qps_use(info_.qps_use);
        info->set_bandwidth_use(info_.bandwidth_use);
        info->set_bandwidth_quota(info_.bandwidth_quota);
        info->set_max_packet_size(info_.max_packet_size);
        info->set_min_packet_size(info_.min_packet_size);
        info->set_average_packet_size(info_.average_packet_size);
        info->set_error_nr(info_.error_nr);

        pthread_spin_unlock(&server_lock_);

        boost::function<void (const mdt::LogSchedulerService::GetNodeListRequest*,
                              mdt::LogSchedulerService::GetNodeListResponse*,
                              bool, int)> callback =
                boost::bind(&AgentImpl::GetServerAddrCallback,
                            this, _1, _2, _3, _4, service);
        rpc_client_->AsyncCall(service, &mdt::LogSchedulerService::LogSchedulerService_Stub::GetNodeList,
                               req, resp, callback);
        server_addr_event_.Wait();
        sleep(10);
    }
}

int AgentImpl::Init() {
    // open leveldb
    log_options_.db_type = DISKDB;
    log_options_.db_dir = FLAGS_db_dir;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, log_options_.db_dir.c_str(), &log_options_.db);
    if (!status.ok()) {
        return -1;
    }

    // parse log dir
    std::vector<std::string> log_vec;
    ParseLogDir(log_vec);
    // add watch event
    for (uint32_t i = 0; i < log_vec.size(); i++) {
        AddWatchPath(log_vec[i]);
    }

    return 0;
}

// parse log dir, watch_log_dir=/root/xxx1/;/root/xxx2;/tmp/xxx3
void AgentImpl::ParseLogDir(std::vector<std::string>& log_vec) {
    std::vector<std::string> tmp_vec;
    boost::split(tmp_vec, FLAGS_watch_log_dir, boost::is_any_of(";,: "));

    for (uint32_t i = 0; i < tmp_vec.size(); i++) {
        if (access(tmp_vec[i].c_str(), F_OK) == 0) {
            log_vec.push_back(tmp_vec[i]);
            VLOG(30) << "watch log dir " << tmp_vec[i];
        }
    }
    return;
}

void* WatchThreadWrapper(void* arg) {
    FileSystemInotify* fs_inotify = (FileSystemInotify*)arg;
    fs_inotify->agent->WatchLogDir(fs_inotify);
    return NULL;
}

int AgentImpl::WaitInotifyFDReadable(int fd) {
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);
    return select(FD_SETSIZE, &rfds, NULL, NULL, NULL);
}

int AgentImpl::FreadEvent(void* dest, size_t size, FILE* file) {
    char* buf = (char*)dest;
    while (size) {
        int n = fread(buf, 1, size, file);
        if (n == 0) {
            return -1;
        }
        size -= n;
        buf += n;
    }
    return 0;
}

void AgentImpl::WatchLogDir(FileSystemInotify* fs_inotify) {
    while (1) {
        VLOG(35) << "step into watch dir phase, dir " << fs_inotify->log_dir;
        if (fs_inotify->stop) {
            break;
        }

        inotify_event event;
        char filename[256];

        if (FreadEvent(&event, sizeof(event), fs_inotify->inotify_FD) == -1) {
            LOG(WARNING) << "inotify FD " << fs_inotify->log_dir << ", read failed";
            sleep(1);
            continue;
        }
        if (event.len) {
            FreadEvent(filename, event.len, fs_inotify->inotify_FD);
        }

        /*
        if (WaitInotifyFDReadable(fs_inotify->inotify_fd) < 0) {
            VLOG(30) << "inotify fd " << fs_inotify->inotify_fd << ", select failed";
            continue;
        }
        int length = read(fs_inotify->inotify_fd, &event, sizeof(event));
        if (length < 0) {
            LOG(WARNING) << "read event: " << fs_inotify->log_dir << ", errno " << errno;
            continue;
        }

        int namelen = 0;
        if (event.len) {
            namelen = read(fs_inotify->inotify_fd, filename, event.len);
            if (namelen < 0) {
                LOG(WARNING) << "read event file name: " << fs_inotify->log_dir << ", fail";
                continue;
            }
        }
        */
        for (uint32_t i = 0; i < 21; ++i) {
            if (event.mask & event_masks[i].flag) {
                VLOG(35) << "file " << filename << " has event: " << event_masks[i].name;
            }
        }
        // parse event
        if (event.mask & (IN_CREATE | IN_MOVED_TO)) {
            AddWriteEvent(fs_inotify->log_dir, filename, &event);
        //} else if (event.mask & (IN_DELETE | IN_DELETE_SELF | IN_MOVE_SELF| IN_MOVED_FROM | IN_CLOSE_WRITE)) {
        } else if (event.mask & (IN_DELETE | IN_DELETE_SELF | IN_MOVE_SELF| IN_MOVED_FROM)) {
            DeleteWatchEvent(fs_inotify->log_dir, filename, &event);
        } else if (event.mask & (IN_MODIFY)) {
            AddWriteEvent(fs_inotify->log_dir, filename, &event);
        } else {

        }
    }
}

// module_name_list=galaxy.INFO.;rtg.INFO.;tabletnode.INFO.;mdt.INFO.
void AgentImpl::ParseModuleName(const std::string& filename, std::string* module_name) {
    std::vector<std::string> tmp_vec;
    boost::split(tmp_vec, FLAGS_module_name_list, boost::is_any_of(";,: "));

    for (uint32_t i = 0; i < tmp_vec.size(); i++) {
        std::string& tmpname = tmp_vec[i];
        if ((filename.size() >= tmpname.size()) &&
            (filename.substr(0, tmpname.size()) == tmpname)) {
            *module_name = tmpname;
            return;
        }
    }
    if (module_name->size() == 0) {
        *module_name = "all";
    }
    return;
}

int AgentImpl::FilterFileByMoudle(const std::string& filename, std::string* expect_module_name) {
    pthread_spin_lock(&lock_);
    std::map<std::string, std::string>::iterator it = module_file_set_.begin();
    for (; it != module_file_set_.end(); ++it) {
        const std::string& module_file_name = it->first;
        const std::string& module_name = it->second;
        if (filename.find(module_file_name) != std::string::npos) {
            *expect_module_name = module_name;
            break;
        }
    }
    pthread_spin_unlock(&lock_);
    return 0;
}

int AgentImpl::AddWriteEvent(const std::string& logdir, const std::string& filename, inotify_event* event) {
    std::string module_name;
    //ParseModuleName(filename, &module_name);
    FilterFileByMoudle(filename, &module_name);
    VLOG(35) << "write event, module name " << module_name << ", log dir " << logdir;
    if (module_name.size() == 0) {
        VLOG(35) << "dir " << filename << ", no module match";
        return -1;
    }

    LogStream* stream = NULL;
    pthread_spin_lock(&lock_);
    std::map<std::string, LogStream*>::iterator it = log_streams_.find(module_name);
    if (it != log_streams_.end()) {
        stream = log_streams_[module_name];
    } else {
        stream = new LogStream(module_name, log_options_, rpc_client_, &server_lock_, &info_);
        log_streams_[module_name] = stream;
    }
    pthread_spin_unlock(&lock_);

    stream->AddWriteEvent(logdir + "/" + filename);
    return 0;
}

int AgentImpl::DeleteWatchEvent(const std::string& logdir, const std::string& filename, inotify_event* event) {
    std::string module_name;
    //ParseModuleName(filename, &module_name);
    FilterFileByMoudle(filename, &module_name);
    VLOG(35) << "delete event, module name " << module_name << ", log dir " << logdir;
    if (module_name.size() == 0) {
        VLOG(35) << "dir " << filename << ", no module match";
        return -1;
    }

    LogStream* stream = NULL;
    pthread_spin_lock(&lock_);
    std::map<std::string, LogStream*>::iterator it = log_streams_.find(module_name);
    if (it != log_streams_.end()) {
        stream = log_streams_[module_name];
    } else {
        stream = new LogStream(module_name, log_options_, rpc_client_, &server_lock_, &info_);
        log_streams_[module_name] = stream;
    }
    pthread_spin_unlock(&lock_);

    stream->DeleteWatchEvent(logdir + "/" + filename, true);
    return 0;
}

void AgentImpl::DestroyWatchPath(FileSystemInotify* fs_inotify) {
    if (fs_inotify->stop == false) {
        fs_inotify->stop = true;
        pthread_join(fs_inotify->tid, NULL);
        VLOG(30) << "stop watch thread";
    }
    if (fs_inotify->watch_fd >= 0) {
        inotify_rm_watch(fs_inotify->inotify_fd, fs_inotify->watch_fd);
        VLOG(30) << "remove watch fd";
    }
    if (fs_inotify->inotify_fd >= 0) {
        close(fs_inotify->inotify_fd);
        VLOG(30) << "close inotify fd";
    }
    if (fs_inotify->inotify_FD) {
        fclose(fs_inotify->inotify_FD);
        VLOG(30) << "fclose inotify FD";
    }
    delete fs_inotify;
}

int AgentImpl::AddWatchPath(const std::string& dir) {
    FileSystemInotify* fs_inotify = new FileSystemInotify;
    fs_inotify->log_dir = dir;
    fs_inotify->agent = this;

    fs_inotify->inotify_fd = inotify_init();
    if (fs_inotify->inotify_fd < 0) {
        VLOG(30) << "init inotify fd error";
        DestroyWatchPath(fs_inotify);
        return -1;
    }

    fs_inotify->inotify_flag = IN_CREATE | IN_MOVED_TO |
                     IN_DELETE | IN_DELETE_SELF | IN_MOVE_SELF| IN_MOVED_FROM | IN_CLOSE_WRITE |
                     IN_MODIFY |
                     IN_ATTRIB;
    fs_inotify->watch_fd = inotify_add_watch(fs_inotify->inotify_fd, dir.c_str(), fs_inotify->inotify_flag);
    if (fs_inotify->watch_fd < 0) {
        DestroyWatchPath(fs_inotify);
        return -1;
    }
    if ((fs_inotify->inotify_FD = fdopen(fs_inotify->inotify_fd, "r")) == NULL) {
        DestroyWatchPath(fs_inotify);
        return -1;
    }


    VLOG(30) << "add watch addr " << dir << ", watch fd " << fs_inotify->watch_fd;
    fs_inotify->stop = false;
    pthread_create(&fs_inotify->tid, NULL, WatchThreadWrapper, fs_inotify);

    // add to management list
    pthread_spin_lock(&lock_);
    std::map<std::string, FileSystemInotify*>::iterator it = inotify_.find(dir);
    if (it != inotify_.end()) {
        pthread_spin_unlock(&lock_);

        DestroyWatchPath(fs_inotify);
        LOG(WARNING) << "dir " << dir << ", has been watch";
        return -1;
    } else {
        inotify_[dir] = fs_inotify;
    }
    pthread_spin_unlock(&lock_);
    return 0;
}

// log_name := module name's log file name prefix
int AgentImpl::AddWatchModuleStream(const std::string& module_name, const std::string& log_name) {
    VLOG(30) << "add module stream, module name " << module_name << ", file name " << log_name;
    LogStream* stream = NULL;
    pthread_spin_lock(&lock_);
    std::map<std::string, LogStream*>::iterator it = log_streams_.find(module_name);
    if (it != log_streams_.end()) {
        stream = log_streams_[module_name];
    } else {
        stream = new LogStream(module_name, log_options_, rpc_client_, &server_lock_, &info_);
        log_streams_[module_name] = stream;
    }

    std::map<std::string, std::string>::iterator file_it = module_file_set_.find(log_name);
    if (file_it == module_file_set_.end()) {
        module_file_set_[log_name] = module_name;
        stream->AddTableName(log_name);
    }
    pthread_spin_unlock(&lock_);
    return 0;
}

///////////////////////////////////////////
/////       rpc method                /////
///////////////////////////////////////////
void AgentImpl::Echo(::google::protobuf::RpcController* controller,
                     const mdt::LogAgentService::EchoRequest* request,
                     mdt::LogAgentService::EchoResponse* response,
                     ::google::protobuf::Closure* done) {
    LOG(INFO) << "Echo: " << request->message();
    done->Run();
}

void AgentImpl::RpcAddWatchPath(::google::protobuf::RpcController* controller,
                                const mdt::LogAgentService::RpcAddWatchPathRequest* request,
                                mdt::LogAgentService::RpcAddWatchPathResponse* response,
                                ::google::protobuf::Closure* done) {
    if (AddWatchPath(request->watch_path()) < 0) {
        response->set_status(mdt::LogAgentService::kRpcError);
        LOG(WARNING) << "add watch event in dir " << request->watch_path() << " failed";
    } else {
        response->set_status(mdt::LogAgentService::kRpcOk);
    }
    done->Run();
}

void AgentImpl::RpcAddWatchModuleStream(::google::protobuf::RpcController* controller,
                                        const mdt::LogAgentService::RpcAddWatchModuleStreamRequest* request,
                                        mdt::LogAgentService::RpcAddWatchModuleStreamResponse* response,
                                        ::google::protobuf::Closure* done) {
    const std::string& module_name = request->production_name();
    const std::string& log_name = request->log_name(); // use for match log file name, if not match, discard such log file

    if (AddWatchModuleStream(module_name, log_name) < 0) {
        response->set_status(mdt::LogAgentService::kRpcError);
        VLOG(35) << "add watch module " << module_name << " failed";
    } else {
        response->set_status(mdt::LogAgentService::kRpcOk);
    }
    done->Run();
}

void AgentImpl::RpcStoreSpan(::google::protobuf::RpcController* controller,
                             const mdt::LogAgentService::RpcStoreSpanRequest* request,
                             mdt::LogAgentService::RpcStoreSpanResponse* response,
                             ::google::protobuf::Closure* done) {
    done->Run();
}

void AgentImpl::RpcTraceGalaxyApp(::google::protobuf::RpcController* controller,
                                  const mdt::LogAgentService::RpcTraceGalaxyAppRequest* request,
                                  mdt::LogAgentService::RpcTraceGalaxyAppResponse* response,
                                  ::google::protobuf::Closure* done) {
    if (request->parse_path_fn() == 2) {
        std::string full_path;
        full_path.append("/");
        full_path.append(request->user_log_dir());

        bool is_success = true;
        // add watch path
        if (AddWatchPath(full_path) < 0) {
            is_success = false;
            LOG(WARNING) << "add watch event in dir " << full_path << " failed";
        }
        if (AddWatchModuleStream(request->db_name(), request->table_name()) < 0) {
            is_success = false;
            VLOG(35) << "add watch module " << request->db_name() << " failed, log file " << request->table_name();
        }

        if (is_success) {
            response->set_status(mdt::LogAgentService::kRpcOk);
        } else {
            response->set_status(mdt::LogAgentService::kRpcError);
        }
        done->Run();
        return;
    }

    // get task work path
    std::string path;
    path.append("/");
    path.append(request->work_dir());
    path.append("/");
    path.append(request->pod_id());

    DIR* dir_ptr = opendir(path.c_str());
    if (dir_ptr == NULL) {
        response->set_status(mdt::LogAgentService::kRpcError);
        done->Run();
        return;
    }

    bool is_success = true;
    struct dirent* dir_entry = NULL;
    while ((dir_entry = readdir(dir_ptr)) != NULL) {
        std::string task_id_half(dir_entry->d_name, strlen(dir_entry->d_name));
        if (task_id_half.find(request->pod_id()) == std::string::npos) {
            continue;
        }
        std::string task_path(path);
        task_path.append("/");
        task_path.append(task_id_half);
        task_path.append("/");
        task_path.append(request->user_log_dir());
        // add watch path
        if (AddWatchPath(task_path) < 0) {
            is_success = false;
            LOG(WARNING) << "add watch event in dir " << task_path << " failed";
        }
        if (AddWatchModuleStream(request->db_name(), request->table_name()) < 0) {
            is_success = false;
            VLOG(35) << "add watch module " << request->db_name() << " failed, log file " << request->table_name();
        }
    }
    closedir(dir_ptr);

    if (is_success) {
        response->set_status(mdt::LogAgentService::kRpcOk);
    } else {
        response->set_status(mdt::LogAgentService::kRpcError);
    }
    done->Run();
}

}
}
