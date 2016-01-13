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

DECLARE_string(scheduler_addr);
DECLARE_string(db_dir);
DECLARE_string(watch_log_dir);
DECLARE_string(module_name_list);

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
    server_addr_ = "nil";
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
        server_addr_ = resp->primary_server_addr();
        VLOG(50) << "agent, collector addr " << server_addr_;
        pthread_spin_unlock(&server_lock_);
    }
    delete req;
    delete resp;
    delete service;
    server_addr_event_.Set();
}

void AgentImpl::GetServerAddr() {
    while (1) {
        if (stop_scheduler_thread_) {
            return;
        }
        std::string scheduler_addr = FLAGS_scheduler_addr;

        mdt::LogSchedulerService::LogSchedulerService_Stub* service;
        rpc_client_->GetMethodList(scheduler_addr, &service);
        mdt::LogSchedulerService::GetNodeListRequest* req = new mdt::LogSchedulerService::GetNodeListRequest();
        mdt::LogSchedulerService::GetNodeListResponse* resp = new mdt::LogSchedulerService::GetNodeListResponse();
        pthread_spin_lock(&server_lock_);
        req->set_current_server_addr(server_addr_);
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
        } else if (event.mask & (IN_DELETE | IN_DELETE_SELF | IN_MOVE_SELF| IN_MOVED_FROM | IN_CLOSE_WRITE)) {
            DeleteWatchEvent(fs_inotify->log_dir, filename, &event);
        } else if (event.mask & (IN_MODIFY)) {
            AddWriteEvent(fs_inotify->log_dir, filename, &event);
        } else {
            //for (int i = 0; i < sizeof(event_masks)/sizeof(mdt::agent::EventMask); ++i) {
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

int AgentImpl::AddWriteEvent(const std::string& logdir, const std::string& filename, inotify_event* event) {
    std::string module_name;
    ParseModuleName(filename, &module_name);
    VLOG(35) << "write event, module name " << module_name << ", log dir " << logdir;
    if (module_name.size() == 0) {
        VLOG(30) << "dir " << filename << ", no module match";
        return -1;
    }

    LogStream* stream = NULL;
    pthread_spin_lock(&lock_);
    std::map<std::string, LogStream*>::iterator it = log_streams_.find(module_name);
    if (it != log_streams_.end()) {
        stream = log_streams_[module_name];
    } else {
        stream = new LogStream(module_name, log_options_, rpc_client_, &server_lock_, &server_addr_);
        log_streams_[module_name] = stream;
    }
    pthread_spin_unlock(&lock_);

    stream->AddWriteEvent(logdir + "/" + filename);
    return 0;
}

int AgentImpl::DeleteWatchEvent(const std::string& logdir, const std::string& filename, inotify_event* event) {
    std::string module_name;
    ParseModuleName(filename, &module_name);
    VLOG(30) << "delete event, module name " << module_name << ", log dir " << logdir;
    if (module_name.size() == 0) {
        VLOG(30) << "dir " << filename << ", no module match";
        return -1;
    }

    LogStream* stream = NULL;
    pthread_spin_lock(&lock_);
    std::map<std::string, LogStream*>::iterator it = log_streams_.find(module_name);
    if (it != log_streams_.end()) {
        stream = log_streams_[module_name];
    } else {
        stream = new LogStream(module_name, log_options_, rpc_client_, &server_lock_, &server_addr_);
        log_streams_[module_name] = stream;
    }
    pthread_spin_unlock(&lock_);

    stream->DeleteWatchEvent(logdir + "/" + filename);
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

}
}
