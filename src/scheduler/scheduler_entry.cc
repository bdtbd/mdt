#include "scheduler/scheduler_impl.h"
#include <iostream>
#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <tera.h>
#include <galaxy.h>

DECLARE_string(scheduler_service_port);
DECLARE_string(log_dir);

void SetupLog(const std::string& name) {
    std::string program_name = "mdt";
    if (!name.empty()) {
        program_name = name;
    }

    if (FLAGS_log_dir.size() == 0) {
        if (access("../log", F_OK)) {
            FLAGS_log_dir = "../log";
        } else {
            FLAGS_log_dir = "./log";
        }
    }

    if (access(FLAGS_log_dir.c_str(), F_OK)) {
        mkdir(FLAGS_log_dir.c_str(), 0777);
    }

    std::string log_filename = FLAGS_log_dir + "/" + program_name + ".INFO.";
    std::string wf_filename = FLAGS_log_dir + "/" + program_name + ".WARNING.";
    google::SetLogDestination(google::INFO, log_filename.c_str());
    google::SetLogDestination(google::WARNING, wf_filename.c_str());
    google::SetLogDestination(google::ERROR, "");
    google::SetLogDestination(google::FATAL, "");

    google::SetLogSymlink(google::INFO, program_name.c_str());
    google::SetLogSymlink(google::WARNING, program_name.c_str());
    google::SetLogSymlink(google::ERROR, "");
    google::SetLogSymlink(google::FATAL, "");
}

static pthread_once_t glog_once = PTHREAD_ONCE_INIT;
static void InternalSetupGoogleLog() {
    // init param, setup log
    std::string log_prefix = "scheduler";
    ::google::InitGoogleLogging(log_prefix.c_str());
    SetupLog(log_prefix);
    tera::Client::SetGlogIsInitialized();
    LOG(INFO) << "start loging...";
}

void SetupGoogleLog() {
    pthread_once(&glog_once, InternalSetupGoogleLog);
}

int main(int ac, char* av[]) {
    ::google::ParseCommandLineFlags(&ac, &av, true);
    SetupGoogleLog();
    mdt::scheduler::SchedulerImpl* scheduler= new mdt::scheduler::SchedulerImpl();
    if (scheduler == NULL) {
        std::cout << "can not new scheduler\n";
        exit(-1);
    }

    // register rpc service
    ::sofa::pbrpc::RpcServerOptions options;
    ::sofa::pbrpc::RpcServer rpc_server(options);
    if (!rpc_server.RegisterService(scheduler)) {
        return -1;
    }
    std::string hostip = std::string("0.0.0.0:") + FLAGS_scheduler_service_port;
    if (!rpc_server.Start(hostip)) {
        std::cout << "scheduler start failed.\n";
        return -1;
    }
    rpc_server.Run();
    return 0;
}

