#include "scheduler/scheduler_impl.h"
#include <iostream>
#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

DECLARE_string(scheduler_service_port);

int main(int ac, char* av[]) {
    ::google::ParseCommandLineFlags(&ac, &av, true);
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

