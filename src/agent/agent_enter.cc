#include "agent/agent_impl.h"
#include <gflags/gflags.h>
#include <unistd.h>
#include <sofa/pbrpc/pbrpc.h>

DECLARE_string(agent_service_port);

int main(int ac, char* av[]) {
    ::google::ParseCommandLineFlags(&ac, &av, true);
    // run agent
    mdt::agent::AgentImpl* agent = new mdt::agent::AgentImpl();
    if (agent == NULL) {
        std::cout << "can not new log agent\n";
        exit(-1);
    }
    if (agent->Init() < 0) {
        std::cout << "agent init error\n";
        exit(-1);
    }

    // register rpc service
    ::sofa::pbrpc::RpcServerOptions options;
    ::sofa::pbrpc::RpcServer rpc_server(options);
    if (!rpc_server.RegisterService(agent)) {
        return -1;
    }
    std::string hostip = std::string("0.0.0.0:") + FLAGS_agent_service_port;
    if (!rpc_server.Start(hostip)) {
        return -1;
    }
    rpc_server.Run();
    return 0;
}

