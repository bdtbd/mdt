#ifndef MDT_COLLECTOR_SEARCH_ENGINE_H_
#define MDT_COLLECTOR_SEARCH_ENGINE_H_

#include <map>
#include "sdk/table.h"
#include "sdk/db.h"
#include "utils/status.h"
#include "utils/event.h"
#include "utils/thread_pool.h"
#include "utils/mutex.h"
#include "proto/query.pb.h"
#include <google/protobuf/service.h>
#include "proto/scheduler.pb.h"
#include "rpc/rpc_client.h"

namespace mdt {

class SearchEngineImpl {
public:
    SearchEngineImpl();
    ~SearchEngineImpl();
    Status InitSearchEngine();
    Status OpenDatabase(const std::string& db_name);
    Status OpenTable(const std::string& db_name, const std::string& table_name);
    void Search(::google::protobuf::RpcController* ctrl,
                  const ::mdt::SearchEngine::RpcSearchRequest* req,
                  ::mdt::SearchEngine::RpcSearchResponse* resp,
                  ::google::protobuf::Closure* done);
    void Store(::google::protobuf::RpcController* ctrl,
               const ::mdt::SearchEngine::RpcStoreRequest* req,
               ::mdt::SearchEngine::RpcStoreResponse* resp,
               ::google::protobuf::Closure* done);
    void ReportMessage();

private:
    ::mdt::Table* GetTable(const std::string& db_name, const std::string& table_name);

    void ReportMessageCallback(const mdt::LogSchedulerService::RegisterNodeRequest* req,
                               mdt::LogSchedulerService::RegisterNodeResponse* resp,
                               bool failed, int error,
                               mdt::LogSchedulerService::LogSchedulerService_Stub* service);

private:
    pthread_t report_tid_;
    volatile bool stop_report_message_;
    RpcClient* rpc_client_; // client for scheduler
    AutoResetEvent report_event_;

    Mutex mu_;
    std::map<std::string, ::mdt::Database*> db_map_;
    std::map<std::string, ::mdt::Table*> table_map_;
};

class SearchEngineServiceImpl : public ::mdt::SearchEngine::SearchEngineService {
public:
    explicit SearchEngineServiceImpl(SearchEngineImpl* se);
    ~SearchEngineServiceImpl();

    void Search(::google::protobuf::RpcController* ctrl,
                     const SearchEngine::RpcSearchRequest* req,
                     SearchEngine::RpcSearchResponse* resp,
                     ::google::protobuf::Closure* done);
    void Store(::google::protobuf::RpcController* ctrl,
               const ::mdt::SearchEngine::RpcStoreRequest* req,
               ::mdt::SearchEngine::RpcStoreResponse* resp,
               ::google::protobuf::Closure* done);
    void OpenTable(::google::protobuf::RpcController* ctrl,
                   const SearchEngine::RpcOpenTableRequest* req,
                   SearchEngine::RpcOpenTableResponse* resp,
                   ::google::protobuf::Closure* done);
    void OpenDatabase(::google::protobuf::RpcController* ctrl,
                   const SearchEngine::RpcOpenDatabaseRequest* req,
                   SearchEngine::RpcOpenDatabaseResponse* resp,
                   ::google::protobuf::Closure* done);
private:
    void BGInfoCollector();

private:
    SearchEngineImpl* se_;
    ThreadPool* se_thread_pool_; // operate on se's method
    ThreadPool* se_read_thread_pool_; // operate on se's method
    ThreadPool* se_info_thread_pool_;
};

}
#endif

