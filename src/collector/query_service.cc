#include <gflags/gflags.h>
#include "collector/query_service.h"
#include <boost/bind.hpp>
#include <glog/logging.h>
#include "proto/scheduler.pb.h"

DECLARE_int32(se_num_threads);
DECLARE_bool(mdt_flagfile_set);
DECLARE_string(mdt_flagfile);
DECLARE_string(flagfile);
DECLARE_string(scheduler_addr);
DECLARE_string(se_service_port);

namespace mdt {

void* ReportThread(void* arg) {
    SearchEngineImpl* se = (SearchEngineImpl*)arg;
    se->ReportMessage();
    return NULL;
}

// operation
SearchEngineImpl::SearchEngineImpl()
    : stop_report_message_(false) {
    rpc_client_ = new RpcClient();
    pthread_create(&report_tid_, NULL, ReportThread, this);
}
SearchEngineImpl::~SearchEngineImpl() {}

void SearchEngineImpl::ReportMessage() {
    char hostname[255];
    if (0 != gethostname(hostname, 256)) {
        LOG(FATAL) << "fail to report message";
    }
    std::string hostname_str = hostname;
    while (1) {
        if (stop_report_message_) {
            return;
        }
        std::string local_addr = hostname_str + ":" + FLAGS_se_service_port;
        VLOG(50) << "hostip " << local_addr;
        std::string scheduler_addr = FLAGS_scheduler_addr;

        // report addr
        mdt::LogSchedulerService::LogSchedulerService_Stub* service;
        rpc_client_->GetMethodList(scheduler_addr, &service);
        mdt::LogSchedulerService::RegisterNodeRequest* req = new mdt::LogSchedulerService::RegisterNodeRequest();
        req->set_server_addr(local_addr);
        mdt::LogSchedulerService::CollectorInfo* info = req->mutable_info();
        info->set_qps(5000);
        info->set_min_packet_size(500);
        info->set_max_packet_size(500);
        info->set_average_packet_size(500);

        mdt::LogSchedulerService::RegisterNodeResponse* resp = new mdt::LogSchedulerService::RegisterNodeResponse();

        boost::function<void (const mdt::LogSchedulerService::RegisterNodeRequest*,
                mdt::LogSchedulerService::RegisterNodeResponse*,
                bool, int)> callback =
            boost::bind(&SearchEngineImpl::ReportMessageCallback,
                    this, _1, _2, _3, _4, service);
        rpc_client_->AsyncCall(service,
                               &mdt::LogSchedulerService::LogSchedulerService_Stub::RegisterNode,
                               req, resp, callback);
        report_event_.Wait();
        sleep(2);
    }
}

void SearchEngineImpl::ReportMessageCallback(const mdt::LogSchedulerService::RegisterNodeRequest* req,
                                             mdt::LogSchedulerService::RegisterNodeResponse* resp,
                                             bool failed, int error,
                                             mdt::LogSchedulerService::LogSchedulerService_Stub* service) {
        VLOG(50) << "report message, addr " << req->server_addr();
        delete req;
        delete resp;
        delete service;
        report_event_.Set();
}

// init mdt.flag
Status SearchEngineImpl::InitSearchEngine() {
#if 0
    if (!FLAGS_mdt_flagfile_set) {
        return Status::NotFound("not mdt.flag");
    }
    if (access(FLAGS_mdt_flagfile.c_str(), F_OK) != 0) {
        return Status::NotFound("not mdt.flag");
    }
    int ac = 1;
    char** av = new char*[2];
    av[0] = (char*)"dummy";
    av[1] = NULL;
    FLAGS_flagfile = FLAGS_mdt_flagfile;
    ::google::ParseCommandLineFlags(&ac, &av, true);
    delete av;
#endif
    return Status::OK();
}

Status SearchEngineImpl::OpenDatabase(const std::string& db_name) {
    MutexLock lock(&mu_);
    ::mdt::Database* db_ptr = NULL;
    std::map<std::string, ::mdt::Database*>::iterator it = db_map_.find(db_name);
    if (it == db_map_.end()) {
        db_ptr = ::mdt::OpenDatabase(db_name);
        if (db_ptr == NULL) {
            return Status::IOError("db cannot open");
        }
        db_map_.insert(std::pair<std::string, ::mdt::Database*>(db_name, db_ptr));
    }
    return Status::OK();
}

Status SearchEngineImpl::OpenTable(const std::string& db_name, const std::string& table_name) {
    MutexLock lock(&mu_);
    ::mdt::Database* db_ptr = NULL;
    std::map<std::string, ::mdt::Database*>::iterator it = db_map_.find(db_name);
    if (it == db_map_.end()) {
        return Status::NotFound("db not found");
    }
    db_ptr = it->second;

    ::mdt::Table* table_ptr = NULL;
    std::string internal_tablename = db_name + "#" + table_name;
    std::map<std::string, ::mdt::Table*>::iterator table_it = table_map_.find(internal_tablename);
    if (table_it == table_map_.end()) {
        table_ptr = ::mdt::OpenTable(db_ptr, table_name);
        if (table_ptr == NULL) {
            return Status::NotFound("table cannot open");
        }
        table_map_.insert(std::pair<std::string, ::mdt::Table*>(internal_tablename, table_ptr));
    }
    return Status::OK();
}

::mdt::Table* SearchEngineImpl::GetTable(const std::string& db_name, const std::string& table_name) {
    MutexLock lock(&mu_);
    ::mdt::Table* table_ptr = NULL;
    std::string internal_tablename = db_name + "#" + table_name;
    std::map<std::string, ::mdt::Table*>::iterator table_it = table_map_.find(internal_tablename);
    if (table_it == table_map_.end()) {
        return NULL;
    }
    table_ptr = table_it->second;
    return table_ptr;
}

void RpcRequestToMdtRequest(const ::mdt::SearchEngine::RpcSearchRequest* req, ::mdt::SearchRequest* req2) {
    req2->primary_key = req->primary_key();
    req2->start_timestamp = req->start_timestamp();
    req2->end_timestamp = req->end_timestamp();
    req2->limit = req->limit();
    for (int i = 0; i < req->condition_size(); i++) {
        const ::mdt::SearchEngine::RpcIndexCondition& idx = req->condition(i);
        ::mdt::IndexCondition idx2;
        idx2.index_name = idx.index_table_name();
        if (idx.cmp() == ::mdt::SearchEngine::RpcGreater) {
            idx2.comparator = kGreater;
        } else if (idx.cmp() == ::mdt::SearchEngine::RpcEqualTo) {
            idx2.comparator = kEqualTo;
        } else if (idx.cmp() == ::mdt::SearchEngine::RpcNotEqualTo) {
            idx2.comparator = kNotEqualTo;
        } else if (idx.cmp() == ::mdt::SearchEngine::RpcLess) {
            idx2.comparator = kLess;
        } else if (idx.cmp() == ::mdt::SearchEngine::RpcLessEqual) {
            idx2.comparator = kLessEqual;
        } else if (idx.cmp() == ::mdt::SearchEngine::RpcGreaterEqual) {
            idx2.comparator = kGreaterEqual;
        } else {
            // cmp type not know
            idx2.comparator = (COMPARATOR)idx.cmp();
        }
        idx2.compare_value = idx.cmp_key();
        req2->index_condition_list.push_back(idx2);
    }
}

void MdtResponseToRpcResponse(::mdt::SearchResponse* resp2, ::mdt::SearchEngine::RpcSearchResponse* resp) {
    for (int i = 0; i < (int)resp2->result_stream.size(); i++) {
        ::mdt::SearchEngine::RpcResultStream* stream = resp->add_result_list();
        const ::mdt::ResultStream& stream2 = resp2->result_stream[i];
        stream->set_primary_key(stream2.primary_key);
        for (int j = 0; j < (int)stream2.result_data_list.size(); j++) {
            std::string* str = stream->add_data_list();
            *str = stream2.result_data_list[j];
            //stream->set_data_list(j, stream2.result_data_list[j]);
        }
    }
}

void SearchEngineImpl::Search(::google::protobuf::RpcController* ctrl,
                                const ::mdt::SearchEngine::RpcSearchRequest* req,
                                ::mdt::SearchEngine::RpcSearchResponse* resp,
                                ::google::protobuf::Closure* done) {
    Status s = OpenDatabase(req->db_name());
    if (!s.ok()) {
        done->Run();
        return;
    }
    s = OpenTable(req->db_name(), req->table_name());
    if (!s.ok()) {
        done->Run();
        return;
    }
    ::mdt::Table* table = GetTable(req->db_name(), req->table_name());
    ::mdt::SearchRequest request;
    ::mdt::SearchResponse response;
    RpcRequestToMdtRequest(req, &request);
    table->Get(&request, &response, NULL, NULL);
    MdtResponseToRpcResponse(&response, resp);
    done->Run();
    return;
}

// store service
void RpcStoreRequestToMdtRequest(const ::mdt::SearchEngine::RpcStoreRequest* req,
                                 ::mdt::StoreRequest* request) {
    request->primary_key = req->primary_key();
    request->timestamp = req->timestamp();
    request->data = req->data();
    for (int i = 0; i < req->index_list_size(); i++) {
        const ::mdt::SearchEngine::RpcStoreIndex& idx = req->index_list(i);
        ::mdt::Index index;
        index.index_name = idx.index_table();
        index.index_key = idx.key();
        request->index_list.push_back(index);
    }
}

void MdtResponseToRpcStoreResponse(::mdt::StoreResponse* response,
                                   ::mdt::SearchEngine::RpcStoreResponse* resp) {
    resp->set_status(::mdt::SearchEngine::RpcOK);
}

struct StoreCallback_param {
    ::mdt::SearchEngine::RpcStoreResponse* resp;
    ::google::protobuf::Closure* done;
};

void StoreCallback_dump(mdt::Table* table, mdt::StoreRequest* request,
                        mdt::StoreResponse* response,
                        void* callback_param) {
    StoreCallback_param* param = (StoreCallback_param*)callback_param;
    MdtResponseToRpcStoreResponse(response, param->resp);
    param->done->Run();
    delete request;
    delete response;
    delete param;
}
void SearchEngineImpl::Store(::google::protobuf::RpcController* ctrl,
                             const ::mdt::SearchEngine::RpcStoreRequest* req,
                             ::mdt::SearchEngine::RpcStoreResponse* resp,
                             ::google::protobuf::Closure* done) {
    VLOG(30) << "begin store, db " << req->db_name() << ", table " << req->table_name() << ", req " << req->DebugString();
    Status s = OpenDatabase(req->db_name());
    if (!s.ok()) {
        done->Run();
        return;
    }
    s = OpenTable(req->db_name(), req->table_name());
    if (!s.ok()) {
        done->Run();
        return;
    }
    ::mdt::Table* table = GetTable(req->db_name(), req->table_name());

    ::mdt::StoreRequest* request = new ::mdt::StoreRequest();
    ::mdt::StoreResponse* response = new ::mdt::StoreResponse();
    StoreCallback_param* param = new StoreCallback_param();
    param->resp = resp;
    param->done = done;
    ::mdt::StoreCallback callback = StoreCallback_dump;
    RpcStoreRequestToMdtRequest(req, request);
    table->Put(request, response, callback, param);
    return;
}

/////////////////////////////////
//      rpc service            //
/////////////////////////////////
SearchEngineServiceImpl::SearchEngineServiceImpl(SearchEngineImpl* se)
    : se_(se),
      se_thread_pool_(new ThreadPool(FLAGS_se_num_threads)) {
}

SearchEngineServiceImpl::~SearchEngineServiceImpl() {
    delete se_thread_pool_;
}

void SearchEngineServiceImpl::Search(::google::protobuf::RpcController* ctrl,
                                     const ::mdt::SearchEngine::RpcSearchRequest* req,
                                     ::mdt::SearchEngine::RpcSearchResponse* resp,
                                     ::google::protobuf::Closure* done) {
    ThreadPool::Task task = boost::bind(&SearchEngineImpl::Search, se_, ctrl, req, resp, done);
    se_thread_pool_->AddTask(task);
}

void SearchEngineServiceImpl::Store(::google::protobuf::RpcController* ctrl,
                                    const ::mdt::SearchEngine::RpcStoreRequest* req,
                                    ::mdt::SearchEngine::RpcStoreResponse* resp,
                                    ::google::protobuf::Closure* done) {
    ThreadPool::Task task = boost::bind(&SearchEngineImpl::Store, se_, ctrl, req, resp, done);
    se_thread_pool_->AddTask(task);
}

void SearchEngineServiceImpl::OpenTable(::google::protobuf::RpcController* ctrl,
                                        const ::mdt::SearchEngine::RpcOpenTableRequest* req,
                                        ::mdt::SearchEngine::RpcOpenTableResponse* resp,
                                        ::google::protobuf::Closure* done) {
    resp->set_status(::mdt::SearchEngine::RpcOK);
    done->Run();
}

void SearchEngineServiceImpl::OpenDatabase(::google::protobuf::RpcController* ctrl,
                                           const ::mdt::SearchEngine::RpcOpenDatabaseRequest* req,
                                           ::mdt::SearchEngine::RpcOpenDatabaseResponse* resp,
                                           ::google::protobuf::Closure* done) {
    resp->set_status(::mdt::SearchEngine::RpcOK);
    done->Run();
}

}
