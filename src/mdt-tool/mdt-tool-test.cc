#include <iostream>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "sdk/sdk.h"
#include "sdk/db.h"
#include "sdk/table.h"

#include "util/env.h"
#include "util/coding.h"

DECLARE_string(flagfile);
DECLARE_string(log_file);

void StoreCallback_Test(mdt::Table* table, const mdt::StoreRequest* request,
                              mdt::StoreResponse* response,
                              void* callback_param) {
    LOG(INFO) << "<<< callabck test >>>";
    bool* store_finish = (bool*)callback_param;
    *store_finish = true;
}

int main(int ac, char* av[]) {
    ::google::ParseCommandLineFlags(&ac, &av, true);

    // create db
    std::cout << "open db ..." << std::endl;
    mdt::Database* db;
    std::string db_name = "mdt-test010";
    db = mdt::OpenDatabase(db_name);
    
    std::cout << "open table ..." << std::endl;
    mdt::Table* table;
    std::string table_name = "kepler01";
    table = OpenTable(db, table_name);

    // insert data
    mdt::StoreRequest* store_req = new mdt::StoreRequest();
    store_req->primary_key = "20150920";
    store_req->timestamp = 638239414;
    mdt::Index query_index, costtime_index, service_index;

    query_index.index_name = "Query";
    query_index.index_key = "beauty girl";
    store_req->index_list.push_back(query_index);

    costtime_index.index_name = "Costtime";
    costtime_index.index_key = "5ms";
    store_req->index_list.push_back(costtime_index);

    service_index.index_name = "Service";
    service_index.index_key = "bs module";
    store_req->index_list.push_back(service_index);

    store_req->data = "this s a test, Query: beauty girl, Costtime: 5ms, Service: bs module";

    mdt::StoreResponse* store_resp = new mdt::StoreResponse();
    mdt::StoreCallback callback = StoreCallback_Test;


    bool store_finish = false;
    std::cout << "put ..." << std::endl;
    table->Put(store_req, store_resp, callback, &store_finish);

    struct timeval now;
    gettimeofday(&now, NULL);

    for (uint64_t i = 0; i < 1; i++) {
        mdt::StoreRequest* req = new mdt::StoreRequest();
        char buf[12];
        char* p = buf;
        p += snprintf(p, 11, "%11lu", i);
        std::string str(buf, 11);
        //req->primary_key = buf;
        req->primary_key = "20150923";
        req->timestamp = time(NULL);

        mdt::Index query, costtime, service;
        query.index_name = "Query";
        query.index_key = "beauty girl";
        costtime.index_name = "Costtime";
        costtime.index_key = "5ms";
        service.index_name = "Service";
        service.index_key = "bs module";

        req->index_list.push_back(query);
        req->index_list.push_back(costtime);
        req->index_list.push_back(service);
        req->data = "this s a TEST, Query: beauty girl, Costtime: 5ms, Service: bs module";

        mdt::StoreResponse* resp = new mdt::StoreResponse();
        mdt::StoreCallback TestCallback = StoreCallback_Test;
        table->Put(req, resp, TestCallback, &store_finish);
    }

    while (!store_finish) {
        usleep(1000);
    }

    struct timeval finish;
    gettimeofday(&finish, NULL);
    LOG(INFO) << "BIGQUERY: begin: tv_sec " << now.tv_sec << ", tv_usec " << now.tv_usec
        << ", now: tv_sec " << finish.tv_sec << ", tv_usec " << finish.tv_usec;

    // search test
    mdt::SearchRequest* search_req = new mdt::SearchRequest;
    search_req->start_timestamp = 638239413;
    search_req->end_timestamp = 638239415;

    mdt::IndexCondition query_index_cond1, query_index_cond2;
    query_index_cond1.index_name = "Query";
    query_index_cond1.comparator = mdt::kGreater;
    query_index_cond1.compare_value = "apple";

    query_index_cond2.index_name = "Query";
    query_index_cond2.comparator = mdt::kLess;
    query_index_cond2.compare_value = "car";

    search_req->index_condition_list.push_back(query_index_cond1);
    search_req->index_condition_list.push_back(query_index_cond2);

    mdt::SearchResponse* search_resp = new mdt::SearchResponse;

    std::cout << "get ..." << std::endl;
    table->Get(search_req, search_resp);
    for (uint32_t i = 0; i < search_resp->result_stream.size(); i++) {
        const mdt::ResultStream& result = search_resp->result_stream[i];
        std::cout << "primary key: " << result.primary_key << std::endl;
        for (uint32_t j = 0; j < result.result_data_list.size(); j++) {
            std::cout << "        data: " << result.result_data_list[j] << std::endl;
        }
    }

    std::cout << "done" << std::endl;
    return 0;
}
