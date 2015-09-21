#include <iostream>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>
#include <sys/stat.h>

#include "sdk/sdk.h"
#include "sdk/db.h"
#include "sdk/table.h"

#include "util/env.h"
#include "util/coding.h"

DECLARE_string(flagfile);
DECLARE_string(log_file);

void SetupLog(const std::string& name) {
    std::string program_name = "mdt";
    if (!name.empty()) {
        program_name = name;
    }

    if (FLAGS_log_dir.size() == 0) {
        FLAGS_log_dir = "./log";
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

void LocationSerialToStringTest() {
    // location.SerialToString test
    std::string s = "90-10:89:10-87";
    uint32_t offset = 100;
    uint32_t size = 40;
    char buf[8];
    char* p = buf;
    mdt::EncodeBigEndian32(p, offset);
    mdt::EncodeBigEndian32(p + 4, size);
    std::string ss(buf, 8);
    std::string res = s + ss;
    LOG(INFO) << "location serial test : " << res;
}

void SetupGoogleLog() {
    // init param, setup log
    std::string log_prefix = "mdt";
    ::google::InitGoogleLogging(log_prefix.c_str());
    SetupLog(log_prefix);
    LOG(INFO) << "start loging...";
}

void StoreCallback_Test(mdt::Table* table, const mdt::StoreRequest* request,
                              mdt::StoreResponse* response,
                              void* callback_param) {
    LOG(INFO) << "<<< callabck test >>>";
    bool* store_finish = (bool*)callback_param;
    *store_finish = true;
}

int main(int ac, char* av[]) {
    ::google::ParseCommandLineFlags(&ac, &av, true);
    //SetupGoogleLog();

    //LocationSerialToStringTest();

    // create db
    std::cout << "open db ..." << std::endl;
    mdt::Database* db;
    std::string db_name = "mdt-test005";
    db = mdt::OpenDatabase(db_name);

    // create table
    std::cout << "create table ..." << std::endl;
    mdt::TableDescription table_desc;
    table_desc.table_name = "table-kepler001";
    table_desc.primary_key_type = mdt::kBytes;

    mdt::IndexDescription index_table1, index_table2, index_table3;
    index_table1.index_name = "Query";
    index_table1.index_key_type = mdt::kBytes;
    index_table2.index_name = "Costtime";
    index_table2.index_key_type = mdt::kBytes;
    index_table3.index_name = "Service";
    index_table3.index_key_type = mdt::kBytes;

    table_desc.index_descriptor_list.push_back(index_table1);
    table_desc.index_descriptor_list.push_back(index_table2);
    table_desc.index_descriptor_list.push_back(index_table3);
    CreateTable(db, table_desc);

    std::cout << "open table ..." << std::endl;
    mdt::Table* table;
    std::string table_name = table_desc.table_name;
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

    while (!store_finish) {
        usleep(1000);
    }

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
