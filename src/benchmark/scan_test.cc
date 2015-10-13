#include <iostream>
#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include "sdk/sdk.h"
#include "sdk/db.h"
#include "sdk/table.h"
#include "util/env.h"
#include "util/coding.h"

void primary_key_scan(mdt::Table* table, std::string& primary_key) {
    mdt::SearchRequest* search_req = new mdt::SearchRequest;
    mdt::SearchResponse* search_resp = new mdt::SearchResponse;
    search_req->primary_key = primary_key;
    std::cout << "scan table, case 1, primary key scan ..." << std::endl;
    
    LOG(INFO) << "magicscan primary key: begin scan.. ";
    table->Get(search_req, search_resp);
    LOG(INFO) << "magicscan primary key: end scan.. ";
    for (uint32_t i = 0; i < search_resp->result_stream.size(); i++) {
        const mdt::ResultStream& result = search_resp->result_stream[i];
        LOG(INFO) << "primary key: " << result.primary_key 
	    << ", num of item " << result.result_data_list.size() << std::endl;
        for (uint32_t j = 0; j < result.result_data_list.size(); j++) {
            std::cout << "        data(size=" << result.result_data_list[j].size() 
		<< ") " << result.result_data_list[j] << std::endl;
        }
    }
    LOG(INFO) << "total row num " << search_resp->result_stream.size();
    delete search_req;
    delete search_resp;
}

void index_key_scan(mdt::Table* table) {
    mdt::SearchRequest* search_req = new mdt::SearchRequest;
    search_req->limit = 100;
    search_req->start_timestamp = 1444465625;
    search_req->end_timestamp = 1444465627;

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

    std::cout << "get index key..." << std::endl;
    LOG(INFO) << "magicscan index key: begin scan.. ";
    table->Get(search_req, search_resp);
    LOG(INFO) << "magicscan index key: end scan.. ";
    for (uint32_t i = 0; i < search_resp->result_stream.size(); i++) {
        const mdt::ResultStream& result = search_resp->result_stream[i];
	LOG(INFO) << "primary key: " << result.primary_key 
	    << ", num of item " << result.result_data_list.size() << std::endl;
        for (uint32_t j = 0; j < result.result_data_list.size(); j++) {
            std::cout << "        data(size=" << result.result_data_list[j].size() 
		<< ") " << result.result_data_list[j] << std::endl;
        }
    }
    LOG(INFO) << "total row num " << search_resp->result_stream.size();
    delete search_req;
    delete search_resp;
}

int main(int ac, char* av[]) {
    ::google::ParseCommandLineFlags(&ac, &av, true);
    std::string db_name = "z012";
    std::string table_name = "kepler001";
     
    // create db
    std::cout << "open db ..." << std::endl;
    mdt::Database* db;
    db = mdt::OpenDatabase(db_name);

    std::cout << "open table ..." << std::endl;
    mdt::Table* table;
    table = OpenTable(db, table_name);


    // search test: case 1: primary key Scan
    LOG(INFO) << "primary key scan";
    std::string primary_key = "0000000056";
    primary_key_scan(table, primary_key); 

    LOG(INFO) << "index key scan";    
    index_key_scan(table);    
 
    std::cout << "done" << std::endl;
    return 0;
}
