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
#include "utils/env.h"
#include "utils/counter.h"
#include "utils/mutex.h"

struct WriteTask {
    mdt::Database* db_;
    std::string table_name_;
    uint64_t start_key_;
    uint64_t num_keys_;
    mdt::Mutex* mu_;
    mdt::CondVar* cond_;
    mdt::Counter* counter_;
};

void StoreCallback_Test(mdt::Table* table, mdt::StoreRequest* request,
                        mdt::StoreResponse* response,
                        void* callback_param) {
    WriteTask* task = (WriteTask*)callback_param;
    if (task->counter_->Dec() == 0) {
	task->cond_->Signal();
    }
}

void* write_task(void* arg) {
    WriteTask* task = (WriteTask*)arg;
    std::string value_str(3000, 'x');
    uint64_t key_len = task->num_keys_ > 10 ? (task->num_keys_ / 10): task->num_keys_;

    std::cout << "open table ..." << std::endl;
    mdt::Table* table;
    table = OpenTable(task->db_, task->table_name_);
    for (uint64_t i = 0; i < task->num_keys_; i++) {
        mdt::StoreRequest* req = new mdt::StoreRequest();
        char rowkeybuf[12];
        char* p = rowkeybuf;
	uint64_t rowkey = i % key_len + task->start_key_;
	p += snprintf(p, 11, "%011lu", rowkey);
        req->primary_key = rowkeybuf;

	char colbuf[12];
	char* col_ptr = colbuf;
	uint64_t col_id = i + task->start_key_;
	col_ptr += snprintf(col_ptr, 11, "%011lu", col_id);
	std::string col = colbuf;

        mdt::Index query, costtime, service;
        query.index_name = "Query";
        query.index_key = "beauty girl";
	query.index_key += col;

        costtime.index_name = "Costtime";
        costtime.index_key = "5ms";
        costtime.index_key += col;

	service.index_name = "Service";
        service.index_key = "bs module";
        service.index_key += col;

        req->index_list.push_back(query);
        req->index_list.push_back(costtime);
        req->index_list.push_back(service);
        req->data = value_str;
        req->timestamp = time(NULL);

        mdt::StoreResponse* resp = new mdt::StoreResponse();
        mdt::StoreCallback TestCallback = StoreCallback_Test;
        table->Put(req, resp, TestCallback, task);
    }
    LOG(INFO) << "thread put finish... ";
    return NULL;
}

void dispatch_task(mdt::Database* db, const std::string& table_name,
		   mdt::Mutex* mu, mdt::CondVar* cond,
		   mdt::Counter* counter, uint64_t num_task, uint64_t num_keys) {
    for (uint64_t i = 0; i < num_task; i++) {
	pthread_t tid;
	WriteTask* task = new WriteTask;
	task->db_ = db;
	task->table_name_ = table_name;
	task->mu_ = mu;
	task->cond_ = cond;
	task->counter_ = counter;
	task->start_key_ = num_keys * i;
	task->num_keys_ = num_keys;
	pthread_create(&tid, NULL, write_task, task);
    }
}

int main(int ac, char* av[]) {
    ::google::ParseCommandLineFlags(&ac, &av, true);
    std::string db_name = "z020";
    std::string table_name = "kepler001";

    std::cout << "open db ..." << std::endl;
    mdt::Database* db;
    db = mdt::OpenDatabase(db_name);

    mdt::Mutex mu;
    mdt::CondVar cond(&mu);
    mdt::Counter counter;
    uint64_t num_task = 20;
    uint64_t num_keys = 100000;
    counter.Set((int64_t)(num_task * num_keys) + 1);
    std::cout << "Test put ..." << std::endl;
    dispatch_task(db, table_name, &mu, &cond, &counter, num_task, num_keys);

    if (counter.Dec() != 0) {
	cond.Wait();
    }

    std::cout << "done" << std::endl;
    return 0;
}
