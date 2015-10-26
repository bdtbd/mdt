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
#include "util/counter.h"
#include "util/mutex.h"

struct WriteTask {
    mdt::Table* table_;
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
    delete request;
    delete response;
}

void* write_task(void* arg) {
    WriteTask* task = (WriteTask*)arg;
    std::string value_str(3000, 'x');
    uint64_t key_len = task->num_keys_ > 10 ? (task->num_keys_ / 10): task->num_keys_;
    mdt::Table* table = task->table_;
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

void dispatch_task(std::vector<WriteTask*>& tasks, mdt::Table* table, mdt::Mutex* mu, mdt::CondVar* cond,
		   mdt::Counter* counter, uint64_t num_task, uint64_t num_keys) {
    for (uint64_t i = 0; i < num_task; i++) {
	pthread_t tid;
	WriteTask* task = tasks[i];
	task->table_ = table;
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
    //std::string db_name = "mdt-test005";
    //std::string table_name = "table-kepler001";
    std::string db_name = "z012";
    std::string table_name = "kepler001";

    std::cout << "open db ..." << std::endl;
    mdt::Database* db;
    db = mdt::OpenDatabase(db_name);

    std::cout << "open table ..." << std::endl;
    mdt::Table* table;
    table = OpenTable(db, table_name);

    mdt::Mutex mu;
    mdt::CondVar cond(&mu);
    mdt::Counter counter;
    uint64_t num_task = 50;
    std::vector<WriteTask*> tasks;
    for (uint64_t i = 0; i < num_task; i++) {
        WriteTask* task = new WriteTask;
        tasks.push_back(task);
    }
    uint64_t num_keys = 1000000;
    counter.Set((int64_t)(num_task * num_keys) + 1);
    std::cout << "Test put ..." << std::endl;
    dispatch_task(tasks, table, &mu, &cond, &counter, num_task, num_keys);

    // wait finish
    if (counter.Dec() != 0) {
	cond.Wait();
    }

    for (uint64_t i = 0; i < num_task; i++) {
        delete tasks[i];
    }
    std::cout << "done" << std::endl;
    return 0;
}
