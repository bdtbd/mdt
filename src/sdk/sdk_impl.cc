// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/mutex.h"
#include "sdk/sdk_impl.h"

namespace mdt {

// 写入接口
void SdkImpl::Put(const StoreRequest* request, StoreResponse* response,
                  StoreCallback callback) {
    Table* table = NULL;
    Status s = FindTable(request->db_name, request->table_name, &table);
    if (!s.ok()) {
        response->error = s.code();
        if (callback) {
            thread_pool_.AddTask(boost::bind(RunStoreCallback, callback, request, response));
        }
        return;
    }
    table->Put(request, response, callback);
}

// 查询接口
void SdkImpl::Get(const SearchRequest* request, SearchResponse* response,
                  SearchCallback callback) {
    Table* table = NULL;
    Status s = FindTable(request->db_name, request->table_name, &table);
    if (!s.ok()) {
        if (callback) {
            thread_pool_.AddTask(boost::bind(RunSearchCallback, callback, request, response));
        }
        return;
    }
    table->Get(request, response, callback);
}

// 建表接口
void SdkImpl::Create(const CreateRequest& request, CreateResponse* response) {

}

Status SdkImpl::FindTable(const std::string& db_name, const std::string& table_name,
                          Table** table_ptr) {
    MutexLock l(&mutex_);

    if (cur_table_->TableName() == table_name
          && cur_table_->DatabaseName() == db_name) {
        *table_ptr = cur_table_;
        return Status();
    }

    Database* db = NULL;
    Status s = FindDatabase(db_name, &db);
    if (!s.ok()) {
        return s;
    }

    Table* table = NULL;
    s = db->OpenTable(request->table_name, &table);
    if (s.ok()) {
        *table_ptr = cur_table_ = table;
    }
    return s;
}

Status SdkImpl::FindDatabase(const std::string& db_name, Database** db_ptr) {
    mutex_.AssertHold();

    if (cur_db_->Name() == db_name) {
        *db_ptr = cur_db_;
        return Status();
    }

    std::map<std::string, Database*>::iterator it = db_map_.find(db_name);
    if (it != db_map_.end()) {
        *db_ptr = cur_db_ = it->second;
        return Status();
    }

    Database* db = NULL;
    Status s = Database::OpenDB(db_name, &db);
    if (s.ok()) {
        *db_ptr = cur_db_ = db_map_[db_name] = db;
        return Status();
    }
    return s;
}

} // namespace mdt
