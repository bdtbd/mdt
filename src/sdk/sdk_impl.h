// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_SDK_IMPL_H_
#define  MDT_SDK_SDK_IMPL_H_

#include <map>

#include "common/mutex.h"
#include "common/thread_pool.h"

#include "util/status.h"
#include "sdk/sdk.h"
#include "sdk/db.h"
#include "sdk/table_impl.h"

namespace mdt {

class Database;
class Table;

class SdkImpl {
public:
    // 写入接口
    static void Put(const StoreRequest* request, StoreResponse* response,
                    StoreCallback callback = NULL);

    // 同步查询接口
    static void Get(const SearchRequest& request, SearchResponse* response);

    // 异步查询接口
    static void Get(const SearchRequest* request, SearchResponse* response, SearchCallback callback);

    // 建表接口
    static void Create(const CreateRequest& request, CreateResponse* response);

private:
    static Status FindTable(const std::string& db_name, const std::string& table_name,
                            Table** table_ptr);
    static Status FindDatabase(const std::string& db_name, Database** db_ptr);

    static void RunStoreCallback(StoreCallback callback, const StoreRequest* request,
                                 const StoreResponse* response);
    static void RunSearchCallback(SearchCallback callback, const SearchRequest* request,
                                  const SearchResponse* response);

private:
    static std::map<std::string, Database*> db_map_;
    static Database* cur_db_;
    static Table* cur_table_;
    static common::Mutex mutex_;

    static common::ThreadPool thread_pool_;

}; // class SdkImpl

} // namespace mdt

#endif  // MDT_SDK_SDK_IMPL_H_
