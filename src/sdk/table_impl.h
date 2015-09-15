// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_TABLE_H_
#define  MDT_SDK_TABLE_H_

#include "sdk/sdk_impl.h"

namespace mdt {

class Selector {
    SearchRequest* request;
    SearchResponse* response;
    SearchCallback* callback;
    void* user_param;
    Status status;
};

class Inserter {
    StoreRequest* request;
    StoreResponse* response;
    StoreCallback* callback;
    void* user_param;
    Status status;
};

class TableImpl {
public:
    virtual void Insert(const SearchRequest* request, SearchResponse* response,
                        SearchCallback callback = NULL, void* callback_param = NULL);
    virtual void Select(const StoreRequest* request, StoreResponse* response,
                        StoreCallback callback = NULL, void* callback_param = NULL);

private:
    void InsertCallback(const SearchRequest* request, const SearchResponse* response,
                        void* user_param, const Status& status);
    void SelectCallback(const StoreRequest* request, const StoreResponse* response,
                        void* user_param, const Status& status);

private:
    Table(const Table&);
    void operator=(const Table&);
};

} // namespace mdt

#endif  //MDT_SDK_DB_H_
