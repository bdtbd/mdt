// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/sdk.h"

#include "sdk/sdk_impl.h"

namespace mdt {

// 同步写入接口
void Put(const StoreRequest& request, StoreResponse* response) {
    SdkImpl::Put(request, response);
}

// 异步写入接口
void Put(const StoreRequest* request, StoreResponse* response, StoreCallback callback) {
    SdkImpl::Put(request, response, callback);
}

// 同步查询接口
void Get(const SearchRequest& request, SearchResponse* response) {
    SdkImpl::Get(request, response);
}

// 异步查询接口
void Get(const SearchRequest* request, SearchResponse* response, SearchCallback callback) {
    SdkImpl::Get(request, response, callback);
}

// 建表接口
void Create(const CreateRequest& request, CreateResponse* response) {
    SdkImpl::Create(request, response);
}

} // namespace mdt
