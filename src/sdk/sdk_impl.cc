// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/sdk_impl.h"

namespace mdt {

// 同步写入接口
void SdkImpl::Put(const StoreRequest& request, StoreResponse* response) {

}

// 异步写入接口
void SdkImpl::Put(const StoreRequest* request, StoreResponse* response,
                  StoreCallback callback) {

}

// 同步查询接口
void SdkImpl::Get(const SearchRequest& request, SearchResponse* response) {

}

// 异步查询接口
void SdkImpl::Get(const SearchRequest* request, SearchResponse* response,
                  SearchCallback callback) {

}

// 建表接口
void SdkImpl::Create(const CreateRequest& request, CreateResponse* response) {

}

} // namespace mdt
