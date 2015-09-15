// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_SDK_IMPL_H_
#define  MDT_SDK_SDK_IMPL_H_

#include "sdk/sdk.h"

namespace mdt {

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

}; // class SdkImpl

} // namespace mdt

#endif  // MDT_SDK_SDK_IMPL_H_
