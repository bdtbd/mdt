// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_TABLE_IMPL_H_
#define  MDT_SDK_TABLE_IMPL_H_

#include "sdk/sdk_impl.h"

namespace mdt {

///////////////////////////////
//      TableImpl class      //
///////////////////////////////
// table in memory control structure
struct TeraOptions;
struct TeraAdapter {
    std::string table_prefix_; // db_name
    TeraOptions opt_;
    std::map<std::string, tera::Table*> tera_table_map_; // <table_name, table desc>
};

struct FileLocation {
    std::string fname_;
    int32_t offset_;
    int32_t size_;

    std::string& SerializeToString();
};

class DataWriter {
public:
    DataWriter(const std::string& fname, WritableFile* file)
        : fname_(fname), file_(file), offset_(0) {}

    AddRecord(const std::string& data, FileLocation* location);

private:
    // write data to filesystem
    std::string fname_;
    WritableFile* file_;
    int32_t offset_; // TODO: no more than 4G per file
};

struct FilesystemAdapter {
    std::string root_path_; // data file dir
    Env* env_;

    DataWriter writer_;
};

struct Options {
    std::string tera_flag_file_path_; // tera.flag's path
    Env* env_;
};

class TableImpl;
struct PutContext {
    const StoreRequest* req_;
    StoreResponse* resp_;
    StoreCallback callback_;
    TableImpl* table_;
    Counter counter_; // atomic counter

    PutContext(TableImpl* table,
               const StoreRequest* request,
               StoreResponse* response,
               StoreCallback callback)
        : table_(table), req_(request),
        resp_(response), callback_(callback) {}
};

struct GetContext {
    const SearchRequest* req_;
    SearchResponse* resp_;
    SearchCallback callback_;
    TableImpl* table_;
    Counter counter_; // atomic counter

    GetContext(TableImpl* table,
               const SearchRequest* request,
               SearchResponse* response,
               SearchCallback callback)
        : table_(table), req_(request),
        resp_(response), callback_(callback) {}
};

class TableImpl : public Table {
public:
    int Put(const StoreRequest* request, StoreResponse* response, StoreCallback callback);
    int Put(const StoreRequest* request, StoreResponse* response);
    int Get(const SearchRequest* request, SearchResponse* response, SearchCallback callback);
    int Get(const SearchRequest& request, SearchResponse* response);

private:
    TableDescription table_desc_;
    TeraAdapter tera_;
    FilesystemAdapter fs_;
};

} // namespace mdt

#endif  // MDT_SDK_TABLE_IMPL_H_
