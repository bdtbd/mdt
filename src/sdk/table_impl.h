// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_TABLE_IMPL_H_
#define  MDT_SDK_TABLE_IMPL_H_

#include "common/counter.h"
#include "proto/kv.pb.h"
#include "util/env.h"
#include "util/tera.h"
#include "sdk/sdk.h"
#include "sdk/table.h"
#include "sdk/option.h"

namespace mdt {

///////////////////////////////
//      TableImpl class      //
///////////////////////////////
// table in memory control structure
struct FileLocation {
    std::string fname_;
    int32_t offset_;
    int32_t size_;

public:
    std::string& SerializeToString();
};

struct TeraAdapter {
    std::string table_prefix_; // db_name
    TeraOptions opt_;
    std::map<std::string, tera::Table*> tera_table_map_; // <table_name, table desc>
};

class DataWriter {
public:
    DataWriter(const std::string& fname, WritableFile* file)
        : fname_(fname), file_(file), offset_(0) {}

    int AddRecord(const std::string& data, FileLocation* location);

private:
    // write data to filesystem
    std::string fname_;
    WritableFile* file_;
    int32_t offset_; // TODO: no more than 4G per file
};

struct FilesystemAdapter {
    std::string root_path_; // data file dir
    Env* env_;

    DataWriter* writer_;
};

class TableImpl : public Table {
public:
    int OpenTable(const std::string& db_name, const TeraOptions& tera_opt,
                  const FilesystemOptions& fs_opt, const TableDescription& table_desc,
                  Table** table_ptr);
    TableImpl(const TableDescription& table_desc,
              const TeraAdapter& tera_adapter,
              const FilesystemAdapter& fs_adapter);
    int Put(const StoreRequest* request, StoreResponse* response, StoreCallback callback);
    int Put(const StoreRequest& request, StoreResponse* response);
    int Get(const SearchRequest* request, SearchResponse* response, SearchCallback callback);
    int Get(const SearchRequest& request, SearchResponse* response);

    std::string& TableName() {return table_desc_.table_name;}

private:
    int AssembleTableSchema(const TableDescription& table_desc,
                            BigQueryTableSchema* schema);
    int DisassembleTableSchema(const BigQueryTableSchema& schema,
                               TableDescription* table_desc);
    DataWriter* GetDataWriter();
    tera::Table* GetTable(const std::string& table_name);
    std::string TimeToString();

private:
    TableDescription table_desc_;
    TeraAdapter tera_;
    FilesystemAdapter fs_;
};

struct PutContext {
    TableImpl* table_;
    const StoreRequest* req_;
    StoreResponse* resp_;
    StoreCallback callback_;
    common::Counter counter_; // atomic counter

    PutContext(TableImpl* table,
               const StoreRequest* request,
               StoreResponse* response,
               StoreCallback callback)
        : table_(table), req_(request),
        resp_(response), callback_(callback) {}
};

struct GetContext {
    TableImpl* table_;
    const SearchRequest* req_;
    SearchResponse* resp_;
    SearchCallback callback_;
    Counter counter_; // atomic counter

    GetContext(TableImpl* table,
               const SearchRequest* request,
               SearchResponse* response,
               SearchCallback callback)
        : table_(table), req_(request),
        resp_(response), callback_(callback) {}
};

} // namespace mdt

#endif  // MDT_SDK_TABLE_IMPL_H_
