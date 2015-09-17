// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_TABLE_IMPL_H_
#define  MDT_SDK_TABLE_IMPL_H_

#include "common/counter.h"
#include "proto/kv.pb.h"
#include "util/env.h"
#include "util/tera.h"
#include "util/coding.h"
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
    uint32_t offset_;
    int32_t size_;

public:
    std::string SerializeToString() {
        char buf[8];
        char* p = buf;
        EncodeBigEndian32(p, offset_);
        EncodeBigEndian32(p + 4, size_);
        std::string offset_size(buf, 8);
        std::string s = fname_ + offset_size;
        return s;
    }
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
    TableImpl(const TableDescription& table_desc,
              const TeraAdapter& tera_adapter,
              const FilesystemAdapter& fs_adapter);
    virtual int Put(const StoreRequest* request, StoreResponse* response,
                    StoreCallback callback = NULL, void* callback_param = NULL);
    virtual int Get(const SearchRequest* request, SearchResponse* response,
                    SearchCallback callback = NULL, void* callback_param = NULL);

    virtual const std::string& TableName() {return table_desc_.table_name;}

public:
    static int OpenTable(const std::string& db_name, const TeraOptions& tera_opt,
                         const FilesystemOptions& fs_opt, const TableDescription& table_desc,
                         Table** table_ptr);

private:
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
    void* callback_param_;
    common::Counter counter_; // atomic counter

    PutContext(TableImpl* table,
               const StoreRequest* request,
               StoreResponse* response,
               StoreCallback callback = NULL,
               void* callback_param = NULL)
        : table_(table), req_(request),
        resp_(response), callback_(callback),
        callback_param_(callback_param) {}
};

struct GetContext {
    TableImpl* table_;
    const SearchRequest* req_;
    SearchResponse* resp_;
    SearchCallback callback_;
    common::Counter counter_; // atomic counter

    GetContext(TableImpl* table,
               const SearchRequest* request,
               SearchResponse* response,
               SearchCallback callback)
        : table_(table), req_(request),
        resp_(response), callback_(callback) {}
};

} // namespace mdt

#endif  // MDT_SDK_TABLE_IMPL_H_
