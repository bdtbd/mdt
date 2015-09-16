// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_DB_IMPL_H_
#define  MDT_SDK_DB_IMPL_H_

#include "sdk/table_impl.h"

namespace mdt {

///////////////////////////////////
//      DatabaseImpl class       //
///////////////////////////////////
struct FilesystemOptions {
    std::string fs_path_; // db_name + FileSystem
};

struct TeraOptions {
    std::string root_path_; // path of tera dir
    std::string tera_flag_; // path of tera.flag
    tera::Client* client_;

    // schema table(kv), key = table_name, value = BigQueryTableSchema (define in kv.proto)
    std::string schema_table_name_;
    tera::Table* schema_table_;
};

class DatabaseImpl : public Database {
public:
    // create fs namespace
    static int CreateDB(const Options& options, std::string db_name, Database** db);
    // if db not exit, create it
    int CreateTable(const CreateRequest& request, CreateResponse* response, Table** table_ptr);

private:
    std::string db_name_;
    const Options options_;
    FilesystemOptions fs_opt_;
    TeraOptions tera_opt_;
    std::map<std::string, TableImpl*> table_map_; // <table_name, table ptr>
};

} // namespace mdt

#endif  // MDT_SDK_DB_IMPL_H_
