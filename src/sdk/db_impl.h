// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_DB_IMPL_H_
#define  MDT_SDK_DB_IMPL_H_

#include "util/tera.h"
#include "util/env.h"
#include "sdk/db.h"

namespace mdt {

///////////////////////////////////
//      DatabaseImpl class       //
///////////////////////////////////
class DatabaseImpl : public Database {
public:
    DatabaseImpl(const Options& options, const std::string& db_name);
    ~DatabaseImpl();

    // create fs namespace
    static Status CreateDB(const Options& options, const std::string& db_name, Database** db_ptr);
    static Status OpenDB(const std::string& db_name, Database** db_ptr);

    // if db not exit, create it
    Status CreateTable(const CreateRequest& request, CreateResponse* response, Table** table_ptr);
    Status CreateTable(const TableDescription& table_desc);
    Status OpenTable(const std::string& table_name, Table** table_ptr);

    std::string& DatabaseName() {return db_name_;}

private:
    int InternalCreateTable(const TableDescription& table_desc, Table** table_ptr);
    DatabaseImpl(const DatabaseImpl&);
    void operator=(const DatabaseImpl&);

private:
    std::string db_name_;
    const Options options_;
    FilesystemOptions fs_opt_;
    TeraOptions tera_opt_;
    std::map<std::string, Table*> table_map_; // <table_name, table ptr>
};

// TableImpl relative

} // namespace mdt

#endif  // MDT_SDK_DB_IMPL_H_
