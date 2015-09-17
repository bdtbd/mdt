// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_DB_H_
#define  MDT_SDK_DB_H_

#include "sdk/sdk.h"
#include "sdk/option.h"
#include "sdk/table.h"
#include "util/env.h"

namespace mdt {

struct Options {
    std::string tera_flag_file_path_; // tera.flag's path
    Env* env_;

public:
    Options() : env_(Env::Default()) {}
};

class Database {
public:
    // create fs namespace
    static Status CreateDB(const Options& options, std::string& db_name, Database** db_ptr);
    static Status OpenDB(const std::string& db_name, Database** db_ptr);
    Database() {}
    virtual ~Database();

    // if db not exit, create it
    virtual Status CreateTable(const CreateRequest& request, CreateResponse* response, Table** table_ptr);
    virtual Status CreateTable(const TableDescription& table_desc);
    virtual Status OpenTable(const std::string& table_name, Table** table_ptr);

    virtual std::string& DatabaseName();

private:
    Database(const Database&);
    void operator=(const Database&);
};

} // namespace mdt

#endif  //MDT_SDK_DB_H_
