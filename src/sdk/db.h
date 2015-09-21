// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_DB_H_
#define  MDT_SDK_DB_H_

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "sdk/sdk.h"
#include "sdk/option.h"
#include "sdk/table.h"
#include "util/env.h"

namespace mdt {

struct Options {
    std::string tera_flag_file_path_; // tera.flag's path
    Env* env_;
};

class Database {
public:
    static Status OpenDB(const std::string& db_name, Database** db_ptr);

    Database() {}
    virtual ~Database();

    virtual Status CreateTable(const TableDescription& table_desc) = 0;
    virtual Status OpenTable(const std::string& table_name, Table** table_ptr) = 0;

    virtual std::string& DatabaseName() = 0;

private:
    Database(const Database&);
    void operator=(const Database&);
};

} // namespace mdt

#endif  //MDT_SDK_DB_H_
