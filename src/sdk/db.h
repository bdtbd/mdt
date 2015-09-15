// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_DB_H_
#define  MDT_SDK_DB_H_

#include "sdk/sdk_impl.h"

namespace mdt {

class Database {
public:
    static Status CreateDB(const std::string& db_name);
    static Status OpenDB(const std::string& db_name, Database** db_ptr);
    virtual Status CreateTable(const TableDescription& table_desc);
    virtual Status OpenTable(const std::string& table_name, Table** table_ptr);

private:
    Database(const Database&);
    void operator=(const Database&);
};

} // namespace mdt

#endif  //MDT_SDK_DB_H_
