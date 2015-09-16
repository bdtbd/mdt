// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/sdk.h"
#include "sdk/db_impl.h"

namespace mdt {

// Database ops
Options InitDefaultOptions(const Options& options, const std::string& db_name) {
    Options opt = options;
    Status s = opt.env_->CreateDir(db_name);
    return opt;
}

DatabaseImpl::DatabaseImpl(const Options& options, const std::string& db_name)
    : db_name_(db_name),
    options_(InitDefaultOptions(options, db_name)) {
    // create fs's dir
    fs_opt_.env_ = options.env_;
    fs_opt_.fs_path_ = db_name + "/Filesystem/";
    fs_opt_.env_->CreateDir(fs_opt_.fs_path_);

    // create tera client
    tera::ErrorCode error_code;
    std::string tera_log_prefix = db_name;
    tera_opt_.root_path_ = db_name + "/Tera/";
    options.env_->CreateDir(tera_opt_.root_path_);
    tera_opt_.tera_flag_ = options.tera_flag_file_path_;
    tera_opt_.client_ = tera::Client::NewClient(tera_opt_.tera_flag_, tera_log_prefix, &error_code);
    assert(tera_opt_.client_);

    // create db schema table (kv mode)
    std::string schema_table_name = db_name + "#schema";
    tera::TableDescriptor schema_desc(schema_table_name);
    assert(tera_opt_.client_->CreateTable(schema_desc, &error_code));

    tera_opt_.schema_table_ = tera_opt_.client_->OpenTable(schema_table_name, &error_code);
    assert(tera_opt_.schema_table_);
}

Status DatabaseImpl::CreateDB(const Options& options,
                              const std::string& db_name,
                              Database** db_ptr) {
    DatabaseImpl* db_impl = new DatabaseImpl(options, db_name);
    assert(db_impl);
    *db_ptr = db_impl;
    return Status::OK();
}

Status DatabaseImpl::CreateTable(const CreateRequest& req,
                                 CreateResponse* resp,
                                 Table** table_ptr) {
    assert(db_name_ == req.db_name);
    std::vector<TableDescription>::const_iterator it;
    for (it = req.table_descriptor_list.begin();
         it != req.table_descriptor_list.end();
         ++it) {
        if (table_map_.find(it->table_name) != table_map_.end()) {
            continue;
        }
        // construct memory structure
        assert(InternalCreateTable(*it, table_ptr));
        table_map_[it->table_name] = *table_ptr;
    }
    return Status::OK();
}

int DatabaseImpl::InternalCreateTable(const TableDescription& table_desc, Table** table_ptr) {
    Table::OpenTable(db_name_, tera_opt_, fs_opt_, table_desc, table_ptr);
    return 0;
}

} // namespace mdt
