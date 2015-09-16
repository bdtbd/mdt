// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/db_impl.h"

namespace mdt {

// Database ops
Options InitDefaultOptions(const Options& options, const std::string& db_name) {
    Options opt = options;
    Status s = opt.env_->CreateDir(db_name);
    return opt;
}

DatabaseImpl::DatabaseImpl(const Options& options, const std::string& db_name)
    : options_(InitDefaultOptions(options, db_name)),
      db_name_(db_name) {
    // create fs's dir
    fs_opt_.fs_path_ = db_name + "/Filesystem/";
    options.env_->CreateDir(fs_opt.fs_path_);

    // create tera client
    tera::ErrorCode error_code;
    std::string tera_log_prefix = db_name;
    tera_opt_.root_path_ = db_name + "/Tera/";
    options.env->CreateDir(tera_opt_.root_path_);
    tera_opt_.tera_flag_ = options.tera_flag_file_path_;
    tera_opt_.client_ = tera::Client::NewClient(tera_opt_.tera_flag_, tera_log_prefix, &error_code);
    assert(tera_opt_.client_);

    // create db schema table (kv mode)
    std::string schema_table_name = db_name + "#schema";
    tera::TableDescriptor schema_desc(schema_table_name);
    assert(tera_opt_.client_->CreateTable(schema_desc));

    tera_opt_.schema_table_ = tera_opt_.client_->OpenTable(schema_table_name, &error_code);
    assert(tera_opt_.schema_table_);
}

int DatabaseImpl::CreateDB(const Options& options,
                           const std::string& db_name,
                           Database** db) {
    DatabaseImpl* db_ptr = new DatabaseImpl(options, db_name);
    assert(db_ptr);
    *db = db_ptr;
    return 0;
}

int DatabaseImpl::CreateTable(const CreateRequest* req,
                             CreateResponse* resp) {
    assert(db_name_ == req->db_name);
    std::vector<TableDescription>::iterator it;
    for (it = req->table_descriptor_list.begin();
         it != req->table_descriptor_list.end();
         ++it) {
        if (table_map_.find(it->table_name) != table_map_.end()) {
            continue;
        }
        // construct memory structure
        TableImpl* table_ptr;
        assert(InternalCreateTable(*it, &table_ptr));
        table_map_[it->table_name] = table_ptr;
    }
    return 0;
}

int DatabaseImpl::InternalCreateTable(const TableDescription& table_desc, TableImpl** table) {
    std::string& table_name = table_desc.table_name;
    // init fs adapter
    FilesystemAdapter fs_adapter;
    fs_adapter.root_path_ = fs_opt_.fs_path_ + "/" + table_name + "/";
    fs_adapter.env_ = options_.env_;

    //init tera adapter
    TeraAdapter tera_adapter;
    tera_adapter.opt_ = tera_opt_;
    tera_adapter.table_prefix_ = db_name_;

    TableImpl* table_ptr = new TableImpl(table_desc, tera_adapter, fs_adapter);
    assert(table_ptr);
    *table = table_ptr;
    return 0;
}

} // namespace mdt
