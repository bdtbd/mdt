// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/tera.h"
#include "sdk/sdk.h"
#include "sdk/table_impl.h"
#include "proto/kv.pb.h"

namespace mdt {

int TableImpl::OpenTable(const std::string& db_name, const TeraOptions& tera_opt,
                     const FilesystemOptions& fs_opt, const TableDescription& table_desc,
                     Table** table_ptr) {
    const std::string& table_name = table_desc.table_name;
    // init fs adapter
    FilesystemAdapter fs_adapter;
    fs_adapter.root_path_ = fs_opt.fs_path_ + "/" + table_name + "/";
    fs_adapter.env_ = fs_opt.env_;
    fs_adapter.writer_ = NULL;

    //init tera adapter
    TeraAdapter tera_adapter;
    tera_adapter.opt_ = tera_opt;
    tera_adapter.table_prefix_ = db_name;

    *table_ptr = new TableImpl(table_desc, tera_adapter, fs_adapter);
    assert(table_ptr);
    return 0;
}

// TableImpl ops
TableImpl::TableImpl(const TableDescription& table_desc,
                     const TeraAdapter& tera_adapter,
                     const FilesystemAdapter& fs_adapter)
    : table_desc_(table_desc),
    tera_(tera_adapter),
    fs_(fs_adapter) {
    // create fs dir
    fs_.env_->CreateDir(fs_.root_path_);

    // insert schema into schema table
    tera::ErrorCode error_code;
    BigQueryTableSchema schema;
    AssembleTableSchema(table_desc, &schema);
    std::string schema_value;
    schema.SerializeToString(&schema_value);
    tera_.opt_.schema_table_->Put(schema.table_name(), "", "", schema_value, &error_code);

    // create primary key table
    std::string primary_table_name = tera_.table_prefix_ + "#" + table_desc_.table_name;
    tera::TableDescriptor primary_table_desc(primary_table_name);
    tera::LocalityGroupDescriptor* lg = primary_table_desc.AddLocalityGroup("lg");
    lg->SetBlockSize(32 * 1024);
    lg->SetCompress(tera::kSnappyCompress);
    tera::ColumnFamilyDescriptor* cf = primary_table_desc.AddColumnFamily("Location", "lg");
    cf->SetTimeToLive(0);
    tera_.opt_.client_->CreateTable(primary_table_desc, &error_code);

    tera::Table* primary_table = tera_.opt_.client_->OpenTable(primary_table_name, &error_code);
    tera_.tera_table_map_[primary_table_name] = primary_table;

    // create index key table
    std::vector<IndexDescription>::iterator it;
    for (it = table_desc_.index_descriptor_list.begin();
         it != table_desc_.index_descriptor_list.end();
         ++it) {
        std::string index_table_name = tera_.table_prefix_ + "#" + it->index_name;
        tera::TableDescriptor index_table_desc(index_table_name);
        tera::LocalityGroupDescriptor* index_lg = index_table_desc.AddLocalityGroup("lg");
        index_lg->SetBlockSize(32 * 1024);
        index_lg->SetCompress(tera::kSnappyCompress);
        tera::ColumnFamilyDescriptor* index_cf = index_table_desc.AddColumnFamily("PrimaryKey", "lg");
        index_cf->SetTimeToLive(0);
        tera_.opt_.client_->CreateTable(index_table_desc, &error_code);

        tera::Table* index_table = tera_.opt_.client_->OpenTable(index_table_name, &error_code);
        tera_.tera_table_map_[index_table_name] = index_table;
    }
}

int TableImpl::AssembleTableSchema(const TableDescription& table_desc,
                                   BigQueryTableSchema* schema) {
    schema->set_table_name(table_desc.table_name);
    schema->set_primary_key_type(table_desc.primary_key_type);
    std::vector<IndexDescription>::const_iterator it;
    for (it = table_desc.index_descriptor_list.begin();
         it != table_desc.index_descriptor_list.end();
         ++it) {
        IndexSchema* index;
        index = schema->add_index_descriptor_list();
        index->set_index_name(it->index_name);
        index->set_index_key_type(it->index_key_type);
    }
    return 0;
}

int TableImpl::DisassembleTableSchema(const BigQueryTableSchema& schema,
                                         TableDescription* table_desc) {
    return 0;
}

void PutCallback(tera::RowMutation* row) {
    PutContext* context = (PutContext*)row->GetContext();
    if (context->counter_.Dec() == 0) {
        context->callback_(context->req_, context->resp_);
        delete context;
    }
}

int TableImpl::Put(const StoreRequest* req, StoreResponse* resp, StoreCallback callback) {
    // add data into fs
    FileLocation location;
    DataWriter* writer = GetDataWriter();
    writer->AddRecord(req->data, &location);

    std::string null_value;
    null_value.clear();
    PutContext* context = new PutContext(this, req, resp, callback);
    context->counter_.Inc();

    // update primary table
    tera::Table* primary_table = GetTable(req->table_name);
    std::string primary_key = req->primary_key;
    tera::RowMutation* primary_row = primary_table->NewRowMutation(primary_key);
    primary_row->Put("Location", location.SerializeToString(), req->timestamp, null_value);
    primary_row->SetContext(context);
    context->counter_.Inc();
    primary_row->SetCallBack(PutCallback);
    primary_table->ApplyMutation(primary_row);

    std::vector<Index>::const_iterator it;
    for (it = req->index_list.begin();
         it != req->index_list.end();
         ++it) {
        tera::Table* index_table = GetTable(it->index_name);
        std::string index_key = it->index_key;
        tera::RowMutation* index_row = index_table->NewRowMutation(index_key);
        index_row->Put("PrimaryKey", primary_key, req->timestamp, null_value);
        index_row->SetContext(context);
        context->counter_.Inc();
        index_row->SetCallBack(PutCallback);
        index_table->ApplyMutation(index_row);
    }

    if (context->counter_.Dec() == 0) {
        // last one, do something
        context->callback_(context->req_, context->resp_);
        delete context;
    }
    return 0;
}

tera::Table* TableImpl::GetTable(const std::string& table_name) {
    std::string index_table_name = tera_.table_prefix_ + "#" + table_name;
    tera::Table* table = tera_.tera_table_map_[index_table_name];
    return table;
}

std::string TableImpl::TimeToString() {
    struct timeval now_tv;
    gettimeofday(&now_tv, NULL);
    const time_t seconds = now_tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    char buf[64];
    char* p = buf;
    p += snprintf(p, 64,
            "%04d-%02d-%02d-%02d:%02d:%02d.%06d",
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min,
            t.tm_sec,
            static_cast<int>(now_tv.tv_usec));
    std::string time_buf(p);
    return time_buf;
}

DataWriter* TableImpl::GetDataWriter() {
    DataWriter* writer = NULL;
    if (fs_.writer_ == NULL) {
        std::string fname = fs_.root_path_ + "/" + TimeToString() + ".data";
        WritableFile* file;
        fs_.env_->NewWritableFile(fname, &file);
        fs_.writer_ = new DataWriter(fname, file);
    }
    writer = fs_.writer_;
    return writer;
}

// data writer impl
int DataWriter::AddRecord(const std::string& data, FileLocation* location) {
    return 0;
}

} // namespace mdt
