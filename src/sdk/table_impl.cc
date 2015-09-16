// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/table_impl.h"

namespace mdt {

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
        tera::LocalityGroupDescriptor* index_lg = index_tablet_desc.AddLocalityGroup("lg");
        lg->SetBlockSize(32 * 1024);
        lg->SetCompress(tera::kSnappyCompress);
        tera::ColumnFamilyDescriptor* index_cf = index_table_desc.AddColumnFamily("PrimaryKey", "lg");
        tera_.op_.client_->CreateTable(index_table_desc, &error_code);

        tera::Table* index_table = tera_.opt_.client_->OpenTable(index_table_name, &error_code);
        tera_.tera_table_map_[index_table_name] = index_table;
    }
}

int TableImpl::AssembleTableSchema(const TableDescription& table_desc,
                                   BigQueryTableSchema* schema) {
    schema->set_table_name(table_desc.table_name);
    schema->set_primary_key_type(table_desc.primary_key_type);
    std::vector<IndexDescription>::iterator it;
    for (it = table_desc.index_descriptor_list.begin();
         it != table_desc.index_descriptor_list.end();
         ++it) {
        IndexSchema index;
        index.set_index_name(it->index_name);
        index.set_index_key_type(it->index_key_type);
        schema->add_index_descriptor_list(index);
    }
    return 0;
}

int TableImpl::DisassembleTableSchema(const BigQueryTableSchema& schema,
                                         TableDescription* table_desc) {
    return 0;
}

int TableImpl::Put(const StoreRequest& req, StoreResponse* resp, StoreCallback callback) {
    // add data into fs
    FileLocation location;
    DataWriter* writer = GetDataWriter();
    writer.AddRecord(req.data, &location);

    std::string null_value;
    null_value.clear();
    PutContext* context = new PutContext(this, req, resp, callback);
    context->counter_.Inc();

    // update primary table
    tera::Table* primary_table = GetTable(req.table_name);
    // TODO: primary key right?
    std::string primary_key = req.primary_key + req.timestamp;
    tera::RowMutation* primary_row = primary_table->NewRowMutation(primary_key);
    primary_row->Put("Location", location.SerializeToString(), null_value);
    primary_row->SetContext(context);
    context->counter_.Inc();
    primary_row->SetCallback(PutCallback);
    primary_table->ApplyMutation(primary_row);

    std::vector<Index>::iterator it;
    for (it = req.index_list.begin();
         it != req.index_list.end();
         ++it) {
        tera::Table* index_table = GetTable(it->index_name);
        // TODO: index key right?
        std::string index_key = it->index_key + req.timestamp;
        tera::RowMutation* index_row = index_table->NewRowMutation(index_key);
        index_row->Put("PrimaryKey", primary_key, null_value);
        index_row->SetContext(context);
        context->counter_.Inc();
        index_row->SetCallback(PutCallback);
        index_table->ApplyMutation(index_row);
    }

    if (context->counter_.Dec() == 0) {
        // last one, do something
        context->callback_(context->req_, context->resp_);
        delete context;
    }
    return 0;
}

void PutCallback(tera::RowMutation* row) {
    PutContext* context = (PutContext*)row->GetContext();
    if (context->counter_.Dec() == 0) {
        context->callback_(context->req_, context->resp_);
        delete context;
    }
}

tera::Table* TableImpl::GetTable(const std::string& table_name) {
    std::string index_table_name = tera_.table_prefix_ + "#" + table_name;
    tera::Table* table = tera_.tera_table_map_[index_table_name];
    return table;
}

std::string& TableImpl::TimeToString() {
    const uint64_t tid = gettid();
    struct timeval now_tv;
    gettimeofday(&now_tv, NULL);
    const time_t seconds = now_tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    char p[64];
    p += snprintf(p, 64,
            "%04d-%02d-%02d-%02d:%02d:%02d.%06d-%llu",
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min,
            t.tm_sec,
            static_cast<int>(now_tv.tv_usec),
            static_cast<long long unsigned int>(thread_id));
    std::string time_buf(p);
    return time_buf;
}

DataWriter* TableImpl::GetDataWriter() {
    DateWriter* writer = NULL;
    if (fs_.writer_ == NULL) {
        std::string fname = fs_.root_path_ + "/" + TimeToString() + ".data";
        WritableFile* file;
        fs_.env_->NewWritableFile(fname, &file);
        fs_writer = new DataWriter(fname, file);
    }
    writer = fs_.writer_;
    return writer;
}

} // namespace mdt
