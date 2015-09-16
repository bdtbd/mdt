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
    std::string primary_key = req.primary_key;
    tera::RowMutation* primary_row = primary_table->NewRowMutation(primary_key);
    primary_row->Put("Location", location.SerializeToString(), req.timestamp, null_value);
    primary_row->SetContext(context);
    context->counter_.Inc();
    primary_row->SetCallback(PutCallback);
    primary_table->ApplyMutation(primary_row);

    std::vector<Index>::iterator it;
    for (it = req.index_list.begin();
         it != req.index_list.end();
         ++it) {
        tera::Table* index_table = GetTable(it->index_name);
        std::string index_key = it->index_key;
        tera::RowMutation* index_row = index_table->NewRowMutation(index_key);
        index_row->Put("PrimaryKey", primary_key, req.timestamp, null_value);
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

enum COMPARATOR_EXTEND {
    kBetween = 100
};

struct IndexConditionExtend {
    std::string index_name;
    enum COMPARATOR comparator;
    std::string compare_value1;
    std::string compare_value2;
    bool flag1;
    bool flag2;
};

int TableImpl::Get(const SearchRequest& req, SearchResponse* resp, SearchCallback callback) {
    typedef std::map<std::string, IndexConditionExtend> IndexConditionExtendMap;
    IndexConditionExtendMap dedup_map;
    std::vector<IndexCondition>::iterator it = req.index_condition_list.begin();
    for (; it != req.index_condition_list.end(); ++it) {
        const IndexCondition& index_cond = *it;
        IndexConditionExtendMap::iterator ex_it = dedup_map.find(index_cond.index_name);
        if (ex_it == dedup_map.end()) {
            IndexConditionExtend index_cond_ex;
            ExtendIndexCondition(index_cond, &index_cond_ex);
            dedup_map[index_cond.index_name] = index_cond_ex;
            continue;
        }

        IndexConditionExtend& index_cond_ex = it->second;
        if (index_cond_ex.comparator == kBetween) {
            // invalid param
            return;
        }

        if (index_cond.comparator == kLess || index_cond.comparator == kLessEqual) {
            if (index_cond_ex.comparator != kGreater && index_cond_ex.comparator != kGreaterEqual) {
                return;
            }
            index_cond_ex.comparator = kBetween;
            index_cond_ex.compare_value2 = index_cond.compare_value;
            index_cond_ex.flag1 = (index_cond_ex.comparator == kGreaterEqual);
            index_cond_ex.flag2 = (index_cond.comparator == kLessEqual);
        } else if (index_cond.comparator == kGreater || index_cond.comparator == kGreaterEqual) {
            if (index_cond_ex.comparator != kLess && index_cond_ex.comparator != kLessEqual) {
                return;
            }
            index_cond_ex.comparator = kBetween;
            index_cond_ex.compare_value1 = index_cond.compare_value;
            index_cond_ex.flag1 = (index_cond.comparator == kGreaterEqual);
            index_cond_ex.flag2 = (index_cond_ex.comparator == kLessEqual);
        } else {
            // invalid param
            return
        }
    }

    std::vector<std::string> primary_key_list;
    IndexConditionExtendMap::iterator ex_it = dedup_map.begin();
    for (; ex_it != dedup_map.end(); ++ex_it) {
        const std::string& index_name = ex_it->first;
        const IndexConditionExtend& index_cond_ex = ex_it->second;
        tera::Table* index_table = GetTable(index_name);
        if (index_cond_ex.comparator == kBetween) {
            tera::ScanDescriptor scan_desc(index_cond_ex.compare_value1);
            scan_desc.SetEnd(index_cond_ex.compare_value2);
            scan_desc.AddColumnFamily("PrimaryKey");

            // scan_desc.AddColumnFamily("Location");
            scan_desc.SetTimeRange(req.start_timestamp, req.end_timestamp);
            tera::ErrorCode err;
            tera::ResultStream* result = index_table->Scan(scan_desc, &err);
            while (!result->Done()) {
                const std::string& primary_key = result->Qualifier();
                primary_key_list.push_back(primary_key);
                result->Next();
            }
        } else {
            tera::RowReader* reader = index_table->NewRowReader(index_cond_ex.compare_value1);
            reader->AddColumnFamily("PrimaryKey");
            reader->SetTimeRange(req.start_timestamp, req.end_timestamp);

            index_table->Get(reader);
            while (!reader->Done()) {
                const std::string& primary_key = reader->Qualifier();
                primary_key_list.push_back(primary_key);
                reader->Next();
            }
        }
    }

    for (uint32_t i = 0; i < primary_key_list.size(); i++) {
        const std::string& primary_key = primary_key_list[i];
        tera::Table* primary_table = GetTable(req.table_name);
        tera::RowReader* reader = primary_table->NewRowReader(primary_key);
        reader->AddColumnFamily("Location");
        primary_table->Get(reader);
        while (!reader->Done()) {
            const std::string& location_buffer = reader->Qualifier();
            FileLocation location;
            location.DeserializeFromString(location_buffer);
            reader->Next();
        }
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
    DataWriter* writer = NULL;
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
