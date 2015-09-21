// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <tera.h>
#include "sdk/sdk.h"
#include "sdk/table_impl.h"
#include "proto/kv.pb.h"

#include <glog/logging.h>

namespace mdt {

int TableImpl::OpenTable(const std::string& db_name, const TeraOptions& tera_opt,
                         const FilesystemOptions& fs_opt, const TableDescription& table_desc,
                         Table** table_ptr) {
    const std::string& table_name = table_desc.table_name;
    // init fs adapter
    FilesystemAdapter fs_adapter;
    fs_adapter.root_path_ = fs_opt.fs_path_ + "/" + table_name + "/";
    LOG(INFO) << "data file dir: " << fs_adapter.root_path_;
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

    tera::ErrorCode error_code;

    // open primary key table
    std::string primary_table_name = tera_.table_prefix_ + "#pri#" + table_desc.table_name;
    //std::string primary_table_name = GetPrimaryTable(table_desc.table_name);
    LOG(INFO) << "open primary table: " << primary_table_name;
    tera::Table* primary_table = tera_.opt_.client_->OpenTable(primary_table_name, &error_code);
    assert(primary_table);
    tera_.tera_table_map_[primary_table_name] = primary_table;

    // open index key table
    std::vector<IndexDescription>::iterator it;
    for (it = table_desc_.index_descriptor_list.begin();
         it != table_desc_.index_descriptor_list.end();
         ++it) {
        std::string index_table_name = tera_.table_prefix_ + "#" + table_desc.table_name + "#" + it->index_name;
        //std::string index_table_name = GetIndexTable(it->index_name);
        LOG(INFO) << "open index table: " << index_table_name;
        tera::Table* index_table = tera_.opt_.client_->OpenTable(index_table_name, &error_code);
        assert(index_table);
        tera_.tera_table_map_[index_table_name] = index_table;
    }
}

void PutCallback(tera::RowMutation* row) {
    PutContext* context = (PutContext*)row->GetContext();
    if (context->counter_.Dec() == 0) {
        context->callback_(context->table_, context->req_, context->resp_,
                           context->callback_param_);
        LOG(INFO) << "put callback";
        delete context;
    }
}

int TableImpl::Put(const StoreRequest* req, StoreResponse* resp, StoreCallback callback,
                   void* callback_param) {
    // add data into fs
    FileLocation location;
    DataWriter* writer = GetDataWriter();
    writer->AddRecord(req->data, &location);

    std::string null_value;
    null_value.clear();
    PutContext* context = new PutContext(this, req, resp, callback, callback_param);
    context->counter_.Inc();

    // update primary table
    tera::Table* primary_table = GetPrimaryTable(table_desc_.table_name);
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
        tera::Table* index_table = GetIndexTable(it->index_name);
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
        context->callback_(context->table_, context->req_, context->resp_,
                           context->callback_param_);
        delete context;
    }
    return 0;
}

int TableImpl::Get(const SearchRequest* req, SearchResponse* resp, SearchCallback callback,
                   void* callback_param) {
    std::vector<IndexConditionExtend> index_cond_ex_list;
    ExtendIndexCondition(req->index_condition_list, &index_cond_ex_list);


    std::vector<std::string> primary_key_list;
    GetPrimaryKeys(index_cond_ex_list, req->start_timestamp,
                   req->end_timestamp, &primary_key_list);

    GetRows(primary_key_list, &resp->result_stream);
    return 0;
}

Status TableImpl::ExtendIndexCondition(const std::vector<IndexCondition>& index_condition_list,
                                       std::vector<IndexConditionExtend>* index_condition_ex_list) {
    std::map<std::string, IndexConditionExtend> dedup_map;
    std::vector<IndexCondition>::const_iterator it = index_condition_list.begin();
    for (; it != index_condition_list.end(); ++it) {
        const IndexCondition& index_cond = *it;
        std::map<std::string, IndexConditionExtend>::iterator ex_it;
        ex_it = dedup_map.find(index_cond.index_name);
        if (ex_it == dedup_map.end()) {
            IndexConditionExtend& index_cond_ex = dedup_map[index_cond.index_name];
            index_cond_ex.index_name = index_cond.index_name;
            index_cond_ex.comparator = index_cond.comparator;
            index_cond_ex.compare_value1 = index_cond.compare_value;
            continue;
        }

        IndexConditionExtend& index_cond_ex = ex_it->second;
        if (index_cond_ex.comparator == (COMPARATOR)kBetween) {
            // invalid param
            return Status::InvalidArgument("");
        }

        if (index_cond.comparator == kLess || index_cond.comparator == kLessEqual) {
            if (index_cond_ex.comparator != kGreater && index_cond_ex.comparator != kGreaterEqual) {
                return Status::InvalidArgument("");
            }
            index_cond_ex.comparator = (COMPARATOR)kBetween;
            index_cond_ex.compare_value2 = index_cond.compare_value;
            index_cond_ex.flag1 = (index_cond_ex.comparator == kGreaterEqual);
            index_cond_ex.flag2 = (index_cond.comparator == kLessEqual);
        } else if (index_cond.comparator == kGreater || index_cond.comparator == kGreaterEqual) {
            if (index_cond_ex.comparator != kLess && index_cond_ex.comparator != kLessEqual) {
                return Status::InvalidArgument("");
            }
            index_cond_ex.comparator = (COMPARATOR)kBetween;
            index_cond_ex.compare_value2 = index_cond_ex.compare_value1;
            index_cond_ex.compare_value1 = index_cond.compare_value;
            index_cond_ex.flag1 = (index_cond.comparator == kGreaterEqual);
            index_cond_ex.flag2 = (index_cond_ex.comparator == kLessEqual);
        } else {
            // invalid param
            return Status::InvalidArgument("");
        }
    }

    std::map<std::string, IndexConditionExtend>::const_iterator ex_it = dedup_map.begin();
    for (; ex_it != dedup_map.end(); ++ex_it) {
        index_condition_ex_list->push_back(ex_it->second);
    }

    return Status::OK();
}

void TableImpl::GetPrimaryKeys(const std::vector<IndexConditionExtend>& index_condition_ex_list,
                               int64_t start_timestamp, int64_t end_timestamp,
                               std::vector<std::string>* primary_key_list) {
    for (size_t i = 0; i < index_condition_ex_list.size(); i++) {
        const IndexConditionExtend& index_cond_ex = index_condition_ex_list[i];
        const std::string& index_name = index_cond_ex.index_name;
        tera::Table* index_table = GetIndexTable(index_name);
        tera::ScanDescriptor* scan_desc = NULL;
        switch (index_cond_ex.comparator) {
        case kEqualTo:
            scan_desc = new tera::ScanDescriptor(index_cond_ex.compare_value1);
            scan_desc->SetEnd(index_cond_ex.compare_value1 + '\0');
            break;
        case kNotEqualTo:
            abort();
            break;
        case kLess:
            scan_desc = new tera::ScanDescriptor("");
            scan_desc->SetEnd(index_cond_ex.compare_value1);
            break;
        case kLessEqual:
            scan_desc = new tera::ScanDescriptor("");
            scan_desc->SetEnd(index_cond_ex.compare_value1 + '\0');
            break;
        case kGreater:
            scan_desc = new tera::ScanDescriptor(index_cond_ex.compare_value1 + '\0');
            scan_desc->SetEnd("");
            break;
        case kGreaterEqual:
            scan_desc = new tera::ScanDescriptor(index_cond_ex.compare_value1);
            scan_desc->SetEnd("");
            break;
        case kBetween:
            if (index_cond_ex.flag1) {
                scan_desc = new tera::ScanDescriptor(index_cond_ex.compare_value1);
            } else {
                scan_desc = new tera::ScanDescriptor(index_cond_ex.compare_value1 + '\0');
            }
            if (index_cond_ex.flag2) {
                scan_desc->SetEnd(index_cond_ex.compare_value2 + '\0');
            } else {
                scan_desc->SetEnd(index_cond_ex.compare_value2);
            }
            break;
        default:
            abort();
            break;
        }

        scan_desc->AddColumnFamily("PrimaryKey");
        scan_desc->SetTimeRange(start_timestamp, end_timestamp);
        tera::ErrorCode err;
        tera::ResultStream* result = index_table->Scan(*scan_desc, &err);
        while (!result->Done()) {
            const std::string& primary_key = result->Qualifier();
            primary_key_list->push_back(primary_key);
            result->Next();
        }

        delete scan_desc;
    }
}

Status TableImpl::GetRows(const std::vector<std::string>& primary_key_list,
                          std::vector<ResultStream>* row_list) {
    for (uint32_t i = 0; i < primary_key_list.size(); i++) {
        const std::string& primary_key = primary_key_list[i];
        ResultStream result;
        if (!GetSingleRow(primary_key, &result).ok()) {
            // TODO
        }
        row_list->push_back(result);
    }
    return Status::OK();
}

Status TableImpl::GetSingleRow(const std::string& primary_key, ResultStream* result) {
    tera::Table* primary_table = GetPrimaryTable(table_desc_.table_name);
    tera::RowReader* reader = primary_table->NewRowReader(primary_key);
    reader->AddColumnFamily("Location");
    primary_table->Get(reader);
    while (!reader->Done()) {
        const std::string& location_buffer = reader->Qualifier();
        FileLocation location;
        location.ParseFromString(location_buffer);
        reader->Next();
    }
    return Status::OK();
}

tera::Table* TableImpl::GetPrimaryTable(const std::string& table_name) {
    std::string index_table_name = tera_.table_prefix_ + "#pri#" + table_name;
    tera::Table* table = tera_.tera_table_map_[index_table_name];
    return table;
}

tera::Table* TableImpl::GetIndexTable(const std::string& index_name) {
    std::string index_table_name = tera_.table_prefix_ + "#" + table_desc_.table_name + "#" + index_name;
    tera::Table* table = tera_.tera_table_map_[index_table_name];
    return table;
}

std::string TableImpl::TimeToString() {
    struct timeval now_tv;
    gettimeofday(&now_tv, NULL);
    const time_t seconds = now_tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    char buf[27];
    char* p = buf;
    p += snprintf(p, 27,
            "%04d-%02d-%02d-%02d:%02d:%02d.%06d",
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min,
            t.tm_sec,
            static_cast<int>(now_tv.tv_usec));
    std::string time_buf(buf, 26);
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
    Status s = file_->Append(data);
    if (!s.ok()) {
        return -1;
    }
    location->size_ = data.size();
    file_->Sync();
    location->offset_ = offset_;
    offset_ += location->size_;
    location->fname_ = fname_;
    return 0;
}

} // namespace mdt
