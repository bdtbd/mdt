// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <deque>

#include <glog/logging.h>
#include <glog/logging.h>

#include "proto/kv.pb.h"
#include "sdk/sdk.h"
#include "sdk/table_impl.h"
#include <tera.h>

DECLARE_int64(concurrent_write_handle_num);
DECLARE_int64(max_write_handle_seq);
DECLARE_int64(data_size_per_sync);

namespace mdt {

std::ostream& operator << (std::ostream& o, const FileLocation& location) {
    o << "file: " << location.fname_
      << " offset: " << location.offset_
      << " size: " << location.size_;
    return o;
}

int TableImpl::OpenTable(const std::string& db_name, const TeraOptions& tera_opt,
                         const FilesystemOptions& fs_opt, const TableDescription& table_desc,
                         Table** table_ptr) {
    const std::string& table_name = table_desc.table_name;
    // init fs adapter
    FilesystemAdapter fs_adapter;
    fs_adapter.root_path_ = fs_opt.fs_path_ + "/" + table_name + "/";
    LOG(INFO) << "data file dir: " << fs_adapter.root_path_;
    fs_adapter.env_ = fs_opt.env_;

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

    // init write handle list
    nr_write_handle_ = (int)FLAGS_concurrent_write_handle_num;
    cur_write_handle_id_ = 0;
    write_handle_list_.clear();
    WriteHandle write_handle;
    write_handle.write_queue_.clear();
    write_handle.writer_ = NULL;
    for (int i = 0; i < nr_write_handle_; ++i) {
        write_handle_list_.push_back(write_handle);
    }

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

TableImpl::~TableImpl() {
    // free write handle
    //ReleaseDataWriter();
    // TODO: write queue release
}

/////////  batch write /////////////
struct InternalBatchPutCallbackParam {
    BatchStoreRequest* req_;
    StoreResponse* resp_;
    BatchStoreCallback user_callback_;
    void* user_param_;
    Counter counter_; // atomic counter
};

void BatchStoreRequestCallback(Table* table, StoreRequest* request,
                               StoreResponse* response,
                               void* callback_param) {
    TableImpl* table_ptr;
    table_ptr = (TableImpl*)table;
    delete request;
    InternalBatchPutCallbackParam* user_callback_param = (InternalBatchPutCallbackParam*)callback_param;
    if (user_callback_param->counter_.Dec() == 0) {
        delete response;
        user_callback_param->user_callback_(table_ptr,
                                            user_callback_param->req_,
                                            user_callback_param->resp_,
                                            user_callback_param->user_param_);
        delete user_callback_param;
    }
}

int TableImpl::Put(const BatchStoreRequest* request, StoreResponse* response,
                BatchStoreCallback callback, void* callback_param) {
    // construct vector<StoreRequest>
    std::vector<StoreRequest*> req_vec;
    std::vector<std::string>::const_iterator it = request->data_list.begin();
    for (; it != request->data_list.end(); ++it) {
        StoreRequest* req = new StoreRequest();
        req->primary_key = request->primary_key;
        req->timestamp = request->timestamp;
        req->index_list = request->index_list;
        req->data = *it;
        req_vec.push_back(req);
    }

    StoreResponse* resp = new StoreResponse();
    InternalBatchPutCallbackParam* user_callback_param = new InternalBatchPutCallbackParam();
    user_callback_param->req_ = (BatchStoreRequest*)request;
    user_callback_param->resp_ = response;
    user_callback_param->user_callback_ = callback;
    user_callback_param->user_param_ = callback_param;
    user_callback_param->counter_.Inc();

    std::vector<StoreRequest*>::iterator iter = req_vec.begin();
    for (; iter != req_vec.end(); ++iter) {
        user_callback_param->counter_.Inc();
        Put(*iter, resp, BatchStoreRequestCallback, user_callback_param);
    }

    StoreRequest* null_req = new StoreRequest();
    null_req->primary_key = "<MagicKey>";
    BatchStoreRequestCallback(this, null_req, resp, user_callback_param);
    return 0;
}

///////////  High concurrency write interface ////////////////
struct DefaultUserCallbackParam {
    CondVar* cond_;
};

void DefaultUserCallback(Table* table, StoreRequest* request,
                         StoreResponse* response,
                         void* callback_param) {
    DefaultUserCallbackParam* param = (DefaultUserCallbackParam*)callback_param;
    param->cond_->Signal();
}

// Concurrence Put interface:
//      1. wait and lock
//      2. merge request while wait
//      3. unlock, write batch
//      4. for each raw write: PutIndex()
//      5. lock, resume other write
int TableImpl::Put(const StoreRequest* req, StoreResponse* resp,
                   StoreCallback callback, void* callback_param) {
    // try to construct Default callback
    Mutex mu;
    CondVar cond(&mu);
    DefaultUserCallbackParam param;
    param.cond_ = &cond;
    if (callback == NULL) {
        callback = DefaultUserCallback;
        callback_param = &param;
    }

    // construct WriteContext
    WriteContext context(&write_mutex_);
    context.req_ = req;
    context.resp_ = resp;
    context.callback_ = callback;
    context.callback_param_ = callback_param;
    context.sync_ = true;
    context.done_ = false;

    // select write handle
    WriteHandle* write_handle = GetWriteHandle();

    // wait and lock
    LOG(INFO) << ">>>>> begin put, ctx " << (uint64_t)(&context);
    write_mutex_.Lock();
    write_handle->write_queue_.push_back(&context);
    while (!context.done_ && (&context != write_handle->write_queue_.front())) {
        LOG(INFO) << "===== waitlock put, ctx " << (uint64_t)(&context);
        context.cv_.Wait();
    }
    if (context.done_) {
        write_mutex_.Unlock();
        // write finish, if sync write, just wait callback
        if (callback == DefaultUserCallback) {
            param.cond_->Wait();
        }
        return 0;
    }

    // merge WriteContext
    WriteBatch wb;
    std::deque<WriteContext*>::iterator iter = write_handle->write_queue_.begin();
    for (; iter != write_handle->write_queue_.end(); ++iter) {
        wb.Append(*iter);
    }

    // unlock do io
    write_mutex_.Unlock();
    LOG(INFO) << ">>>>> lock put, ctx " << (uint64_t)(&context);

    // batch write file system
    FileLocation location;
    DataWriter* writer = GetDataWriter(write_handle);
    writer->AddRecord(wb.rep_, &location);

    // write index table
    std::vector<WriteContext*>::iterator it = wb.context_list_.begin();
    for (; it != wb.context_list_.end(); ++it) {
        WriteContext* ctx = *it;
        FileLocation data_location = location;
        data_location.size_ = ctx->req_->data.size();
        data_location.offset_ += ctx->offset_;
        LOG(INFO) << "put record: offset " << data_location.offset_ << ", size " << data_location.size_ 
                << ", pri key " << ctx->req_->primary_key;
        WriteIndexTable(ctx->req_, ctx->resp_, ctx->callback_, ctx->callback_param_, data_location);
    }

    // lock, resched other WriteContext
    VLOG(30) << "<<<<< unlock put";
    write_mutex_.Lock();
    WriteContext* last_writer = wb.context_list_.back();
    while (true) {
        WriteContext* ready = write_handle->write_queue_.front();
        write_handle->write_queue_.pop_front();
        if (ready != &context) {
            ready->done_ = true;
            ready->cv_.Signal();
        }
        if (ready == last_writer) break;
    }
    if (!write_handle->write_queue_.empty()) {
        write_handle->write_queue_.front()->cv_.Signal();
    }

    write_mutex_.Unlock();
    LOG(INFO) << "<<<<< finish put, ctx " << (uint64_t)(&context);
    // write finish, if sync write, just wait callback
    if (callback == DefaultUserCallback) {
        param.cond_->Wait();
    }
    return 0;
}

void PutCallback(tera::RowMutation* row) {
    PutContext* context = (PutContext*)row->GetContext();
    // the last one invoke user callback
    if (context->counter_.Dec() == 0) {
        context->callback_(context->table_, (StoreRequest*)context->req_, context->resp_,
                context->callback_param_);
        VLOG(12) << "put callback";
        delete context;
    }
}

int TableImpl::WriteIndexTable(const StoreRequest* req, StoreResponse* resp,
                               StoreCallback callback, void* callback_param,
                               FileLocation& location) {
    std::string null_value;
    null_value.clear();
    PutContext* context = new PutContext(this, req, resp, callback, callback_param);
    context->counter_.Set(1 + req->index_list.size());

    // update primary table
    VLOG(10) << "write pri : " << req->primary_key;
    tera::Table* primary_table = GetPrimaryTable(table_desc_.table_name);
    std::string primary_key = req->primary_key;
    VLOG(12) << " write pri table, primary key: " << primary_key;
    tera::RowMutation* primary_row = primary_table->NewRowMutation(primary_key);
    primary_row->Put(kPrimaryTableColumnFamily, location.SerializeToString(), req->timestamp, null_value);
    primary_row->SetContext(context);
    primary_row->SetCallBack(PutCallback);
    primary_table->ApplyMutation(primary_row);

    std::vector<Index>::const_iterator it;
    for (it = req->index_list.begin();
         it != req->index_list.end();
         ++it) {
        tera::Table* index_table = GetIndexTable(it->index_name);
        std::string index_key = it->index_key;
        VLOG(12) << " write index table: " << it->index_name << ", index key: " << index_key;
        tera::RowMutation* index_row = index_table->NewRowMutation(index_key);
        index_row->Put(kIndexTableColumnFamily, primary_key, req->timestamp, null_value);
        index_row->SetContext(context);
        index_row->SetCallBack(PutCallback);
        index_table->ApplyMutation(index_row);
    }

    return 0;
}

Status TableImpl::Get(const SearchRequest* req, SearchResponse* resp, SearchCallback callback,
                      void* callback_param) {
    Status s;

    std::vector<std::string> primary_key_list;
    if (!req->primary_key.empty()) {
        primary_key_list.push_back(req->primary_key);
    } else {
        VLOG(10) << "sanity index conditions";
        std::vector<IndexConditionExtend> index_cond_ex_list;
        s = ExtendIndexCondition(req->index_condition_list, &index_cond_ex_list);

        if (s.ok()) {
            VLOG(10) << "get primary keys";
            s = GetPrimaryKeys(index_cond_ex_list, req, &primary_key_list);
        }
    }

    if (s.ok()) {
        VLOG(10) << "get rows";
        s = GetRows(primary_key_list, &resp->result_stream);
    }
    return s;
}

Status TableImpl::ExtendIndexCondition(const std::vector<IndexCondition>& index_condition_list,
                                       std::vector<IndexConditionExtend>* index_condition_ex_list) {
    std::map<std::string, IndexConditionExtend> dedup_map;
    std::vector<IndexCondition>::const_iterator it = index_condition_list.begin();
    for (; it != index_condition_list.end(); ++it) {
        const IndexCondition& index_cond = *it;
        std::map<std::string, IndexConditionExtend>::iterator ex_it;
        ex_it = dedup_map.find(index_cond.index_name);
        VLOG(10) << "sanity index : " << index_cond.index_name;
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

Status TableImpl::GetPrimaryKeys(const std::vector<IndexConditionExtend>& index_condition_ex_list,
                                 const SearchRequest* req,
                                 std::vector<std::string>* primary_key_list) {
    size_t nr_index_table = index_condition_ex_list.size();
    std::vector<std::vector<std::string> > pri_vec(nr_index_table);

    primary_key_list->clear();
    if (nr_index_table <= 0) {
        return Status::OK();
    }

    for (size_t i = 0; i < nr_index_table; i++) {
        bool skip_scan = false;
        const IndexConditionExtend& index_cond_ex = index_condition_ex_list[i];
        const std::string& index_name = index_cond_ex.index_name;
        tera::Table* index_table = GetIndexTable(index_name);
        LOG(INFO) << "select op, get primary key, index name " << index_name;
        tera::ScanDescriptor* scan_desc = NULL;
        switch ((int)index_cond_ex.comparator) {
        case kEqualTo:
            scan_desc = new tera::ScanDescriptor(index_cond_ex.compare_value1);
            scan_desc->SetEnd(index_cond_ex.compare_value1 + '\0');
            break;
        case kNotEqualTo:
            LOG(WARNING) << "Scan not support !=";
            skip_scan = true;
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
            LOG(WARNING) << "Scan: unknown cmp";
            skip_scan = true;
            break;
        }

        if (skip_scan) continue;

        scan_desc->AddColumnFamily(kIndexTableColumnFamily);
        scan_desc->SetTimeRange(req->end_timestamp, req->start_timestamp);
        tera::ErrorCode err;
        tera::ResultStream* result = index_table->Scan(*scan_desc, &err);

        pri_vec[i].clear();
        int32_t num_pkey = 0;
        while (!result->Done()) {
            if (req->limit <= num_pkey) break;

            // TODO: sync scan is enough
            const std::string& primary_key = result->Qualifier();
            LOG(INFO) << "select op, primary key: " << primary_key;
            pri_vec[i].push_back(primary_key);
            num_pkey++;

            result->Next();
        }
        delete result;
        delete scan_desc;
    }

    PrimaryKeyMergeSort(pri_vec, primary_key_list);
    return Status::OK();
}

///////   fast merge sort algorithm   ////////
Status TableImpl::PrimaryKeyMergeSort(std::vector<std::vector<std::string> >& pri_vec,
                                      std::vector<std::string>* primary_key_list) {
    uint32_t nr_stream = pri_vec.size();
    if (nr_stream <= 0) {
        return Status::OK();
    }

    // collect iterator, min_key
    std::string min_key;
    std::vector<std::vector<std::string>::iterator > iter_vec(nr_stream);
    min_key = (pri_vec[0])[0];
    for (uint32_t i = 0; i < nr_stream; i++) {
        std::vector<std::string>::iterator it = pri_vec[i].begin();
        iter_vec[i] = it;
        if (min_key.compare((pri_vec[i])[0]) > 0) min_key = (pri_vec[i])[0];
    }

    while (1) {
        std::string max_key = min_key;
        // collect min && max key in all stream
        for (uint32_t i = 0; i < nr_stream; i++) {
            if (min_key.compare(*(iter_vec[i])) != 0) {
                // binary search
                iter_vec[i] = std::lower_bound(iter_vec[i], pri_vec[i].end(), min_key);
                if (iter_vec[i] == pri_vec[i].end()) {
                    // this stream finish, so exit
                    return Status::OK();
                }
                if (max_key.compare(*(iter_vec[i])) < 0) max_key = *(iter_vec[i]);
            }
        }

        // collect result
        if (min_key.compare(max_key) == 0) {
            primary_key_list->push_back(max_key);
            ++(iter_vec[0]);
            if (iter_vec[0] == pri_vec[0].end()) {
                return Status::OK();
            }
            min_key = *(iter_vec[0]);
        } else {
            // drop invalid result, update min_key, use max_key enhance merge sort
            min_key = max_key;
        }
    }
    assert(0);
    return Status::OK();
}

Status TableImpl::GetRows(const std::vector<std::string>& primary_key_list,
                          std::vector<ResultStream>* row_list) {
    Status s;
    for (uint32_t i = 0; i < primary_key_list.size(); i++) {
        const std::string& primary_key = primary_key_list[i];
        ResultStream result;
        s = GetSingleRow(primary_key, &result);
        if (s.ok()) {
            VLOG(5) << "get primary data: " << primary_key;
            row_list->push_back(result);
        } else {
            LOG(WARNING) << "fail to get primary data: " << primary_key;
        }
    }

    if (primary_key_list.size() > 0 && row_list->size() == 0) {
        return Status::NotFound("row list not found");
    }
    return Status::OK();
}

Status TableImpl::GetSingleRow(const std::string& primary_key, ResultStream* result) {
    tera::Table* primary_table = GetPrimaryTable(table_desc_.table_name);
    tera::RowReader* reader = primary_table->NewRowReader(primary_key);
    LOG(INFO) << "get single row: primary_key " << primary_key << ", table name " << table_desc_.table_name;
    reader->AddColumnFamily(kPrimaryTableColumnFamily);
    primary_table->Get(reader);
    while (!reader->Done()) {
        const std::string& location_buffer = reader->Qualifier();
        FileLocation location;
        location.ParseFromString(location_buffer);
        LOG(INFO) << "read data from file " << location;
        std::string data;
        // TODO: async scan
        Status s = ReadDataFromFile(location, &data);
        if (s.ok()) {
            VLOG(5) << "read data from " << location;
            result->result_data_list.push_back(data);
        } else {
            LOG(WARNING) << "fail to read data from " << location << " error: " << s.ToString();
        }
        reader->Next();
    }

    if (result->result_data_list.size() == 0) {
        return Status::NotFound("row not found");
    }
    result->primary_key = primary_key;
    return Status::OK();
}

Status TableImpl::ReadDataFromFile(const FileLocation& location, std::string* data) {
    RandomAccessFile* file = OpenFileForRead(location.fname_);
    if (file == NULL) {
        return Status::NotFound("file not found");
    }
    char* scratch = new char[location.size_];
    Slice result;
    Status s = file->Read(location.offset_, location.size_, &result, scratch);
    if (s.ok()) {
        data->assign(result.data(), result.size());
    }
    return s;
}

tera::Table* TableImpl::GetPrimaryTable(const std::string& table_name) {
    std::string index_table_name = tera_.table_prefix_ + "#pri#" + table_name;
    VLOG(12) << "get primary table: " << index_table_name;
    tera::Table* table = tera_.tera_table_map_[index_table_name];
    return table;
}

tera::Table* TableImpl::GetIndexTable(const std::string& index_name) {
    std::string index_table_name = tera_.table_prefix_ + "#" + table_desc_.table_name + "#" + index_name;
    VLOG(12) << "get index table: " << index_table_name;
    tera::Table* table = tera_.tera_table_map_[index_table_name];
    return table;
}

std::string TableImpl::TimeToString() {
#ifdef OS_LINUX
    pid_t tid = syscall(SYS_gettid);
#else
    pthread_t tid = pthread_self();
#endif
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));

    struct timeval now_tv;
    gettimeofday(&now_tv, NULL);
    const time_t seconds = now_tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    char buf[34];
    char* p = buf;
    p += snprintf(p, 34,
            "%04d-%02d-%02d-%02d:%02d:%02d.%06d.%06lu",
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min,
            t.tm_sec,
            static_cast<int>(now_tv.tv_usec),
            (unsigned long)thread_id);
    std::string time_buf(buf, 33);
    return time_buf;
}

// cache file handler
RandomAccessFile* TableImpl::OpenFileForRead(const std::string& filename) {
    MutexLock l(&file_mutex_);

    // get file from cache
    std::map<std::string, RandomAccessFile*>::iterator it = file_map_.find(filename);
    if (it != file_map_.end()) {
        VLOG(5) << "find file in cache: " << filename;
        return it->second;
    }

    // open file
    file_mutex_.Unlock();
    RandomAccessFile* file = NULL;
    Status s = fs_.env_->NewRandomAccessFile(filename, &file);
    file_mutex_.Lock();
    if (!s.ok()) {
        LOG(WARNING) << "fail to open file: " << filename << " error: " << s.ToString();
        return NULL;
    }

    // insert into cache
    VLOG(5) << "open file: " << filename;
    it = file_map_.find(filename);
    if (file_map_.find(filename) == file_map_.end()) {
        file_map_[filename] = file;
    } else {
        delete file;
        file = it->second;
    }
    return file;
}

// DataWriter Impl
DataWriter* TableImpl::GetDataWriter(WriteHandle* write_handle) {
    DataWriter* writer = NULL;
    if (write_handle->writer_ && write_handle->writer_->SwitchDataFile()) {
        LOG(INFO) << "data file too large, switch";
        delete write_handle->writer_;
        write_handle->writer_ = NULL;
    }
    if (write_handle->writer_ == NULL) {
        std::string fname = fs_.root_path_ + "/" + TimeToString() + ".data";
        WritableFile* file;
        fs_.env_->NewWritableFile(fname, &file);
        write_handle->writer_ = new DataWriter(fname, file);
    }
    writer = write_handle->writer_;
    return writer;
}

void TableImpl::ReleaseDataWriter(WriteHandle* write_handle) {
    // TODO
    if (write_handle->writer_) {
        delete write_handle->writer_;
    }
    write_handle->writer_ = NULL;
}

TableImpl::WriteHandle* TableImpl::GetWriteHandle() {
    MutexLock mu(&write_mutex_);

    if (cur_write_handle_seq_++ >= FLAGS_max_write_handle_seq) {
        cur_write_handle_seq_ = 0;
        cur_write_handle_id_ = (cur_write_handle_id_ + 1) % nr_write_handle_;
    }

    WriteHandle* write_handle = (WriteHandle*)&(write_handle_list_[cur_write_handle_id_]);
    LOG(INFO) << "id " << cur_write_handle_id_ << ", seq " << cur_write_handle_seq_
            << ", write_handle addr " << (uint64_t)write_handle;
    return write_handle;
}

int DataWriter::AddRecord(const std::string& data, FileLocation* location) {
    Status s = file_->Append(data);
    if (!s.ok()) {
        return -1;
    }
    location->size_ = data.size();
    location->offset_ = offset_;
    offset_ += location->size_;
    location->fname_ = fname_;
    // per 256KB, trigger sync
    assert(offset_ >= cur_sync_offset_);
    if ((offset_ - cur_sync_offset_) > (int32_t)FLAGS_data_size_per_sync) {
        file_->Sync();
        cur_sync_offset_ = offset_;
    }
    LOG(INFO) << "add record, offset " << location->offset_
        << ", size " << location->size_;
    return 0;
}

// WriteBatch format
//      body := value(non fixed len) + red_zone(8 Bytes magic code)
int WriteBatch::Append(WriteContext* context) {
    context->offset_ = rep_.size();
    rep_.append(context->req_->data);
    rep_.append("aaccbbdd");

    context_list_.push_back(context);
    return 0;
}

} // namespace mdt
