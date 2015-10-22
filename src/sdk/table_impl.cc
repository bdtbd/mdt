// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <deque>

#include <glog/logging.h>
#include <glog/logging.h>
#include <boost/bind.hpp>

#include "proto/kv.pb.h"
#include "sdk/sdk.h"
#include "sdk/table_impl.h"
#include "util/string_util.h"
#include <tera.h>

DECLARE_int64(concurrent_write_handle_num);
DECLARE_int64(max_write_handle_seq);
DECLARE_int64(data_size_per_sync);
DECLARE_bool(use_tera_async_write);
DECLARE_int64(write_batch_queue_size);
DECLARE_int64(request_queue_flush_internal);
DECLARE_int64(max_timestamp_table_num);

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
    fs_(fs_adapter),
    queue_timer_stop_(false),
    queue_timer_cv_(&queue_timer_mu_) {
    // create fs dir
    fs_.env_->CreateDir(fs_.root_path_);

    // create timer
    pthread_create(&timer_tid_, NULL, &TableImpl::TimerThreadWrapper, this);

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
    // open timestamp index table
    nr_timestamp_table_ = (int)FLAGS_max_timestamp_table_num;
    cur_timestamp_table_id_ = 0;
    for (int i = 0; i < nr_timestamp_table_; i++) {
        char ts_name[32];
        sprintf(ts_name, "timestamp#%d", i);
        std::string index_table_name = tera_.table_prefix_ + "#" + table_desc.table_name + "#" + ts_name;
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
    //

    // stop timer, flush request
    queue_timer_mu_.Lock();
    queue_timer_stop_ = true;
    queue_timer_cv_.Wait();
    queue_timer_mu_.Unlock();

    std::vector<WriteContext*> local_queue;
    WriteContext* context = NULL;
    GetAllRequest(&context, &local_queue);
    // Batch write
    if (context) {
        InternalBatchWrite(context, local_queue);
        delete context;
    }

    return;
}

/////////  batch write /////////////
struct DefaultBatchWriteCallbackParam {
    CondVar* cond_;
    BatchWriteContext* ctx_;
    Counter counter_; // last one wakeup wait thread
};

void DefaultBatchWriteCallback(Table* table, StoreRequest* request,
                       StoreResponse* response,
                       void* callback_param) {
    DefaultBatchWriteCallbackParam* param = (DefaultBatchWriteCallbackParam*)callback_param;
    if (response->error != 0) {
        param->ctx_->error = response->error;
    }
    if (param->counter_.Dec() == 0) {
        if (param->ctx_->callback == NULL) {
            param->cond_->Signal();
        } else {
            param->ctx_->callback(table, param->ctx_);
            delete param;
        }
    }
}

int TableImpl::BatchWrite(BatchWriteContext* ctx) {
    if (ctx == NULL || ctx->nr_batch <= 0) {
        return 0;
    }
    ctx->error = 0;

    // if sync write, construct default callback
    Mutex mu;
    CondVar cond(&mu);
    DefaultBatchWriteCallbackParam param;
    param.cond_ = &cond;

    StoreCallback internal_callback = DefaultBatchWriteCallback;
    void* internal_param = NULL;
    if (ctx->callback == NULL) {
        // sync batchwrite
        param.ctx_ = ctx;
        param.counter_.Set(ctx->nr_batch);
        internal_param = &param;
    } else {
        // async batchwrite
        DefaultBatchWriteCallbackParam* async_param = new DefaultBatchWriteCallbackParam;
        async_param->ctx_ = ctx;
        async_param->counter_.Set(ctx->nr_batch);
        internal_param = async_param;
    }

    // first request need batch control
    WriteContext* context = new WriteContext(&write_mutex_);
    context->req_ = &(ctx->req[0]);
    context->resp_ = &(ctx->resp[0]);
    context->callback_ = internal_callback;
    context->callback_param_ = internal_param;
    context->sync_ = true;
    context->is_wait_ = false;
    context->done_ = false;

    std::vector<WriteContext*> local_queue;
    for (int i = 1; i < ctx->nr_batch; i++) {
        WriteContext* write_ctx = new WriteContext(&write_mutex_);
        write_ctx->req_ = &(ctx->req[i]);
        write_ctx->resp_ = &(ctx->resp[i]);
        write_ctx->callback_ = internal_callback;
        write_ctx->callback_param_ = internal_param;
        write_ctx->sync_ = true;
        write_ctx->is_wait_ = false;
        write_ctx->done_ = false;
        local_queue.push_back(write_ctx);
    }

    // exec batch write
    InternalBatchWrite(context, local_queue);

    // sync wait
    if (context->callback_ == DefaultBatchWriteCallback) {
        param.cond_->Wait();
    }
    delete context;
    return 0;
}

///////////  High concurrency write interface ////////////////
// cache req optimise for write file system with high latency in ASYNC write tera mode
bool TableImpl::SubmitRequest(WriteContext* context, std::vector<WriteContext*>* local_queue) {
    queue_mutex_.Lock();
    // only async write can use batch
    if (FLAGS_use_tera_async_write && (batch_queue_.size() < (uint32_t)FLAGS_write_batch_queue_size)) {
        batch_queue_.push_back(context);
        queue_mutex_.Unlock();
        return true;
    }
    // copy to local queue
    std::vector<WriteContext*>::iterator wc_it = batch_queue_.begin();
    for (; wc_it != batch_queue_.end(); ++wc_it) {
        local_queue->push_back(*wc_it);
    }
    batch_queue_.clear();
    queue_mutex_.Unlock();
    return false;
}

void TableImpl::GetAllRequest(WriteContext** context_ptr, std::vector<WriteContext*>* local_queue) {
    *context_ptr = NULL;
    local_queue->clear();
    queue_mutex_.Lock();
    std::vector<WriteContext*>::iterator it = batch_queue_.begin();
    for (; it != batch_queue_.end(); ++it) {
        if (*context_ptr == NULL) {
            *context_ptr = *it;
        } else {
            local_queue->push_back(*it);
        }
    }
    batch_queue_.clear();
    queue_mutex_.Unlock();
    return;
}

void* TableImpl::TimerThreadWrapper(void* arg) {
    reinterpret_cast<TableImpl*>(arg)->QueueTimerFunc();
    return NULL;
}

void TableImpl::QueueTimerFunc() {
    while (1) {
        // Get request from queue
        std::vector<WriteContext*> local_queue;
        WriteContext* context = NULL;
        GetAllRequest(&context, &local_queue);
        // Batch write
        if (context) {
            InternalBatchWrite(context, local_queue);
            delete context;
        }

        // timer controller
        queue_timer_mu_.Lock();
        if (queue_timer_stop_) {
            queue_timer_mu_.Unlock();
            queue_timer_cv_.Signal();
            return;
        }
        queue_timer_mu_.Unlock();
        usleep(FLAGS_request_queue_flush_internal);
    }
    return;
}

// timer context
int TableImpl::InternalBatchWrite(WriteContext* context, std::vector<WriteContext*>& ctx_queue) {
    // Get write handle
    WriteHandle* write_handle = GetWriteHandle();

    VLOG(20) << ">>>>> begin put, ctx " << (uint64_t)(context);
    write_mutex_.Lock();
    write_handle->write_queue_.push_back(context);
    std::vector<WriteContext*>::iterator wc_it = ctx_queue.begin();
    for (; wc_it != ctx_queue.end(); ++wc_it) {
        write_handle->write_queue_.push_back(*wc_it);
    }
    while (!context->done_ && (context != write_handle->write_queue_.front())) {
        VLOG(20) << "===== waitlock put, ctx " << (uint64_t)(context);
        context->is_wait_ = true;
        context->cv_.Wait();
    }
    if (context->done_) {
        write_mutex_.Unlock();
        //delete context;
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
    VLOG(20) << ">>>>> lock put, ctx " << (uint64_t)(context);

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
        VLOG(20) << "put record: offset " << data_location.offset_ << ", size " << data_location.size_
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
        if (ready != context) {
            ready->done_ = true;
            if (ready->is_wait_) {
                // wait up suspend thread, so that it can do something cleanup
                ready->cv_.Signal();
            } else {
                // this write context has no context, help it free memory
                delete ready;
            }
        }
        // cannot access ready any more
        if (ready == last_writer) {
            break;
        }
    }
    if (!write_handle->write_queue_.empty()) {
        write_handle->write_queue_.front()->cv_.Signal();
    }

    write_mutex_.Unlock();
    VLOG(20) << "<<<<< finish put, ctx " << (uint64_t)(context);
    //delete context;
    return 0;
}

struct SyncWriteCallbackParam {
    CondVar* cond_;
};

void SyncWriteCallback(Table* table, StoreRequest* request,
                       StoreResponse* response,
                       void* callback_param) {
    SyncWriteCallbackParam* param = (SyncWriteCallbackParam*)callback_param;
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
    SyncWriteCallbackParam param;
    param.cond_ = &cond;
    if (callback == NULL && !FLAGS_use_tera_async_write) {
        callback = SyncWriteCallback;
        callback_param = &param;
    }

    // construct WriteContext
    WriteContext* context = new WriteContext(&write_mutex_);
    context->req_ = req;
    context->resp_ = resp;
    context->callback_ = callback;
    context->callback_param_ = callback_param;
    context->sync_ = true;
    context->is_wait_ = false;
    context->done_ = false;

    // internal batch help enhance single thread
    std::vector<WriteContext*> local_queue;
    if (SubmitRequest(context, &local_queue)) {
        return 0;
    }

    InternalBatchWrite(context, local_queue);
    // write finish, if sync write, just wait callback
    if (callback == SyncWriteCallback) {
        param.cond_->Wait();
    }
    delete context;
    return 0;

#if 0
    // select write handle
    WriteHandle* write_handle = GetWriteHandle();

    // wait and lock
    LOG(INFO) << ">>>>> begin put, ctx " << (uint64_t)(context);
    write_mutex_.Lock();
    write_handle->write_queue_.push_back(context);
    std::vector<WriteContext*> wc_it = local_queue.begin();
    for (; wc_it != local_queue.end(); wc_it) {
        write_handle->write_queue_.push_back(*wc_it);
    }
    while (!context->done_ && (context != write_handle->write_queue_.front())) {
        LOG(INFO) << "===== waitlock put, ctx " << (uint64_t)(context);
        context->is_wait_ = true;
        context->cv_.Wait();
    }
    if (context->done_) {
        write_mutex_.Unlock();
        // write finish, if sync write, just wait callback
        if (callback == DefaultUserCallback) {
            param.cond_->Wait();
        }
        delete context;
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
    LOG(INFO) << ">>>>> lock put, ctx " << (uint64_t)(context);

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
        if (ready != context) {
            ready->done_ = true;
            if (ready->is_wait_) {
                // wait up suspend thread, so that it can do something cleanup
                ready->cv_.Signal();
            } else {
                // this write context has no context, help it free memory
                delete ready;
            }
        }
        // cannot access ready any more
        if (ready == last_writer) {
            break;
        }
    }
    if (!write_handle->write_queue_.empty()) {
        write_handle->write_queue_.front()->cv_.Signal();
    }

    write_mutex_.Unlock();
    LOG(INFO) << "<<<<< finish put, ctx " << (uint64_t)(context);
    // write finish, if sync write, just wait callback
    if (callback == DefaultUserCallback) {
        param.cond_->Wait();
    }
    delete context;
    return 0;
#endif
}

void PutCallback(tera::RowMutation* row) {
    PutContext* context = (PutContext*)row->GetContext();
    // the last one invoke user callback
    if (context->counter_.Dec() == 0) {
        if (context->callback_) {
            context->callback_(context->table_, (StoreRequest*)context->req_, context->resp_,
                context->callback_param_);
        }
        VLOG(12) << "put callback";
        delete context;
    }
    delete row;
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

    // write index tables
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

    // write timestamp table
    tera::Table* ts_table = GetTimestampTable();
    char buf[8];
    EncodeBigEndian(buf, req->timestamp);
    std::string ts_key(buf, sizeof(buf));
    VLOG(12) << " write ts table: " << std::hex << ts_table << ", timestamp: "<< req->timestamp
             << ", ts string: " << DebugString(ts_key);
    tera::RowMutation* ts_row = ts_table->NewRowMutation(ts_key);
    ts_row->Put(kIndexTableColumnFamily, primary_key, req->timestamp, null_value);
    ts_row->SetContext(context);
    ts_row->SetCallBack(PutCallback);
    ts_table->ApplyMutation(ts_row);

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

    for (size_t i = 0; i < nr_index_table; i++) {
        bool skip_scan = false;
        const IndexConditionExtend& index_cond_ex = index_condition_ex_list[i];
        const std::string& index_name = index_cond_ex.index_name;
        tera::Table* index_table = GetIndexTable(index_name);
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

        VLOG(10) << "select op, scan index table: " << index_name;
        scan_desc->AddColumnFamily(kIndexTableColumnFamily);
        scan_desc->SetTimeRange(req->end_timestamp, req->start_timestamp);
        tera::ErrorCode err;
        tera::ResultStream* result = index_table->Scan(*scan_desc, &err);

        pri_vec[i].clear();
        while (!result->Done()) {
            // TODO: sync scan is enough
            const std::string& primary_key = result->Qualifier();
            VLOG(12) << "select op, primary key: " << primary_key;
            pri_vec[i].push_back(primary_key);

            result->Next();
        }
        delete result;
        delete scan_desc;
    }
    if (nr_index_table <= 0) {
        std::vector<tera::Table*> ts_table_list;
        GetAllTimestampTables(&ts_table_list);
        pri_vec.resize(1);

        char buf[8];
        EncodeBigEndian(buf, req->start_timestamp);
        std::string start_ts_key(buf, sizeof(buf));
        EncodeBigEndian(buf, req->end_timestamp);
        std::string end_ts_key(buf, sizeof(buf));
        VLOG(10) << "scan timestamp table: " << DebugString(start_ts_key) << " ~ "
                 << DebugString(end_ts_key);
        for (size_t i = 0; i < ts_table_list.size(); i++) {
            tera::Table* ts_table = ts_table_list[i];
            tera::ScanDescriptor* scan_desc = new tera::ScanDescriptor(start_ts_key);
            scan_desc->SetEnd(end_ts_key + '\0');
            scan_desc->AddColumnFamily(kIndexTableColumnFamily);
            tera::ErrorCode err;
            tera::ResultStream* result = ts_table->Scan(*scan_desc, &err);

            while (!result->Done()) {
                // TODO: sync scan is enough
                const std::string& primary_key = result->Qualifier();
                VLOG(12) << "select op, primary key: " << primary_key;
                pri_vec[0].push_back(primary_key);

                result->Next();
            }
            delete result;
            delete scan_desc;
        }
    }

    // merge sort
    PrimaryKeyMergeSort(pri_vec, primary_key_list, req->limit);
    return Status::OK();
}

///////   fast merge sort algorithm   ////////
Status TableImpl::PrimaryKeyMergeSort(std::vector<std::vector<std::string> >& pri_vec,
                                      std::vector<std::string>* primary_key_list,
                                      int32_t limit) {
    uint32_t nr_stream = pri_vec.size();
    if (nr_stream <= 0) {
        return Status::OK();
    }

    // collect iterator, min_key
    std::string min_key;
    std::vector<std::vector<std::string>::iterator > iter_vec(nr_stream);
    for (uint32_t i = 0; i < nr_stream; i++) {
        if (pri_vec[i].size() == 0) {
            LOG(INFO) << "no result in stream " << i;
            return Status::OK();
        }

        // sort primary_key stream, erase unique primary key
        std::string unique_key;
        std::sort(pri_vec[i].begin(), pri_vec[i].end());
        std::vector<std::string>::iterator unique_it = pri_vec[i].begin();
        unique_key = *unique_it;
        ++unique_it;
        while (unique_it != pri_vec[i].end()) {
            if ((*unique_it).compare(unique_key) == 0) {
                unique_it = pri_vec[i].erase(unique_it);
            } else {
                unique_key = *unique_it;
                ++unique_it;
            }
        }

        // get min_key in primary_key stream
        std::vector<std::string>::iterator it = pri_vec[i].begin();
        iter_vec[i] = it;
        if ((min_key.size() == 0) ||
            (min_key.compare((pri_vec[i])[0]) > 0)) {
            min_key = (pri_vec[i])[0];
        }
    }

    while (1) {
        std::string max_key = min_key;
        // collect min && max key in all stream
        for (uint32_t i = 0; i < nr_stream; i++) {
            if (min_key.compare(*(iter_vec[i])) == 0) {
                ++(iter_vec[i]);
            } else {
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
            LOG(INFO) << "merge pri key " << max_key;
            primary_key_list->push_back(max_key);
            // get next min_key
            if ((iter_vec[0] == pri_vec[0].end()) ||
                ((int32_t)primary_key_list->size() >= limit)) {
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

tera::Table* TableImpl::GetTimestampTable() {
    MutexLock mu(&write_mutex_);
    cur_timestamp_table_id_ = (cur_timestamp_table_id_ + 1) % nr_timestamp_table_;

    char ts_name[32];
    sprintf(ts_name, "timestamp#%d", cur_timestamp_table_id_);
    std::string index_table_name = tera_.table_prefix_ + "#" + table_desc_.table_name + "#" + ts_name;
    VLOG(12) << "get index table: " << index_table_name;
    tera::Table* table = tera_.tera_table_map_[index_table_name];
    return table;
}

void TableImpl::GetAllTimestampTables(std::vector<tera::Table*>* table_list) {
    for (int i = 0; i < nr_timestamp_table_; i++) {
        char ts_name[32];
        sprintf(ts_name, "timestamp#%d", i);
        std::string index_table_name = tera_.table_prefix_ + "#" + table_desc_.table_name + "#" + ts_name;
        tera::Table* table = tera_.tera_table_map_[index_table_name];
        table_list->push_back(table);
    }
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
