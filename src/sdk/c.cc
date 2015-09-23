// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <sdk/c.h>

#include <assert.h>
#include <string>
#include <string.h>
#include <gflags/gflags.h>
#include "sdk/sdk.h"

DECLARE_string(flagfile);

extern "C" {

struct mdt_db_t {
    mdt::Database* rep;
};

struct mdt_table_t {
    mdt::Table* rep;
};

void mdt_slice_to_string(const mdt_slice_t& slice, std::string* str) {
    str->assign(slice.data, slice.size);
}

void mdt_string_to_slice(const std::string& str, mdt_slice_t* slice) {
    slice->size = str.size();
    if (slice->size == 0) {
        return;
    }
    char* data = new char[slice->size];
    memcpy(data, str.data(), slice->size);
    slice->data = data;
}

// 打开数据库
mdt_db_t* mdt_open_db(const char* db_name, const char* conf_path) {
    int argc = 1;
    char** argv = new char*[2];
    argv[0] = (char*)"dummy";
    argv[1] = NULL;
    FLAGS_flagfile = conf_path;
    google::ParseCommandLineFlags(&argc, &argv, true);

    mdt::Database* internal_db = mdt::OpenDatabase(db_name);
    mdt_db_t* db = new mdt_db_t;
    db->rep = internal_db;
    return db;
}

// 关闭数据库
void mdt_close_db(mdt_db_t* db) {
    mdt::CloseDatabase(db->rep);
    delete db;
}

// 打开表格
mdt_table_t* mdt_open_table(mdt_db_t* db, const char* table_name) {
    mdt::Table* internal_table = mdt::OpenTable(db->rep, table_name);
    mdt_table_t* table = new mdt_table_t;
    table->rep = internal_table;
    return table;
}

// 关闭表格
void mdt_close_table(mdt_table_t* table) {
    mdt::CloseTable(table->rep);
    delete table;
}

struct mdt_c_store_callback_param {
    mdt_table_t* table;
    const mdt_store_request_t* request;
    mdt_store_response_t* response;
    mdt_store_callback callback;
    void* callback_param;
};

// 写入回调
void mdt_c_store_callback(mdt::Table* internal_table,
                          mdt::StoreRequest* internal_request,
                          mdt::StoreResponse* internal_response,
                          void* callback_param) {
    mdt_c_store_callback_param* param = (mdt_c_store_callback_param*)callback_param;
    param->response->error = internal_response->error;
    (*param->callback)(param->table, param->request, param->response, param->callback_param);
    delete internal_request;
    delete internal_response;
    delete param;
}

// 写入接口
void mdt_store(mdt_table_t* table,
               const mdt_store_request_t* request,
               mdt_store_response_t* response,
               mdt_store_callback callback,
               void* callback_param) {
    // build internal request
    mdt::StoreRequest* internal_request = new mdt::StoreRequest;
    internal_request->primary_key.assign(request->primary_key.data, request->primary_key.size);
    internal_request->timestamp = request->timestamp;
    internal_request->index_list.resize(request->index_list_len);
    for (size_t i = 0; i < request->index_list_len; i++) {
        mdt_index_t& index = request->index_list[i];
        mdt::Index& internal_index = internal_request->index_list[i];
        mdt_slice_to_string(index.index_name, &internal_index.index_name);
        mdt_slice_to_string(index.index_key, &internal_index.index_key);
    }
    mdt_slice_to_string(request->data, &internal_request->data);

    // build internal response
    mdt::StoreResponse* internal_response = new mdt::StoreResponse;

    // build callback context
    mdt::StoreCallback internal_callback = NULL;
    mdt_c_store_callback_param* param = NULL;
    if (callback != NULL) {
        internal_callback = &mdt_c_store_callback;
        param = new mdt_c_store_callback_param;
        param->table = table;
        param->request = request;
        param->response = response;
        param->callback = callback;
        param->callback_param = callback_param;
    }

    // call internal store func
    mdt::Put(table->rep, internal_request, internal_response, internal_callback, param);

    if (callback == NULL) {
        delete internal_request;
        delete internal_response;
    }
}

struct mdt_c_search_callback_param {
    mdt_table_t* table;
    const mdt_search_request_t* request;
    mdt_search_response_t* response;
    mdt_search_callback callback;
    void* callback_param;
};

// 查询回调
void mdt_c_search_callback(mdt::Table* internal_table,
                           mdt::SearchRequest* internal_request,
                           mdt::SearchResponse* internal_response,
                           void* callback_param) {
    mdt_c_search_callback_param* param = (mdt_c_search_callback_param*)callback_param;
    mdt_search_response_t* response = param->response;

    // build response from internal response
    response->result_list_len = internal_response->result_stream.size();
    if (response->result_list_len > 0) {
        response->result_list = new mdt_search_result_t[response->result_list_len];
        for (size_t i = 0; i < response->result_list_len; i++) {
            mdt::ResultStream& internal_result = internal_response->result_stream[i];
            mdt_search_result_t& result = response->result_list[i];

            // copy primary_key
            mdt_string_to_slice(internal_result.primary_key, &result.primary_key);

            // copy data list
            result.data_list_len = internal_result.result_data_list.size();
            assert(result.data_list_len > 0);
            result.data_list = new mdt_slice_t[result.data_list_len];
            for (size_t i = 0; i < result.data_list_len; i++) {
                mdt_string_to_slice(internal_result.result_data_list[i], &result.data_list[i]);
            }
        }
    }

    (*param->callback)(param->table, param->request, param->response, param->callback_param);
    delete internal_request;
    delete internal_response;
    delete param;
}

// 查询接口
void mdt_search(mdt_table_t* table,
                const mdt_search_request_t* request,
                mdt_search_response_t* response,
                mdt_search_callback callback,
                void* callback_param) {
    // build internal request
    mdt::SearchRequest* internal_request = new mdt::SearchRequest;
    internal_request->index_condition_list.resize(request->index_condition_list_len);
    for (size_t i = 0; i < request->index_condition_list_len; i++) {
        mdt::IndexCondition& internal_index_cond = internal_request->index_condition_list[i];
        mdt_index_condition_t& index_cond = request->index_condition_list[i];
        mdt_slice_to_string(index_cond.index_name, &internal_index_cond.index_name);
        internal_index_cond.comparator = (mdt::COMPARATOR)index_cond.comparator;
        mdt_slice_to_string(index_cond.compare_value, &internal_index_cond.compare_value);
    }
    internal_request->start_timestamp = request->start_timestamp;
    internal_request->end_timestamp = request->end_timestamp;
    internal_request->limit = request->limit;

    // build internal response
    mdt::SearchResponse* internal_response = new mdt::SearchResponse;

    // build callback context
    mdt::SearchCallback internal_callback = NULL;
    mdt_c_search_callback_param* param = NULL;
    if (callback != NULL) {
        internal_callback = &mdt_c_search_callback;
        param = new mdt_c_search_callback_param;
        param->table = table;
        param->request = request;
        param->response = response;
        param->callback = callback;
        param->callback_param = callback_param;
    }

    // call internal store func
    mdt::Get(table->rep, internal_request, internal_response, internal_callback, param);

    if (callback == NULL) {
        // build response from internal response
        response->result_list_len = internal_response->result_stream.size();
        if (response->result_list_len > 0) {
            response->result_list = new mdt_search_result_t[response->result_list_len];
            for (size_t i = 0; i < response->result_list_len; i++) {
                mdt::ResultStream& internal_result = internal_response->result_stream[i];
                mdt_search_result_t& result = response->result_list[i];

                // copy primary_key
                mdt_string_to_slice(internal_result.primary_key, &result.primary_key);

                // copy data list
                result.data_list_len = internal_result.result_data_list.size();
                assert(result.data_list_len > 0);
                result.data_list = new mdt_slice_t[result.data_list_len];
                for (size_t i = 0; i < result.data_list_len; i++) {
                    mdt_string_to_slice(internal_result.result_data_list[i], &result.data_list[i]);
                }
            }
        }
        delete internal_request;
        delete internal_response;
    }
}

// 建表
int mdt_create_table(mdt_db_t* db, mdt_table_description_t table_desc) {
    // build internal table description
    mdt::TableDescription internal_table_desc;
    mdt_slice_to_string(table_desc.table_name, &internal_table_desc.table_name);
    internal_table_desc.primary_key_type = (mdt::TYPE)table_desc.primary_key_type;
    internal_table_desc.index_descriptor_list.resize(table_desc.index_description_list_len);
    for (size_t i = 0; i < table_desc.index_description_list_len; i++) {
        mdt::IndexDescription& internal_index_desc = internal_table_desc.index_descriptor_list[i];
        mdt_index_description_t& index_desc = table_desc.index_description_list[i];

        // build internal index description
        mdt_slice_to_string(index_desc.index_name, &internal_index_desc.index_name);
        internal_index_desc.index_key_type = (mdt::TYPE)index_desc.index_key_type;
    }

    return mdt::CreateTable(db->rep, internal_table_desc);
}

} // extern "C"
