// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_C_H_
#define  MDT_SDK_C_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct mdt_db_t mdt_db_t;
typedef struct mdt_table_t mdt_table_t;

// 打开数据库
extern mdt_db_t* mdt_open_db(const char* db_name, const char* conf_path);

// 关闭数据库
extern void mdt_close_db(mdt_db_t* db);

// 打开表格
extern mdt_table_t* mdt_open_table(mdt_db_t* db, const char* table_name);

// 关闭表格
extern void mdt_close_table(mdt_table_t* table);

typedef struct mdt_slice_t {
    const char* data;
    size_t size;
} mdt_slice_t;

//////////////////////////////////////////////
// 写入
//////////////////////////////////////////////

// 索引数据
typedef struct mdt_index_t {
    mdt_slice_t index_name; // index table name
    mdt_slice_t index_key; // key after encode
} mdt_index_t;

// 写入请求
typedef struct mdt_store_request_t {
    mdt_slice_t primary_key; // key after encode
    int64_t timestamp;
    mdt_index_t* index_list;
    size_t index_list_len;
    mdt_slice_t data;
    // mdt_slice_t* data_list;
    // size_t data_list_len;
} mdt_store_request_t;

// 写入结果
typedef struct mdt_store_response_t {
    int error;
} mdt_store_response_t;

// 异步写入回调
typedef void (*mdt_store_callback)(mdt_table_t* table,
                                   const mdt_store_request_t* request,
                                   mdt_store_response_t* response,
                                   void* callback_param);

// 写入接口。callback != NULL时，是异步调用。
extern void mdt_store(mdt_table_t* table,
                      const mdt_store_request_t* request,
                      mdt_store_response_t* response,
                      mdt_store_callback callback,
                      void* callback_param);

//////////////////////////////////////////////
// 查询
//////////////////////////////////////////////

// 比较器
enum MDT_CMP {
    mdt_cmp_equal_to = 0,       // ==
    mdt_cmp_not_equal_to = 1,    // !=
    mdt_cmp_less = 2,          // <
    mdt_cmp_less_equal = 3,     // <=
    mdt_cmp_greater = 4,       // >=
    mdt_cmp_greater_equal = 5,  // >
};

// 检索条件
typedef struct mdt_index_condition_t {
    mdt_slice_t index_name;
    enum MDT_CMP comparator;
    mdt_slice_t compare_value; // value after enconde
} mdt_index_condition_t;

// 查询请求
typedef struct mdt_search_request_t {
    mdt_slice_t primary_key;
    mdt_index_condition_t* index_condition_list;
    size_t index_condition_list_len;
    int64_t start_timestamp;
    int64_t end_timestamp;
    int32_t limit;
} mdt_search_request_t;

// 查询结果
typedef struct mdt_search_result_t {
    mdt_slice_t primary_key;
    mdt_slice_t* data_list;
    size_t data_list_len;
} mdt_search_result_t;

typedef struct mdt_search_response_t {
    mdt_search_result_t* result_list;
    size_t result_list_len;
} mdt_search_response_t;

// 异步查询回调
typedef void (*mdt_search_callback)(mdt_table_t* table,
                                    const mdt_search_request_t* request,
                                    const mdt_search_response_t* response,
                                    void* callback_param);

// 查询接口。callback != NULL时，是异步调用。
extern void mdt_search(mdt_table_t* table,
                       const mdt_search_request_t* request,
                       mdt_search_response_t* response,
                       mdt_search_callback callback,
                       void* callback_param);

//////////////////////////////////////////////
// 建表
//////////////////////////////////////////////

// 数据类型
enum TYPE {
    mdt_bytes = 0,
    mdt_bool = 1,
    mdt_int8 = 2,
    mdt_uint8 = 3,
    mdt_int16 = 4,
    mdt_uint16 = 5,
    mdt_int32 = 6,
    mdt_uint32 = 7,
    mdt_int64 = 8,
    mdt_uint64 = 9,
    mdt_float = 10,
    mdt_double = 11
};

// 索引描述
typedef struct mdt_index_description_t {
    mdt_slice_t index_name;
    enum TYPE index_key_type;
} mdt_index_description_t;

// 数据类描述
typedef struct mdt_table_description_t {
    mdt_slice_t table_name;
    enum TYPE primary_key_type;
    mdt_index_description_t* index_description_list;
    size_t index_description_list_len;
} mdt_table_description_t;

// 建表
extern int mdt_create_table(mdt_db_t* db, mdt_table_description_t table_desc);

#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif  //MDT_SDK_SDK_H_
