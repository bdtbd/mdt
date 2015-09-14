// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_SDK_H_
#define  MDT_SDK_SDK_H_

#include <stdint.h>
#include <string>
#include <vector>

namespace mdt {

//////////////////////////////////////////////
// 写入
//////////////////////////////////////////////

// 索引数据
struct Index {
    std::string index_name;
    std::string index_key;// key after encode
};

// 写入请求
struct StoreRequest {
    std::string db_name;
    std::string table_name;
    std::string primary_key;
    uint64_t timestamp;
    std::vector<struct Index> index_list;
    std::string data;
};

// 写入结果
struct StoreResponse {
    int error;
    void* user_ptr;
};

// 异步写入回调
typedef void (*StoreCallback)(const StoreRequest* request, const StoreResponse* response);

// 同步写入接口
void Put(const StoreRequest& request, StoreResponse* response);

// 异步写入接口
void Put(const StoreRequest* request, StoreResponse* response, StoreCallback callback);

//////////////////////////////////////////////
// 查询
//////////////////////////////////////////////

// 比较器
enum COMPARATOR {
    kEqualTo = 0,       // ==
    kNotEqualTo = 1,    // !=
    kLess = 2,          // <
    kLessEqual = 3,     // <=
    kGreater = 4,       // >=
    kGreaterEqual = 5,  // >
};

// 检索条件
struct IndexCondition {
    std::string index_name;
    enum COMPARATOR comparator;
    std::string compare_value; // value after enconde
};

// 查询请求
struct SearchRequest {
    std::string db_name;
    std::string table_name;
    std::vector<struct IndexCondition> index_condition_list;
    struct IndexCondition start_tm;
    struct IndexCondition end_tm;
    int32_t limit;
};

// 查询结果
struct SearchResponse {
    std::vector<std::string> result_data_list;
    void* user_ptr;
};

// 异步查询回调
typedef void (*SearchCallback)(const SearchRequest* request, const SearchResponse* response);

// 同步查询接口
void Get(const SearchRequest& request, SearchResponse* response);

// 异步查询接口
void Get(const SearchRequest* request, SearchResponse* response, SearchCallback callback);

//////////////////////////////////////////////
// 建表
//////////////////////////////////////////////

// 数据类型
enum TYPE {
    kBytes = 0,
    kBool = 1,
    kInt8 = 2,
    kUInt8 = 3,
    kInt16 = 4,
    kUInt16 = 5,
    kInt32 = 6,
    kUInt32 = 7,
    kInt64 = 8,
    kUint64 = 9,
    kFloat = 10,
    kDouble = 11
};

// 索引描述
struct IndexDescription {
    std::string index_name;
    enum TYPE index_key_type;
};
// 数据类描述
struct TableDescription {
    std::string table_name;
    enum TYPE primary_key_type;
    std::vector<struct IndexDescription> index_descriptor_list;
};

// 建表请求
struct CreateRequest {
    std::string db_name;
    std::vector<struct TableDescription> table_descriptor_list;
};

// 建表结果
struct CreateResponse {
    int error;
};

void Create(const CreateRequest& request, CreateResponse* response);

//////////////////////////////////
//      c++ interface           //
//////////////////////////////////
class Database {
public:
    static int CreateDB(std::string db_name);
    virtual int OpenTable(const CreateRequest& request, CreateResponse* response, table** table_ptr) = 0;
private:
    Database(const Database&);
    void operator=(const Database&);
};

class Table {
public:
    virtual int Put(const StoreRequest* request, StoreResponse* response, StoreCallback callback) = 0;
    virtual int Put(const StoreRequest* request, StoreResponse* response) = 0;
private:
    Table(const Table&);
    void operator=(const Table&);
};

} // namespace mdt

#endif  //MDT_SDK_SDK_H_
