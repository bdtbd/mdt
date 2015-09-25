// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package mdt

type db_t struct db_t
type table_t struct table_t

// 打开数据库
func OpenDB(db_name string, conf_path string) *db_t

// 关闭数据库
func CloseDB(db *db_t)

// 打开表格
func OpenTable(db *db_t, table_name string) *table_t

// 关闭表格
func CloseTable(table *table_t)

//////////////////////////////////////////////
// 写入
//////////////////////////////////////////////

// 索引数据
type index_t struct {
    index_name string // index table name
    index_key string // key after encode
}

// 写入请求
type store_request_t struct {
    primary_key string // key after encode
    timestamp int64
    index_list []index_t
    data string
}

// 写入结果
type store_response_t struct {
    error int32
}

// 写入接口
func Store(table *table_t,
           request *store_request_t,
           response *store_response_t)

//////////////////////////////////////////////
// 查询
//////////////////////////////////////////////

// 比较器
const (
    cmp_equal_to = 0       // ==
    cmp_not_equal_to = 1   // !=
    cmp_less = 2           // <
    cmp_less_equal = 3     // <=
    cmp_greater = 4        // >=
    cmp_greater_equal = 5  // >
)

// 检索条件
type index_condition_t struct {
    index_name string
    comparator rune
    compare_value string // value after enconde
}

// 查询请求
type search_request_t struct {
    index_condition_list []index_condition_t
    start_timestamp int64
    end_timestamp int64
    limit int32
}

// 查询结果
type search_result_t struct {
    primary_key string
    data_list []string
}

type search_response_t struct {
    result_list []search_result_t
}

// 查询接口
func Search(table *table_t,
            request *search_request_t,
            response *search_response_t)

//////////////////////////////////////////////
// 建表
//////////////////////////////////////////////

// 数据类型
const (
    type_bytes = 0
    type_bool = 1
    type_int8 = 2
    type_uint8 = 3
    type_int16 = 4
    type_uint16 = 5
    type_int32 = 6
    type_uint32 = 7
    type_int64 = 8
    type_uint64 = 9
    type_float = 10
    type_double = 11
}

// 索引描述
type index_description_t struct {
    index_name string
    index_key_type rune
}

// 数据类描述
type table_description_t struct {
    table_name string
    primary_key_type rune
    index_description_list []index_description_t
}

// 建表
func Create_table(db *db_t, table_desc table_description_t) int32
