// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package mdt

// #cgo LDFLAGS: -L. -L../mdt/lib -lmdt -ltera -lins_sdk -lsofa-pbrpc -lprotobuf -lsnappy -lzookeeper_mt -lgtest_main -lgtest -lglog -lgflags -ltcmalloc_minimal -lstdc++ -lc -lm -ldl -lpthread -lrt -lz
// #include "mdt_c.h"
import "C"
import "unsafe"

// 错误码，int
const (
    Ok = 0
    NotFound = 1
    Corruption = 2
    NotSupported = 3
    InvalidArgument = 4
    IOError = 5
)
  
type DB struct {
    rep *C.struct_mdt_db_t
}

type Table struct {
    rep *C.struct_mdt_table_t
}

// 打开数据库
func OpenDB(db_name string, conf_path string) *DB {
    c_db_name := C.CString(db_name)
    c_conf_path := C.CString(conf_path)
    c_db := C.mdt_open_db(c_db_name, c_conf_path)
    C.free(unsafe.Pointer(c_db_name))
    C.free(unsafe.Pointer(c_conf_path))
    if c_db == nil {
        return nil
    }
    return &DB{c_db}
}

// 关闭数据库
func CloseDB(db *DB) {
    C.mdt_close_db(db.rep);
}

// 打开表格
func OpenTable(db *DB, table_name string) *Table {
    c_table_name := C.CString(table_name)
    c_table := C.mdt_open_table(db.rep, c_table_name)
    C.free(unsafe.Pointer(c_table_name))
    if c_table == nil {
        return nil
    }
    return &Table{c_table}
}

// 关闭表格
func CloseTable(table *Table) {
    C.mdt_close_table(table.rep)
}

// 索引数据
type Index struct {
    IndexName string // index table name
    IndexKey string // key after encode
}

// 写入接口
func Store(table *Table,
           primary_key string,  // key after encode
           timestamp int64,
           index_list []Index,
           data string) int {
    var c_request C.mdt_store_request_t
    var c_response C.mdt_store_response_t
    
    // convert request
    c_request.primary_key.data = C.CString(primary_key)
    c_request.primary_key.size = C.size_t(len(primary_key))
    c_request.timestamp = C.int64_t(timestamp)
    c_request.index_list = nil
    c_request.index_list_len = C.size_t(len(index_list))
    c_request.index_list = (*C.mdt_index_t)(C.malloc(32 * c_request.index_list_len))
    c_index_list := (*[1 << 30]C.mdt_index_t)(unsafe.Pointer(c_request.index_list))
    for i := C.size_t(0); i < c_request.index_list_len; i++ {
        c_index_list[i].index_name.data = C.CString(index_list[i].IndexName)
        c_index_list[i].index_name.size = C.size_t(len(index_list[i].IndexName))
        c_index_list[i].index_key.data = C.CString(index_list[i].IndexKey)
        c_index_list[i].index_key.size = C.size_t(len(index_list[i].IndexKey))
    }
    c_request.data.data = C.CString(data)
    c_request.data.size = C.size_t(len(data))
    
    // invoke C API
    C.mdt_store(table.rep, &c_request, &c_response, nil, nil)
    
    // convert result
    error := int(c_response.error)
    
    // free request memory
    C.free(unsafe.Pointer(c_request.primary_key.data))
    for i := C.size_t(0); i < c_request.index_list_len; i++ {
        C.free(unsafe.Pointer(c_index_list[i].index_name.data))
        C.free(unsafe.Pointer(c_index_list[i].index_key.data))
    }
    C.free(unsafe.Pointer(c_request.index_list))
	C.free(unsafe.Pointer(c_request.data.data))
	
	return error
}

// 比较器
const (
    EqualTo = 0        // ==
    NotEqualTo = 1     // !=
    Less = 2           // <
    LessEqual = 3      // <=
    Greater = 4        // >=
    GreaterEqual = 5   // >
)

// 检索条件
type IndexCondition struct {
    IndexName string
    Comparator uint32
    CompareValue string // value after enconde
}

// 查询结果
type Result struct {
    PrimaryKey string
    DataList []string
}

// 查询接口（按primary key查）
func SearchByPrimaryKey(table *Table,
                        primary_key string) (int, []string) {
    var c_request C.mdt_search_request_t
    var c_response C.mdt_search_response_t
    
    // convert request
    c_request.primary_key.data = C.CString(primary_key)
    c_request.primary_key.size = C.size_t(len(primary_key))
    
    // invoke C api
    C.mdt_search(table.rep, &c_request, &c_response, nil, nil)
    
    // convert result & free result memory
    error := Ok
    var data_list []string
    c_result_list := (*[1<<30]C.mdt_search_result_t)(unsafe.Pointer(c_response.result_list))
    if c_response.result_list_len == C.size_t(1) {
        c_result := &c_result_list[0]
        C.free(unsafe.Pointer(c_result.primary_key.data))
        
        c_data_list := (*[1<<30]C.mdt_slice_t)(unsafe.Pointer(c_result.data_list))
        for j := C.size_t(0); j < c_result.data_list_len; j++ {
            data_list = append(data_list, C.GoStringN(c_data_list[j].data, C.int(c_data_list[j].size)))
            C.free(unsafe.Pointer(c_data_list[j].data))
        }
        C.free(unsafe.Pointer(c_result.data_list))
    } else {
        error = NotFound
    }
    C.free(unsafe.Pointer(c_response.result_list))
    
    // free request memory
    C.free(unsafe.Pointer(c_request.primary_key.data))
    
    return error, data_list
}

// 查询接口（按index key查）
func SearchByIndexKey(table *Table,
                      index_condition_list []IndexCondition,
                      start_timestamp int64,
                      end_timestamp int64,
                      limit int32) (int, []Result) {
    var c_request C.mdt_search_request_t
    var c_response C.mdt_search_response_t
        
    // convert request
    c_request.primary_key.size = 0
    c_request.index_condition_list_len = C.size_t(len(index_condition_list))
    c_request.index_condition_list = (*C.mdt_index_condition_t)(C.malloc(36 * c_request.index_condition_list_len))
    c_index_condition_list := (*[1<<30]C.mdt_index_condition_t)(unsafe.Pointer(c_request.index_condition_list))
    for i := C.size_t(0); i < c_request.index_condition_list_len; i++ {
        c_index_condition := &c_index_condition_list[i]
        index_condition := &index_condition_list[i]
        c_index_condition.index_name.data = C.CString(index_condition.IndexName)
        c_index_condition.index_name.size = C.size_t(len(index_condition.IndexName))
        c_index_condition.comparator = index_condition.Comparator
        c_index_condition.compare_value.data = C.CString(index_condition.CompareValue)
        c_index_condition.compare_value.size = C.size_t(len(index_condition.CompareValue))
    }
    c_request.start_timestamp = C.int64_t(start_timestamp)
    c_request.end_timestamp = C.int64_t(end_timestamp)
    c_request.limit = C.int32_t(limit)
    
    // invoke C api
    C.mdt_search(table.rep, &c_request, &c_response, nil, nil)
    
    // convert result & free result memory
    error := Ok
    var result_list []Result
    c_result_list := (*[1<<30]C.mdt_search_result_t)(unsafe.Pointer(c_response.result_list))
    for i := C.size_t(0); i < c_response.result_list_len; i++ {
        c_result := &c_result_list[i]        
        result := &Result{}
        
        result.PrimaryKey = C.GoStringN(c_result.primary_key.data, C.int(c_result.primary_key.size))
        C.free(unsafe.Pointer(c_result.primary_key.data))
        
        c_data_list := (*[1<<30]C.mdt_slice_t)(unsafe.Pointer(c_result.data_list))
        for j := C.size_t(0); j < c_result.data_list_len; j++ {
            result.DataList = append(result.DataList, C.GoStringN(c_data_list[j].data, C.int(c_data_list[j].size)))
            C.free(unsafe.Pointer(c_data_list[j].data))
        }
        result_list = append(result_list, *result)
        C.free(unsafe.Pointer(c_result.data_list))
    }
    C.free(unsafe.Pointer(c_response.result_list))
    if result_list == nil {
        error = NotFound
    }
    
    // free request memory
    for i := C.size_t(0); i < c_request.index_condition_list_len; i++ {
        c_index_condition := &c_index_condition_list[i]
        C.free(unsafe.Pointer(c_index_condition.index_name.data))
        C.free(unsafe.Pointer(c_index_condition.compare_value.data))
    }
    C.free(unsafe.Pointer(c_request.index_condition_list))
    
    return error, result_list
}
