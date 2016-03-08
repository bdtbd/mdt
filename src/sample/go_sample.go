package main

import (
        "fmt"
        "time"
        "mdt"
        )

func main() {
    fmt.Println("hello world!")
    db := mdt.OpenDB("mdt-test006", "./conf/mdt.flag")
    if db == nil {
        fmt.Println("open db failed")
        return
    }
    defer mdt.CloseDB(db)

    table := mdt.OpenTable(db, "table-kepler001")
    if table == nil {
        fmt.Println("open table failed")
        return
    }
    defer mdt.CloseTable(table)

    index_req := mdt.Index{"Query", "nihao"}
    index_req2 := mdt.Index{"Service", "serv"}

    Primary_key := "abcdefaaaa"
    now := time.Now();
    Timestamp := int64(now.UnixNano() / 1000)
    var Index_list []mdt.Index
    Index_list = append(Index_list, index_req)
    Index_list = append(Index_list, index_req2)
    Data := "hello world"

    error := mdt.Store(table, Primary_key, Timestamp, Index_list, Data)
    fmt.Println("store result: %d", error)

    var Index_condition_list []mdt.IndexCondition
    index_cond := mdt.IndexCondition{"Query", mdt.EqualTo, "nihao"}
    Index_condition_list = append(Index_condition_list, index_cond)
    Start_timestamp := int64(0)
    End_timestamp := int64(now.UnixNano() / 1000)
    Limit := int32(1)

    error, result_list := mdt.SearchIndexByIndexKey(table, Index_condition_list, Start_timestamp, End_timestamp, Limit)
    fmt.Println("get index %d: %v", error, result_list)

    error, result_list = mdt.SearchByIndexKey(table, Index_condition_list, Start_timestamp, End_timestamp, Limit)
    fmt.Println("get data %d: %v", error, result_list)

    error, index_list := mdt.SearchIndexByPrimaryKey(table, Primary_key)
    fmt.Println("get index %d: %v", error, index_list)

    error, data_list := mdt.SearchByPrimaryKey(table, Primary_key)
    fmt.Println("get data %d: %v", error, data_list)
}

