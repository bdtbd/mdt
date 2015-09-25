package main

import (
        "fmt"
        "mdt"
        )

func main() {
    fmt.Println("hello world!")
    db := mdt.OpenDB("mdt-test100", "./conf/mdt.flag")
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

    Primary_key := "abcdefaaaa"
    Timestamp := int64(11111)
    var Index_list []mdt.Index
    Index_list = append(Index_list, index_req)
    Data := "hello world"

    error := mdt.Store(table, Primary_key, Timestamp, Index_list, Data)
    fmt.Println("store result: %d", error)

    var Index_condition_list []mdt.IndexCondition
    index_cond := mdt.IndexCondition{"Query", mdt.EqualTo, "nihao"}
    Index_condition_list = append(Index_condition_list, index_cond)
    Start_timestamp := int64(0)
    End_timestamp := int64(222222)
    Limit := int32(1)
    error, result_list := mdt.SearchByIndexKey(table, Index_condition_list, Start_timestamp, End_timestamp, Limit)

    fmt.Println("get result: %v", result_list)

    error, data_list := mdt.SearchByPrimaryKey(table, Primary_key)

    fmt.Println("get result: %v", data_list)
}

