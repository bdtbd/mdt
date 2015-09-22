#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sdk/c.h"

void store_callback_test(mdt_table_t* table, const mdt_store_request_t* request,
                         mdt_store_response_t* response, void* callback_param) {
    printf("<<< callabck test >>>");
    int* store_finish = (int*)callback_param;
    *store_finish = 1;
}

int main(int ac, char* av[]) {
    if (ac < 2) {
        printf("Usage: %s conf_file\n", av[0]);
        return 0;
    }
    const char* conf_path = av[1];

    // create db
    printf("open db ...\n");
    const char* db_name = "mdt-test005";
    mdt_db_t* db = mdt_open_db(db_name, conf_path);

    // create table
    printf("create table ...\n");
    mdt_table_description_t table_desc;
    table_desc.table_name.data = "table-kepler001";
    table_desc.table_name.size = strlen(table_desc.table_name.data);
    table_desc.primary_key_type = mdt_bytes;
    table_desc.index_description_list_len = 3;
    table_desc.index_description_list = malloc(sizeof(mdt_index_description_t));
    mdt_index_description_t* index_table1 = &table_desc.index_description_list[0];
    mdt_index_description_t* index_table2 = &table_desc.index_description_list[1];
    mdt_index_description_t* index_table3 = &table_desc.index_description_list[2];
    index_table1->index_name.data = "Query";
    index_table1->index_name.size = strlen(index_table1->index_name.data);
    index_table1->index_key_type = mdt_bytes;
    index_table2->index_name.data = "Costtime";
    index_table2->index_name.size = strlen(index_table2->index_name.data);
    index_table2->index_key_type = mdt_bytes;
    index_table3->index_name.data = "Service";
    index_table3->index_name.size = strlen(index_table3->index_name.data);
    index_table3->index_key_type = mdt_bytes;

    mdt_create_table(db, table_desc);

    printf( "open table ...\n");
    mdt_table_t* table = mdt_open_table(db, table_desc.table_name.data);

    // insert data
    mdt_store_request_t* store_req = malloc(sizeof(mdt_store_request_t));
    store_req->primary_key.data = "20150920";
    store_req->primary_key.size = strlen(store_req->primary_key.data);
    store_req->timestamp = 638239414;
    store_req->index_list_len = 3;
    store_req->index_list = malloc(sizeof(mdt_index_t) * 3);
    mdt_index_t* query_index = &store_req->index_list[0];
    mdt_index_t* costtime_index = &store_req->index_list[1];
    mdt_index_t* service_index = &store_req->index_list[2];

    query_index->index_name.data = "Query";
    query_index->index_name.size = strlen(query_index->index_name.data);
    query_index->index_key.data = "beauty girl";
    query_index->index_key.size = strlen(query_index->index_key.data);

    costtime_index->index_name.data = "Costtime";
    costtime_index->index_name.size = strlen(costtime_index->index_name.data);
    costtime_index->index_key.data = "5ms";
    costtime_index->index_key.size = strlen(costtime_index->index_key.data);

    service_index->index_name.data = "Service";
    service_index->index_name.size = strlen(service_index->index_name.data);
    service_index->index_key.data = "bs module";
    service_index->index_key.size = strlen(service_index->index_key.data);

    store_req->data.data = "this s a test, Query: beauty girl, Costtime: 5ms, Service: bs module";
    store_req->data.size = strlen(store_req->data.data);

    mdt_store_response_t* store_resp = malloc(sizeof(mdt_store_response_t));
    mdt_store_callback callback = &store_callback_test;
    int store_finish = 0;

    printf("put ... %p\n", callback);
    mdt_store(table, store_req, store_resp, callback, &store_finish);

    while (store_finish != 1) {
        // usleep(1000);
    }

    mdt_search_request_t* search_req = malloc(sizeof(mdt_search_request_t));
    search_req->start_timestamp = 638239413;
    search_req->end_timestamp = 638239415;
    search_req->index_condition_list_len = 2;
    search_req->index_condition_list = malloc(sizeof(mdt_index_condition_t) * 2);
    mdt_index_condition_t* query_index_cond1 = &search_req->index_condition_list[0];
    mdt_index_condition_t* query_index_cond2 = &search_req->index_condition_list[1];
    query_index_cond1->index_name.data = "Query";
    query_index_cond1->index_name.size = strlen(query_index_cond1->index_name.data);
    query_index_cond1->comparator = mdt_cmp_greater;
    query_index_cond1->compare_value.data = "apple";
    query_index_cond1->compare_value.size = strlen(query_index_cond1->compare_value.data);

    query_index_cond2->index_name.data = "Query";
    query_index_cond2->index_name.size = strlen(query_index_cond2->index_name.data);
    query_index_cond2->comparator = mdt_cmp_less;
    query_index_cond2->compare_value.data = "car";
    query_index_cond2->compare_value.size = strlen(query_index_cond2->compare_value.data);

    mdt_search_response_t* search_resp = malloc(sizeof(mdt_search_response_t));

    printf("get ...\n");
    mdt_search(table, search_req, search_resp, NULL, NULL);
    for (size_t i = 0; i < search_resp->result_list_len; i++) {
        const mdt_search_result_t* result = &search_resp->result_list[i];
        printf("primary key: %s\n", result->primary_key.data);
        for (size_t j = 0; j < result->data_list_len; j++) {
            printf("        data: %s\n", result->data_list[j].data);
        }
    }

    printf("done\n");
    return 0;
}
