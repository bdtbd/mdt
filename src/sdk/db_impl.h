/***************************************************************************
 * 
 * Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
#ifndef  __SRC/SDK/DB_IMPL_H_
#define  __SRC/SDK/DB_IMPL_H_

// table in memory control structure
struct tera_adapter {
    tera::Client* client;
    std::map<std::string, tera::table*> tera_table_map; // <table_name, table desc>
};

struct fs_adapter {
    std::string root_path;
    // TODO:
};

class database_impl : public database {
public:
    // create fs namespace
    static int CreateDB(std::string db_name, database** db);
    // if db not exit, create it
    static int OpenTable(const CreateRequest& request, CreateResponse* response, table** table_ptr);

private:
    std::string db_name;
    std::map<std::string, table_impl*> table_map; // <table_name, table ptr>
};

class table_impl : public table {
public:
    int Put(const StoreRequest* request, StoreResponse* response, StoreCallback callback);
    int Put(const StoreRequest* request, StoreResponse* response);

private:
    TableDescription table_desc;
    tera_adapter _tera;
    fs_adapter _fs;
};

#endif  //__SRC/SDK/DB_IMPL_H_
