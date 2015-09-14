/***************************************************************************
 * 
 * Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
#ifndef  __SRC/SDK/DB_IMPL_H_
#define  __SRC/SDK/DB_IMPL_H_

namespace mdt {
// table in memory control structure
struct TeraAdapter {
    tera::Client* client;
    std::map<std::string, tera::Table*> tera_table_map; // <table_name, table desc>
};

struct FsAdapter {
    std::string root_path;
    // TODO:
};

class TableImpl : public Table {
public:
    int Put(const StoreRequest* request, StoreResponse* response, StoreCallback callback);
    int Put(const StoreRequest* request, StoreResponse* response);

private:
    TableDescription table_desc;
    TeraAdapter _tera;
    FsAdapter _fs;
};

class DatabaseImpl : public Database {
public:
    // create fs namespace
    static int CreateDB(std::string db_name, Database** db);
    // if db not exit, create it
    static int OpenTable(const CreateRequest& request, CreateResponse* response, Table** table_ptr);

private:
    std::string db_name;
    std::map<std::string, TableImpl*> table_map; // <table_name, table ptr>
};

}
#endif  //__SRC/SDK/DB_IMPL_H_
