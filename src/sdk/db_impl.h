#ifndef  SRC_SDK_DB_IMPL_H_
#define  SRC_SDK_DB_IMPL_H_

namespace mdt {
///////////////////////////////
//      TableImpl class      //
///////////////////////////////
// table in memory control structure
struct TeraOptions;
struct TeraAdapter {
    std::string table_prefix_; // db_name
    TeraOptions opt_;
    std::map<std::string, tera::Table*> tera_table_map_; // <table_name, table desc>
};

struct FileLocation {
    std::string fname_;
    int32_t offset_;
    int32_t size_;

    std::string& SerializeToString();
};

class DataWriter {
public:
    DataWriter(const std::string& fname, WritableFile* file)
        : fname_(fname), file_(file), offset_(0) {}

    AddRecord(const std::string& data, FileLocation* location);

private:
    // write data to filesystem
    std::string fname_;
    WritableFile* file_;
    int32_t offset_; // TODO: no more than 4G per file
};

struct FilesystemAdapter {
    std::string root_path_; // data file dir
    Env* env_;

    DataWriter writer_;
};

struct Options {
    std::string tera_flag_file_path_; // tera.flag's path
    Env* env_;
};

class TableImpl;
struct PutContext {
    const StoreRequest* req_;
    StoreResponse* resp_;
    StoreCallback callback_;
    TableImpl* table_;
    Counter counter_; // atomic counter

    PutContext(TableImpl* table,
               const StoreRequest* request,
               StoreResponse* response,
               StoreCallback callback)
        : table_(table), req_(request),
        resp_(response), callback_(callback) {}
};

class TableImpl : public Table {
public:
    int Put(const StoreRequest* request, StoreResponse* response, StoreCallback callback);
    int Put(const StoreRequest* request, StoreResponse* response);
    int Get(const SearchRequest* request, SearchResponse* response, SearchCallback callback);
    int Get(const SearchRequest& request, SearchResponse* response);

private:
    TableDescription table_desc_;
    TeraAdapter tera_;
    FilesystemAdapter fs_;
};

///////////////////////////////////
//      DatabaseImpl class       //
///////////////////////////////////
struct FilesystemOptions {
    std::string fs_path_; // db_name + FileSystem
};

struct TeraOptions {
    std::string root_path_; // path of tera dir
    std::string tera_flag_; // path of tera.flag
    tera::Client* client_;

    // schema table(kv), key = table_name, value = BigQueryTableSchema (define in kv.proto)
    std::string schema_table_name_;
    tera::Table* schema_table_;
};

class DatabaseImpl : public Database {
public:
    // create fs namespace
    static int CreateDB(const Options& options, std::string db_name, Database** db);
    // if db not exit, create it
    int CreateTable(const CreateRequest& request, CreateResponse* response, Table** table_ptr);

private:
    std::string db_name_;
    const Options options_;
    FilesystemOptions fs_opt_;
    TeraOptions tera_opt_;
    std::map<std::string, TableImpl*> table_map_; // <table_name, table ptr>
};

}
#endif  //SRC_SDK_DB_IMPL_H_
