#ifndef MDT_SDK_TABLE_H_
#define MDT_SDK_TABLE_H_

#include "sdk/sdk.h"
#include "sdk/option.h"

namespace mdt {

class Table {
public:
    static int OpenTable(const std::string& db_name, const TeraOptions& tera_opt,
                         const FilesystemOptions& fs_opt, const TableDescription& table_desc,
                         Table** table_ptr);
    Table() {}
    virtual ~Table() {}
    virtual int Put(const StoreRequest* request, StoreResponse* response, StoreCallback callback);
    virtual int Put(const StoreRequest* request, StoreResponse* response);
    virtual int Get(const SearchRequest* request, SearchResponse* response, SearchCallback callback);
    virtual int Get(const SearchRequest& request, SearchResponse* response);

    virtual std::string& TableName();

private:
    Table(const Table&);
    void operator=(const Table&);
};

}
#endif // MDT_SDK_TABLE_H_
