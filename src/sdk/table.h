#ifndef MDT_SDK_TABLE_H_
#define MDT_SDK_TABLE_H_

#include "sdk/sdk.h"
#include "sdk/option.h"

namespace mdt {

struct TableProfile {
    uint64_t tera_nr_pending;
};

class Table {
public:
    Table() {}
    virtual ~Table() {}
    virtual int BatchWrite(BatchWriteContext* ctx) = 0;
    virtual int Put(const StoreRequest* request, StoreResponse* response,
                    StoreCallback callback = NULL, void* callback_param = NULL) = 0;
    virtual Status Get(const SearchRequest* request, SearchResponse* response,
                       SearchCallback callback = NULL, void* callback_param = NULL) = 0;

    virtual const std::string& TableName() = 0;
    virtual void Profile(TableProfile* profile) = 0;

private:
    Table(const Table&);
    void operator=(const Table&);
};

}
#endif // MDT_SDK_TABLE_H_
