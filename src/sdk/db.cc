#include "sdk/db.h"
#include "sdk/db_impl.h"

namespace mdt{

Status Database::CreateDB(const Options& options,
                             std::string& db_name,
                         Database** db_ptr) {
        DatabaseImpl* db_impl = new DatabaseImpl(options, db_name);
        assert(db_impl);
         *db_ptr = db_impl;
       return Status::OK();
     }

Database::~Database() {}

}
