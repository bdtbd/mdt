#include "sdk/db.h"
#include "sdk/db_impl.h"

namespace mdt{

Status Database::CreateDB(const Options& options,
                          std::string& db_name,
                          Database** db_ptr) {
    return DatabaseImpl::CreateDB(options, db_name, db_ptr);
}

Database::~Database() {}

}
