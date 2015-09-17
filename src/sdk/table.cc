#include "sdk/table.h"
#include "sdk/table_impl.h"

namespace mdt {

int Table::OpenTable(const std::string& db_name, const TeraOptions& tera_opt,
                     const FilesystemOptions& fs_opt, const TableDescription& table_desc,
                                      Table** table_ptr) {

    return TableImpl::OpenTable(db_name, tera_opt, fs_opt, table_desc, table_ptr);
}

}
