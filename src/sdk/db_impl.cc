
namespace mdt {

// Database ops
Options InitDefaultOptions(const Options& options, const std::string& db_name) {
    Options opt = options;
    Status s = opt.env->CreateDir(db_name);
    return opt;
}

DatabaseImpl::DatabaseImpl(const Options& options, const std::string& db_name) 
    : options_(InitDefaultOptions(options, db_name)),
      db_name_(db_name) {
    // tera relative
    tera::ErrorCode error_code;
    std::string tera_log_prefix = "BigQueryTeraDB";
    tera_opt_.root_path_ = db_name + "/tera/";
    tera_opt_.tera_flag_ = options.tera_flag_file_path_;
    tera_opt_.client_ = tera::Client::NewClient(tera_opt_.tera_flag_, tera_log_prefix, &error_code);
    assert(tera_opt_.client_);
    
    //tera::Table* table = tera_opt_.client_->OpenTable(db_name);

}

int DatabaseImpl::CreateDB(const Options& options,
                           const std::string& db_name, 
                           Database** db) {
    DatabaseImpl* db_ptr = new DatabaseImpl(options, db_name);
    assert(db_ptr);
    *db = db_ptr;
    // schema table: row=db_name.table_name, cf=major_key_type, qualifer=
    // row=db/table.name, major_key_type
    return 0;
}

int DatabaseImpl::OpenTables(const CreateRequest* req, 
                             CreateResponse* resp) {
    assert(db_name_ == req->db_name);
    std::vector<TableDescription>::iterator it;
    for (it = req->table_descriptor_list.begin();
         it != req->table_descriptor_list.end();
         ++it) {
        if (table_map_.find(it->table_name) != table_map_.end()) {
            continue;
        }
        // construct memory structure
        TableImpl* table_ptr;
        assert(OpenTable(..., &table_ptr));
        table_map_[it->table_name] = table_ptr;

    }
    return 0; 
}

int DatabaseImpl::OpenTable(TableImpl** table) {
     
    TableImpl* table_ptr = new TableImpl(db_name_, table_desc, 
                                         tera_adapter, fs_adapter);
    assert(table_ptr);

    *table = table_ptr;
    return 0;
}

TableImpl::TableImpl(std::string) {

}


// TableImpl ops


}
