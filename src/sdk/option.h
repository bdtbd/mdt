#ifndef MDT_SDK_OPTION_H_
#define MDT_SDK_OPTION_H_

#include "util/env.h"
#include "util/tera.h"

namespace mdt {

struct Options {
    std::string tera_flag_file_path_; // tera.flag's path
    Env* env_;
};

struct FilesystemOptions {
    std::string fs_path_; // db_name + FileSystem
    Env* env_;
};

struct TeraOptions {
    std::string root_path_; // path of tera dir
    std::string tera_flag_; // path of tera.flag
    tera::Client* client_;

    // schema table(kv), key = table_name, value = BigQueryTableSchema (define in kv.proto)
    std::string schema_table_name_;
    tera::Table* schema_table_;
};

}

#endif // MDT_SDK_OPTION_H_
