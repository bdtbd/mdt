#ifndef AGENT_OPTIONS_H_
#define AGENT_OPTIONS_H_

#include <string>
#include <iostream>
#include "leveldb/db.h"

namespace mdt {
namespace agent {

struct EventMask {
    unsigned int flag;
    const char* name;
};

enum DBTYPE {
    DISKDB = 1,
    MEMDB = 2,
};
struct LogOptions {
    DBTYPE db_type;
    std::string db_dir;
    leveldb::DB* db;
};

}
}
#endif
