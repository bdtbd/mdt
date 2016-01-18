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

struct AgentInfo {
    int64_t qps_use;
    int64_t qps_quota;
    int64_t bandwidth_use;
    int64_t bandwidth_quota;

    int64_t max_packet_size;
    int64_t min_packet_size;
    int64_t average_packet_size;

    int64_t error_nr;
    std::string collector_addr;
};

}
}
#endif
