#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>
#include <sys/stat.h>

#include "sdk/sdk.h"
#include "sdk/db.h"
#include "sdk/table.h"

#include "util/env.h"
#include "util/coding.h"

DECLARE_string(flagfile);
DECLARE_string(log_file);

void SetupLog(const std::string& name) {
    std::string program_name = "mdt";
    if (!name.empty()) {
        program_name = name;
    }

    if (FLAGS_log_dir.size() == 0) {
        FLAGS_log_dir = "./log";
    }

    if (access(FLAGS_log_dir.c_str(), F_OK)) {
        mkdir(FLAGS_log_dir.c_str(), 0777);
    }

    std::string log_filename = FLAGS_log_dir + "/" + program_name + ".INFO.";
    std::string wf_filename = FLAGS_log_dir + "/" + program_name + ".WARNING.";
    google::SetLogDestination(google::INFO, log_filename.c_str());
    google::SetLogDestination(google::WARNING, wf_filename.c_str());
    google::SetLogDestination(google::ERROR, "");
    google::SetLogDestination(google::FATAL, "");

    google::SetLogSymlink(google::INFO, program_name.c_str());
    google::SetLogSymlink(google::WARNING, program_name.c_str());
    google::SetLogSymlink(google::ERROR, "");
    google::SetLogSymlink(google::FATAL, "");
}

void LocationSerialToStringTest() {
    // location.SerialToString test
    std::string s = "90-10:89:10-87";
    uint32_t offset = 100;
    uint32_t size = 40;
    char buf[8];
    char* p = buf;
    mdt::EncodeBigEndian32(p, offset);
    mdt::EncodeBigEndian32(p + 4, size);
    std::string ss(buf, 8);
    std::string res = s + ss;
    LOG(INFO) << "location serial test : " << res;
}

void SetupGoogleLog() {
    // init param, setup log
    std::string log_prefix = "mdt";
    ::google::InitGoogleLogging(log_prefix.c_str());
    SetupLog(log_prefix);
    LOG(INFO) << "start loging...";
}

int main(int ac, char* av[]) {
    ::google::ParseCommandLineFlags(&ac, &av, true);
    SetupGoogleLog();

    LocationSerialToStringTest();

    // create db
    mdt::Database* db;
    std::string db_name = "mdt-test";
    db = mdt::OpenDatabase(db_name);

    // create table
    mdt::TableDescription table_desc;
    CreateTable(db, table_desc);

    mdt::Table* table;
    std::string table_name = "table";
    table = OpenTable(db, table_name);

    // insert data
    mdt::StoreRequest store_req;
    mdt::StoreResponse store_resp;
    mdt::StoreCallback callback;
    table->Put(&store_req, &store_resp, callback);
    return 0;
}

