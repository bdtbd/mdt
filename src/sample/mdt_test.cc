#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>
#include <sys/stat.h>

#include "sdk/sdk.h"
#include "sdk/db.h"
#include "sdk/table.h"

#include "util/env.h"

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


int main(int ac, char* av[]) {
    // init param, setup log
    ::google::ParseCommandLineFlags(&ac, &av, true);
    std::string log_prefix = "mdt";
    ::google::InitGoogleLogging(log_prefix.c_str());
    SetupLog(log_prefix);
    LOG(INFO) << "start loging...";

    mdt::Options options;
    mdt::Database* db;
    std::string db_name = "mdt-test";
    mdt::Database::CreateDB(options, db_name, &db);
    mdt::Table* table;
    const mdt::CreateRequest req;
    mdt::CreateResponse resp;
    db->CreateTable(req, &resp, &table);

    const mdt::StoreRequest store_req;
    mdt::StoreResponse store_resp;
    mdt::StoreCallback callback;
    table->Put(&store_req, &store_resp, callback);
    return 0;
}

