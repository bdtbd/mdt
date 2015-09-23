#include <stdio.h>
#include <stdlib.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <iostream>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "sdk/sdk.h"
#include "sdk/db.h"
#include "sdk/table.h"

#include "util/env.h"
#include "util/coding.h"


char* StripWhite(char* line) {
    char *s, *t;
    for (s = line; whitespace(*s); s++);
    if (*s == 0)
        return s;
    t = s + strlen(s) - 1;
    while (t > s && whitespace(*t)) {
        t--;
    }
    *++t = '\0';
    return s;
}

std::string cmd_name[] = {
    "quit",
    "help",
    "OpenDatabase",
};

void HelpManue() {
    printf("========= usage ===========\n");
    printf("cmd: quit\n");
    printf("cmd: help\n");
    printf("cmd: CreateTable dbname tablename primary_key_type [index_name index_type]...\n");
    printf("===========================\n");
}

int CreateTableOp(std::vector<std::string>& cmd_vec) {
    // create db
    std::cout << "open db ..." << std::endl;
    mdt::Database* db;
    std::string db_name = cmd_vec[1];
    db = mdt::OpenDatabase(db_name);

    // create table
    std::cout << "create table ..." << std::endl;
    mdt::TableDescription table_desc;
    table_desc.table_name = cmd_vec[2];
    if (cmd_vec[3].compare("kBytes") != 0) {
        std::cout << "create table fail, primary key type not support!\n";
        return 0;
    }
    table_desc.primary_key_type = mdt::kBytes;

    int num_index = cmd_vec.size() - 4;
    if (num_index % 2 != 0) {
        std::cout << "create table fail, [index_name index_type] not match!\n";
        return 0;
    }
    for (int i = 0; i < num_index; i += 2) {
        mdt::IndexDescription table_index;
        table_index.index_name = cmd_vec[i + 4];
        if (cmd_vec[i + 5].compare("kBytes") != 0) {
            std::cout << "create table fail, index key: " << cmd_vec[i + 4]
                << ", key type not support!\n";
            return 0;
        }
        table_index.index_key_type = mdt::kBytes;
        table_desc.index_descriptor_list.push_back(table_index);
    }
    CreateTable(db, table_desc);
    return 0;
}

int main(int ac, char* av[]) {
    while (1) {
        char *line = readline("mdt:");
        char *cmd = StripWhite(line);
        std::string command(cmd, strlen(cmd));

        // split cmd
        std::vector<std::string> cmd_vec;
        boost::split(cmd_vec, command, boost::is_any_of(" "), boost::token_compress_on);
        if (cmd_vec.size() == 0) {
            free(line);
            continue;
        } else if (cmd_vec[0].compare("quit") == 0) {
            std::cout << "bye\n";
            return 0;
        } else if (cmd_vec[0].compare("help") == 0) {
            HelpManue();
            free(line);
            continue;
        } else if (cmd_vec[0].compare("CreateTable") == 0 && cmd_vec.size() >= 6) {
            // cmd: CreateTable dbname tablename primary_key_type [index_name index_type]...
            std::cout << "create table: dbname " << cmd_vec[1]
                << "tablename " << cmd_vec[2]
                << "\n";
            CreateTableOp(cmd_vec);
            free(line);
            continue;
        } else {
            std::cout << "cmd not known\n";
            free(line);
            continue;
        }

        std::cout << command
            << ", size " << cmd_vec.size()
            << ", cmd " << cmd_vec[0]
            << "\n";
        free(line);
    }
    return 0;
}
