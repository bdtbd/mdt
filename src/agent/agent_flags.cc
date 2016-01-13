#include <sys/inotify.h>
#include "agent/options.h"
#include <gflags/gflags.h>

mdt::agent::EventMask event_masks[] = {
    {IN_ACCESS        , "IN_ACCESS"}        ,
    {IN_ATTRIB        , "IN_ATTRIB"}        ,
    {IN_CLOSE_WRITE   , "IN_CLOSE_WRITE"}   ,
    {IN_CLOSE_NOWRITE , "IN_CLOSE_NOWRITE"} ,
    {IN_CREATE        , "IN_CREATE"}        ,
    {IN_DELETE        , "IN_DELETE"}        ,
    {IN_DELETE_SELF   , "IN_DELETE_SELF"}   ,
    {IN_MODIFY        , "IN_MODIFY"}        ,
    {IN_MOVE_SELF     , "IN_MOVE_SELF"}     ,
    {IN_MOVED_FROM    , "IN_MOVED_FROM"}    ,
    {IN_MOVED_TO      , "IN_MOVED_TO"}      ,
    {IN_OPEN          , "IN_OPEN"}          ,
    {IN_DONT_FOLLOW   , "IN_DONT_FOLLOW"}   ,
    {IN_EXCL_UNLINK   , "IN_EXCL_UNLINK"}   ,
    {IN_MASK_ADD      , "IN_MASK_ADD"}      ,
    {IN_ONESHOT       , "IN_ONESHOT"}       ,
    {IN_ONLYDIR       , "IN_ONLYDIR"}       ,
    {IN_IGNORED       , "IN_IGNORED"}       ,
    {IN_ISDIR         , "IN_ISDIR"}         ,
    {IN_Q_OVERFLOW    , "IN_Q_OVERFLOW"}    ,
    {IN_UNMOUNT       , "IN_UNMOUNT"}       ,
};

DEFINE_string(agent_service_port, "33331", "agent port");
DEFINE_int32(file_stream_max_pending_request, 1000, "max pending write req");
DEFINE_string(scheduler_addr, "0.0.0.0:11111", "scheduler server addr");
DEFINE_string(db_dir, "../leveldb_dir/", "leveldb dir for cp");
DEFINE_string(watch_log_dir, "../watch_log_dir/", "log dir");
DEFINE_string(module_name_list, "tabletnode.1.", "identify module name");

DEFINE_string(db_name, "mdttrace-debug", "production name");
DEFINE_string(table_name, "trace", "table name");
DEFINE_string(primary_key, "", "primary key name");
DEFINE_string(user_time, "", "user point out which field use as timestamp");
DEFINE_int32(time_type, 1, "use for parse user time from log");

// split string by substring
//DEFINE_string(string_delims, "||", "split string by substring");
DEFINE_string(string_delims, "", "split string by substring");

// split string by char
DEFINE_string(line_delims, " ", "log tailer's line delim");
DEFINE_string(kv_delims, "=", "log tailer's kv delim");
DEFINE_bool(enable_index_filter, false, "do not filter log line by index list in agent");
DEFINE_string(index_list, "", "index table name list");
DEFINE_string(alias_index_list, "", "alias index table name list");

// split string by index number
DEFINE_bool(use_fixed_index_list, true, "use fixed index list");
DEFINE_string(fixed_index_list, "url:5,time:2", "use for fix index list match");

