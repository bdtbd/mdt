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
DEFINE_int32(file_stream_max_pending_request, 10000, "max pending write req");
DEFINE_string(scheduler_addr, "0.0.0.0:11111", "scheduler server addr");
DEFINE_string(db_dir, "../leveldb_dir/", "leveldb dir for cp");
DEFINE_string(watch_log_dir, "../watch_log_dir/", "log dir");
DEFINE_string(module_name_list, "tabletnode.1.", "identify module name");



