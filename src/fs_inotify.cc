#include <sys/inotify.h>
#include "agent/options.h"

///////////////////////////////////////////
// agent flags
///////////////////////////////////////////
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


