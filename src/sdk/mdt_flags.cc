#include <gflags/gflags.h>

// nfs cluster location
DEFINE_string(env_fs_type, "nfs", "file system type");
DEFINE_string(env_nfs_mountpoint, "/disk/DefaultDir/", "nfs mount point");
DEFINE_string(env_nfs_conf_path, "../conf/nfs.conf", "nfs's client configure file");
DEFINE_string(env_nfs_so_path, "../lib/libnfs.so", "nfs's client's .so lib");

// tera cluster location
DEFINE_string(tera_flag_file_path, "../conf/tera.flag", "tera's client's configure file");
DEFINE_string(tera_root_dir, "/disk/tera/TeraDatabase/", "tera cluster's dir");

// mdt cluster

