################################################################
# Note: Edit the variable below to help find your own package
#       that tera depends on.
#       If you build tera using build.sh or travis.yml, it will
#       automatically config this for you.
################################################################

BOOST_INCDIR=thirdparty/boost_1_57_0
SOFA_PBRPC_PREFIX=thirdparty
PROTOBUF_PREFIX=thirdparty
SNAPPY_PREFIX=thirdparty
ZOOKEEPER_PREFIX=thirdparty
GFLAGS_PREFIX=thirdparty
GLOG_PREFIX=thirdparty
GTEST_PREFIX=thirdparty
GPERFTOOLS_PREFIX=thirdparty
INS_PREFIX=thirdparty
TERA_PREFIX=thirdparty
GALAXY_PREFIX=thirdparty
COMMON_PREFIX=thirdparty

SOFA_PBRPC_INCDIR = $(SOFA_PBRPC_PREFIX)/include
PROTOBUF_INCDIR = $(PROTOBUF_PREFIX)/include
SNAPPY_INCDIR = $(SNAPPY_PREFIX)/include
ZOOKEEPER_INCDIR = $(ZOOKEEPER_PREFIX)/include
GFLAGS_INCDIR = $(GFLAGS_PREFIX)/include
GLOG_INCDIR = $(GLOG_PREFIX)/include
GTEST_INCDIR = $(GTEST_PREFIX)/include
GPERFTOOLS_INCDIR = $(GPERFTOOLS_PREFIX)/include
INS_INCDIR = $(INS_PREFIX)/include
TERA_INCDIR = $(TERA_PREFIX)/include
GALAXY_INCDIR = $(GALAXY_PREFIX)/include

SOFA_PBRPC_LIBDIR = $(SOFA_PBRPC_PREFIX)/lib
PROTOBUF_LIBDIR = $(PROTOBUF_PREFIX)/lib
SNAPPY_LIBDIR = $(SNAPPY_PREFIX)/lib
ZOOKEEPER_LIBDIR = $(ZOOKEEPER_PREFIX)/lib
GFLAGS_LIBDIR = $(GFLAGS_PREFIX)/lib
GLOG_LIBDIR = $(GLOG_PREFIX)/lib
GTEST_LIBDIR = $(GTEST_PREFIX)/lib
GPERFTOOLS_LIBDIR = $(GPERFTOOLS_PREFIX)/lib
INS_LIBDIR = $(INS_PREFIX)/lib
TERA_LIBDIR = $(TERA_PREFIX)/lib
GALAXY_LIBDIR = $(GALAXY_PREFIX)/lib
COMMON_LIBDIR = $(COMMON_PREFIX)/lib

PROTOC = $(PROTOBUF_PREFIX)/bin/protoc

################################################################
# Note: No need to modify things below.
################################################################

DEPS_INCPATH = -I$(SOFA_PBRPC_INCDIR) -I$(PROTOBUF_INCDIR) \
               -I$(SNAPPY_INCDIR) -I$(ZOOKEEPER_INCDIR) \
               -I$(GFLAGS_INCDIR) -I$(GLOG_INCDIR) -I$(GTEST_INCDIR) \
               -I$(GPERFTOOLS_INCDIR) -I$(BOOST_INCDIR) -I$(INS_INCDIR) \
               -I$(TERA_INCDIR) -I$(GALAXY_INCDIR)
DEPS_LDPATH = -L$(TERA_LIBDIR) -L$(SOFA_PBRPC_LIBDIR) -L$(PROTOBUF_LIBDIR) \
              -L$(SNAPPY_LIBDIR) -L$(ZOOKEEPER_LIBDIR) \
              -L$(GFLAGS_LIBDIR) -L$(GLOG_LIBDIR) -L$(GTEST_LIBDIR) \
              -L$(GPERFTOOLS_LIBDIR) -L$(INS_LIBDIR) \
              -L$(TERA_LIBDIR) -L$(GALAXY_LIBDIR)
DEPS_LDFLAGS = -ltera -lins_sdk -lsofa-pbrpc -lprotobuf -lsnappy -lzookeeper_mt \
               -lgtest_main -lgtest -lglog -lgflags -lgalaxy -lcommon -ltcmalloc_minimal
DEPS_LIBRARIES = $(COMMON_LIBDIR)/libcommon.a $(GALAXY_LIBDIR)/libgalaxy.a $(TERA_LIBDIR)/libtera.a $(INS_LIBDIR)/libins_sdk.a \
                 $(SOFA_PBRPC_LIBDIR)/libsofa-pbrpc.a $(PROTOBUF_LIBDIR)/libprotobuf.a \
                 $(SNAPPY_LIBDIR)/libsnappy.a $(ZOOKEEPER_LIBDIR)/libzookeeper_mt.a \
                 $(GTEST_LIBDIR)/libgtest_main.a $(GTEST_LIBDIR)/libgtest.a \
                 $(GLOG_LIBDIR)/libglog.a $(GFLAGS_LIBDIR)/libgflags.a $(GPERFTOOLS_LIBDIR)/libtcmalloc_minimal.a 

