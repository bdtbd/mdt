include depends.mk

# OPT ?= -O2 -DNDEBUG       # (A) Production use (optimized mode)
OPT ?= -g2 -Wall -Werror      # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DNDEBUG   # (C) Profiling mode: opt, but w/debugging symbols

CC = gcc
CXX = g++

SHARED_CFLAGS = -fPIC
SHARED_LDFLAGS = -shared -Wl,-soname -Wl,

INCPATH += -I./src -I./include $(DEPS_INCPATH) 
CFLAGS += -std=c99 $(OPT) $(SHARED_CFLAGS) $(INCPATH)
CXXFLAGS += $(OPT) $(SHARED_CFLAGS) $(INCPATH)
LDFLAGS += -rdynamic $(DEPS_LDPATH) $(DEPS_LDFLAGS) -lpthread -lrt -lz -ldl

PROTO_FILES := $(wildcard src/proto/*.proto)
PROTO_OUT_CC := $(PROTO_FILES:.proto=.pb.cc)
PROTO_OUT_H := $(PROTO_FILES:.proto=.pb.h)

SDK_SRC := $(wildcard src/sdk/*.cc)
COMMON_SRC := $(wildcard src/common/*.cc)
UTIL_SRC := $(wildcard src/util/*.cc)
PROTO_SRC := $(filter-out %.pb.cc, $(wildcard src/proto/*.cc)) $(PROTO_OUT_CC)
VERSION_SRC := src/version.cc
MDTTOOL_SRC := $(wildcard src/mdt-tool/mdt-tool.cc)
SAMPLE_SRC := $(wildcard src/sample/mdt_test.cc)
C_SAMPLE_SRC := $(wildcard src/sample/c_sample.c)

SDK_OBJ := $(SDK_SRC:.cc=.o)
COMMON_OBJ := $(COMMON_SRC:.cc=.o)
UTIL_OBJ := $(UTIL_SRC:.cc=.o)
PROTO_OBJ := $(PROTO_SRC:.cc=.o)
VERSION_OBJ := $(VERSION_SRC:.cc=.o)
SAMPLE_OBJ := $(SAMPLE_SRC:.cc=.o)
MDTTOOL_OBJ := $(MDTTOOL_SRC:.cc=.o)

C_SAMPLE_OBJ := $(C_SAMPLE_SRC:.c=.o)

CXX_OBJ := $(SDK_OBJ) $(COMMON_OBJ) $(UTIL_OBJ) $(PROTO_OBJ) $(VERSION_OBJ) \
           $(SAMPLE_OBJ) $(MDTTOOL_OBJ)
C_OBJ := $(C_SAMPLE_OBJ)

PROGRAM = 
LIBRARY = libmdt.a
SAMPLE = sample
MDTTOOL = mdt-tool
MDTTOOL_TEST = mdt-tool-test
C_SAMPLE = c_sample
.PHONY: all clean cleanall test
all: $(PROGRAM) $(LIBRARY) $(SAMPLE) $(C_SAMPLE) $(MDTTOOL)
	mkdir -p build/include build/lib build/bin
	#cp $(PROGRAM) build/bin
	cp $(LIBRARY) build/lib
	cp src/sdk/sdk.h build/include/mdt.h
	cp -r conf build
	echo 'Done'

clean:
	rm -rf $(CXX_OBJ) $(C_OBJ)
	rm -rf $(PROGRAM) $(LIBRARY) $(SAMPLE) $(C_SAMPLE) $(MDTTOOL)

cleanall:
	$(MAKE) clean
	rm -rf build

sample: $(SAMPLE_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(SAMPLE_OBJ) $(LIBRARY) $(LDFLAGS)

mdt-tool: $(MDTTOOL_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(MDTTOOL_OBJ) $(LIBRARY) $(LDFLAGS) -lreadline -lhistory -lncurses

c_sample: $(C_SAMPLE_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(C_SAMPLE_OBJ) $(LIBRARY) $(LDFLAGS)

libmdt.a: $(SDK_OBJ) $(COMMON_OBJ) $(UTIL_OBJ) $(PROTO_OBJ) $(VERSION_OBJ)
	$(AR) -rs $@ $(SDK_OBJ) $(COMMON_OBJ) $(UTIL_OBJ) $(PROTO_OBJ) $(VERSION_OBJ)

$(CXX_OBJ): %.o: %.cc $(PROTO_OUT_H)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(C_OBJ): %.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

$(VERSION_SRC): FORCE
	sh build_version.sh

.PHONY: proto
proto: $(PROTO_OUT_CC) $(PROTO_OUT_H)
 
%.pb.cc %.pb.h: %.proto
	$(PROTOC) --proto_path=./src/proto/ --proto_path=$(PROTOBUF_INCDIR) \
              --proto_path=$(SOFA_PBRPC_INCDIR) \
              --cpp_out=./src/proto/ $< 

.PHONY: FORCE
FORCE:
