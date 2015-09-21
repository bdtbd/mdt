include depends.mk

# OPT ?= -O2 -DNDEBUG       # (A) Production use (optimized mode)
OPT ?= -g2 -Wall -Werror      # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DNDEBUG   # (C) Profiling mode: opt, but w/debugging symbols

CC = cc
CXX = g++

SHARED_CFLAGS = -fPIC
SHARED_LDFLAGS = -shared -Wl,-soname -Wl,

INCPATH += -I./src -I./include $(DEPS_INCPATH) 
CFLAGS += $(OPT) $(SHARED_CFLAGS) $(INCPATH)
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
SAMPLE_SRC := $(wildcard src/sample/*.cc)

SDK_OBJ := $(SDK_SRC:.cc=.o)
COMMON_OBJ := $(COMMON_SRC:.cc=.o)
UTIL_OBJ := $(UTIL_SRC:.cc=.o)
PROTO_OBJ := $(PROTO_SRC:.cc=.o)
VERSION_OBJ := $(VERSION_SRC:.cc=.o)
SAMPLE_OBJ := $(SAMPLE_SRC:.cc=.o)

ALL_OBJ := $(SDK_OBJ) $(COMMON_OBJ) $(UTIL_OBJ) $(PROTO_OBJ) $(VERSION_OBJ) $(SAMPLE_OBJ)

PROGRAM = 
LIBRARY = libmdt.a
SAMPLE = sample

.PHONY: all clean cleanall test

all: $(PROGRAM) $(LIBRARY) $(SAMPLE)
	mkdir -p build/include build/lib build/bin
	#cp $(PROGRAM) build/bin
	cp $(LIBRARY) build/lib
	cp src/sdk/sdk.h build/include/mdt.h
	cp -r conf build
	echo 'Done'

clean:
	rm -rf $(ALL_OBJ)
	rm -rf $(PROGRAM) $(LIBRARY) $(SAMPLE)

cleanall:
	$(MAKE) clean
	rm -rf build

sample: $(SAMPLE_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(SAMPLE_OBJ) $(LIBRARY) $(LDFLAGS)

libmdt.a: $(SDK_OBJ) $(COMMON_OBJ) $(UTIL_OBJ) $(PROTO_OBJ) $(VERSION_OBJ)
	$(AR) -rs $@ $(SDK_OBJ) $(COMMON_OBJ) $(UTIL_OBJ) $(PROTO_OBJ) $(VERSION_OBJ)

$(ALL_OBJ): %.o: %.cc $(PROTO_OUT_H)
	$(CXX) $(CXXFLAGS) -c $< -o $@

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
