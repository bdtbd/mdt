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

SDK_SRC := $(wildcard src/sdk/*.cc)
VERSION_SRC := src/version.cc
OTHER_SRC := $(VERSION_SRC)

SDK_OBJ := $(SDK_SRC:.cc=.o)
OTHER_OBJ := $(OTHER_SRC:.cc=.o)

ALL_OBJ := $(SDK_OBJ) $(OTHER_OBJ)

PROGRAM = 
LIBRARY = libmdt.a

.PHONY: all clean cleanall test

all: $(PROGRAM) $(LIBRARY)
	mkdir -p build/include build/lib build/bin
	#cp $(PROGRAM) build/bin
	cp $(LIBRARY) build/lib
	cp src/sdk/sdk.h build/include/mdt.h
	cp -r conf build
	echo 'Done'

clean:
	rm -rf $(ALL_OBJ)
	rm -rf $(PROGRAM) $(LIBRARY)

cleanall:
	$(MAKE) clean
	rm -rf build

libmdt.a: $(SDK_OBJ) $(OTHER_OBJ)
	$(AR) -rs $@ $(SDK_OBJ) $(OTHER_OBJ)

$(VERSION_SRC): FORCE
	sh build_version.sh

.PHONY: FORCE
FORCE:
