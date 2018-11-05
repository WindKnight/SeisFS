######################################################################################################
# Makefile.inc   				28 June 2018
######################################################################################################


#######################################   DEFINES    #################################################

#detect arch
arch = G_ARCH_GENERIC
uname_m = $(shell uname -m)
ifeq ($(uname_m), i686)
	arch = G_ARCH_I386
endif
ifeq ($(uname_m), ia64)
	arch = G_ARCH_IA64
endif
ifeq ($(uname_m), x86_64)
	arch = G_ARCH_X86_64
endif

DEFINES = -D$(arch) -DG_CC_GNU -DG_DISABLE_STRAGGLER_TOLERANCE -D_REENTRANT

#######################################   INCPATH    #################################################

INCPATH += -I$(SEISFS_HOME)/include
INCPATH += -I$(SEISCACHE_HOME)/include 
INCPATH += -I$(SEISFILE_HOME)/include 
INCPATH += -I$(SEISFILEHDFS_HOME)/include
INCPATH += -I$(SEISFILENAS_HOME)/include
INCPATH += -I$(GCACHE_HOME)/include
INCPATH += -I$(GLOCK_HOME)
INCPATH += -I$(ZOOKEEPER_INSTALL)/src/c/include/zookeeper
INCPATH += -I$(GPP)/include/

INCPATH += -I$(HDFS_INCLUDE)
INCPATH += -I$(GEOBENCH)/third/libuuid-1.0.3/include
INCPATH += -I$(GEOBENCH)/third/boost/include
INCPATH += -I$(ZSTD_HOME)/lib




######################################   COMMAND    ###############################################

MAKE = make

CC = gcc
CLINKER = gcc

CXX = g++
CXXLINKER = g++

AR = ar

CHK_DIR_EXISTS= test -d
DEL_FILE      = rm -f
MKDIR		= mkdir -p

#######################################   FLAGS    #################################################


OPTFLAGS = -std=c++0x -O3 -g -Wall -rdynamic -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE

CFLAGS = -fPIC $(INCPATH) $(OPTFLAGS) $(DEFINES)
CXXFLAGS = -fPIC $(INCPATH)  $(OPTFLAGS) $(DEFINES)

LDFLAGS = -shared 
ARFLAGS = rv
