

OPTFLAGS	= -Wall -rdynamic
CFLAGS		= -fPIC -pipe -g -Wall -D_REENTRANT $(OPTFLAGS)
CXXFLAGS	= -fPIC -pipe -g -Wall -D_REENTRANT $(OPTFLAGS)

CC		= gcc -g
CXX		= g++ -g
CXXLINKER	=$(CXX)

#INCPATH		+= -I. -I../include -I../../include -I../../../include -I$(GEFILE_HOME)/include
#LIBS		+= -L./  -L$(SEISFILE_HOME)/lib -lseisfile -L$(SEISFILENAS_HOME)/lib -lsfnas -L$(GEFILE_HOME)/lib -ldpi_fs_cssl -L/s0/ext/extlmc12/workspace/SeisFS/SeisFS-0.4_cssl/lib -lseisfs
INCPATH		+= -I. -I../include -I../../include -I../../../include -I$(GEFILE_HOME)/include

LIBS		+= -L$(SEISFILEHDFS_HOME)/lib -lsfhdfs
LIBS		+= -L./  -L$(SEISFILE_HOME)/lib -lseisfile -L$(SEISFILENAS_HOME)/lib -lsfnas -L$(GEFILE_HOME)/lib -ldpi_fs_cssl -L$(SEISFS_HOME)/lib -lseisfs
#LIBS		+= -L$(HDFS_LIB) -lhdfs3
#LIBS            += -L$(GCACHE_HOME)/lib -lsfgcache

AR		= ar rvcs
MKDIR		= mkdir -p

OBJECTS		= test.o

TARGET		= ./test
INCDIR		= 
BINDIR		=
LIBDIR		=

CHK_DIR_EXISTS	= test -d
DEL_FILE		= rm -f

first : all

.SUFFIXES: .o .c .cpp .cc .cxx .C

.cpp.o:
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"
.cc.o:
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"
.cxx.o:
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"
.C.o:
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"
.c.o:
	$(CC) -c $(CFLAGS) $(INCPATH) -o "$@" "$<"
	
all : $(TARGET)

$(TARGET) : $(OBJECTS)
	$(CXXLINKER)  $(OBJECTS) $(LIBS) -o $(TARGET)
	
	
clean:
	-$(DEL_FILE) $(OBJECTS)
	-$(DEL_FILE) $(TARGET) 
	-$(DEL_FILE) *.core *.o
