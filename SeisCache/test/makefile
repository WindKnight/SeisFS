include $(SEISFS_HOME)/Makefile.inc


LIBS		+= -L./  -L$(SEISCACHE_HOME)/lib -lseiscache -rdynamic
LIBS		+= -L$(SEISFS_HOME)/lib -lseisfs
LIBS		+= -L$(SEISFILEHDFS_HOME)/lib -lsfhdfs
#LIBS		+= -L$(GCACHE_HOME)/lib -lsfgcache
LIBS    	+= -L$(GEFILE_HOME)/lib -ldpi_fs_cssl


MKDIR		= mkdir -p

OBJECTS		= test.o

TARGET		= ./test

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
	$(CXXLINKER) $(LIBS) $(OBJECTS) -o $(TARGET)
	
	
clean:
	-$(DEL_FILE) $(OBJECTS)
	-$(DEL_FILE) $(TARGET) 
	-$(DEL_FILE) *.core *.o
