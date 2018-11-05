

OPTFLAGS	= -O3 -Wall -rdynamic
CFLAGS		= -fPIC -pipe -g -Wall -D_REENTRANT $(OPTFLAGS)
CXXFLAGS	= -fPIC -pipe -g -Wall -D_REENTRANT $(OPTFLAGS)

CC		= gcc
CXX		= g++
CXXLINKER	=$(CXX)

INCPATH		+= -I. -I../include -I../../include
LIBS		+= -L./  -L../lib -lseisfile

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
