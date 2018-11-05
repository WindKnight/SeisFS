

OPTFLAGS	= -O3 -Wall -rdynamic
CFLAGS		= -fPIC -pipe -g -Wall -D_REENTRANT $(OPTFLAGS)
CXXFLAGS	= -fPIC -pipe -g -Wall -D_REENTRANT $(OPTFLAGS)

CC		= gcc
CXX		= g++
CXXLINKER	=$(CXX)

INCPATH		+= -I. -I../include
LIBS		+= -L./  -L../lib -lseisfs

AR		= ar rvcs
MKDIR		= mkdir -p

OBJECTS		= test.o

TARGET		= ./test
INCDIR		= 
BINDIR		=
LIBDIR		=

CHK_DIR_EXISTS	= test -d
DEL_FILE		= rm -f


%.o : %.cpp
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"
%.o : %.cc
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"
%.o : %.cxx
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"
%.o : %.C
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"
%.o : %.c
	$(CC) -c $(CFLAGS) $(INCPATH) -o "$@" "$<"
	
all : $(TARGET)

$(TARGET) : $(OBJECTS)
	$(CXXLINKER)  $(OBJECTS) $(LIBS) -o $(TARGET)
	
	
clean:
	-$(DEL_FILE) $(OBJECTS)
	-$(DEL_FILE) $(TARGET) 
