
include ../Makefile.conf

C_FLAGS	= -pipe -ggdb3 -O0 -Wall -std=c++0x #-fsanitize=address

SRC_DIR = ..
INC_DIR := $(SRC_DIR)/$(INC_DIR)

LD_LIBRARY_PATH += $(SRC_DIR)/$(INSTALL_LIB_PATH)
EXT_LIB	:= -L$(SRC_DIR)/$(INSTALL_LIB_PATH)  $(EXT_LIB) -lpthread
SRCS := $(wildcard *.cpp)
TARGET  := $(patsubst %.cpp, %, $(SRCS))
OBJDIR = $(SRC_DIR)/bin

.cpp:	
	$(CC) $(C_FLAGS) -o $@ $< -I$(INC_DIR) $(EXT_INC) $(EXT_LIB)
	mkdir -p $(OBJDIR)
	mv -f $@ $(OBJDIR)
	rm -rf *.o
all : $(TARGET)
clean:
#	rm -rf *.o
	rm -rf $(TARGET)
	rm -rf $(OBJDIR)

