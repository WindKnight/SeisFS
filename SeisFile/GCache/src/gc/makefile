include ../../Makefile.conf

SRC_DIR := ../..
INC_DIR := $(SRC_DIR)/$(INC_DIR)
#OBJ_DIR := $(SRC_DIR)/$(OBJ_DIR)
INSTALL_LIB_PATH := $(SRC_DIR)/$(INSTALL_LIB_PATH)


#包含需要的头文件路径  
CFLAGS += -I$(INC_DIR) $(EXT_INC) $(EXT_LIB)

#搜索指定目录下的所有以.c结尾的文件（搜索结果保存在SOURCE,结果中包含路径相关信息）
SOURCE = $(wildcard *.cpp)
#去除结果中的路径信息
SOURCER = $(notdir $(SOURCE))
#把.c换成.o
OBJS = $(patsubst %.cpp,%.o,$(SOURCER))

#把所有.o加上想要的路径信息
#OBJS := $(foreach X,$(OBJS),$(OBJ_DIR)/$(X))
#OBJS := $(OBJ_DIR)/*.o
#OBJS := $(OBJ_DIR)/a.o \
#   $(OBJ_DIR)/main.o
#OBJS := $(foreach x,$(OBJS),$(x).o)
all : $(OBJS)

$(OBJS):%.o: %.cpp
	$(CC) $(CFLAGS) -c $^ -o $@
#	mv $(OBJS)  $(OBJ_DIR)

clean: 
	rm -f $(OBJS)
#	cd $(OBJ_DIR) && rm -f $(OBJS)
#$(TARGET):$(OBJS)
#   $(CC) $(CFLAGS) -o $(TARGET_DIR)/$@ $(OBJS)
#用.o生成静态库文件.a,并放到指定目录下