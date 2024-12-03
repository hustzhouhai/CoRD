CC = g++ -std=c++11
JCC = gcc 
CLIBS = -pthread -lgf_complete lib/libhiredis.a
#CFLAGS = -O3 -mmmx -msse -mpclmul -msse4.2 -DINTEL_SSE4 -mavx
CFLAGS = -g -Wall -O3 -lm -lrt

SRC_DIR =  src
UTIL_SRC_DIR = $(SRC_DIR)/Util

CC_FILES = $(filter-out $(SRC_DIR)/ECCoordinator.cc $(SRC_DIR)/ECHelper.cc $(SRC_DIR)/ECPipeClient.cc, \
			$(wildcard $(SRC_DIR)/*.cc))
UTIL_CC_FILES = $(wildcard $(UTIL_SRC_DIR)/*.cpp)
JERASURE_C_FILES = $(wildcard $(UTIL_SRC_DIR)/*.c)

OBJ_DIR = obj
OBJ_FILES = $(addprefix $(OBJ_DIR)/CC_, $(notdir $(CC_FILES:.cc=.o)))
UTIL_OBJ_FILES = $(addprefix $(OBJ_DIR)/UTIL_, $(notdir $(UTIL_CC_FILES:.cpp=.o)))
JERASURE_OBJ_FILES = $(addprefix $(OBJ_DIR)/J_, $(notdir $(JERASURE_C_FILES:.c=.o)))

O_FILES := $(OBJ_FILES) $(UTIL_OBJ_FILES) $(JERASURE_OBJ_FILES)

all : directories ECCoordinator ECHelper ECPipeClient

test : initRedisTest extractorTest initRedisCycTest PPRIdxTest

directories : $(OBJ_DIR)

$(OBJ_DIR) : 
	mkdir -p $(OBJ_DIR)

ECCoordinator : $(SRC_DIR)/ECCoordinator.cc $(O_FILES)
	$(CC) $(CFLAGS) -o $@ $^ $(CLIBS)

ECHelper : $(SRC_DIR)/ECHelper.cc $(O_FILES)
	$(CC) $(CFLAGS) -o $@ $^ $(CLIBS)

ECPipeClient : $(SRC_DIR)/ECPipeClient.cc $(O_FILES)
	$(CC) $(CFLAGS) -o $@ $^ $(CLIBS)

$(OBJ_DIR)/CC_%.o : $(SRC_DIR)/%.cc $(SRC_DIR)/%.hh
	$(CC) $(CFLAGS) -o $@ -c $<

$(OBJ_DIR)/UTIL_%.o : $(UTIL_SRC_DIR)/%.cpp $(UTIL_SRC_DIR)/%.h
	$(CC) $(CFLAGS) -o $@ -c $<

$(OBJ_DIR)/J_%.o : $(UTIL_SRC_DIR)/%.c $(UTIL_SRC_DIR)/%.h
	$(JCC) $(CFLAGS) -o $@ -c $<

%Test : src/test/%.cc
	$(CC) $(CFLAGS) -o $@ $< $(CLIBS)

clean :
	rm -f ECCoordinator ECHelper ECPipeClient obj/CC_*
