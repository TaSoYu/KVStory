
CC = gcc
CFLAGS = -Wall -Wextra
LIBS =  -L ./NtyCo/ -lntyco -lpthread -ldl
INC = -I ./NtyCo/core

SRCS = kvstore.c rbtree.c hash.c
SUBDIR = NtyCo/

OBJS = $(SRCS:.c=.o)

TARGET = kvstore

all: $(SUBDIR) $(TARGET)
.PHONY : all

$(SUBDIR): ECHO
	make -C $@ 
ECHO:
	@echo $(SUBDIR)

$(TARGET): $(OBJS)
	$(CC) -o $@ $^ $(CFLAGS) $(INC) $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

clean:
	rm -rf $(OBJS) $(TARGET)

