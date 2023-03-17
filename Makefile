CC = gcc
CFLAGS = -std=gnu11 -D_GNU_SOURCE -Wall -Wextra -O2 -g 
LDFLAGS = -libverbs -lrdmacm -pthread
OBJS = $(patsubst %.c,%.o, $(wildcard *.c))

all: bw

bw: $(OBJS)
	@printf  "\e[33mlink %s\e[0m\n" $@
	@$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	@printf "\e[32mbuild %s\e[0m\n" $^
	@$(CC) $(CFLAGS) -c $^

slow: CFLAGS += -DMULTI_SGE
slow: all
	@printf "\e[31mslow version\e[0m\n"

.PHONY: clean
clean:
	@rm -f *.a *.o *.gch bw a.out core.*

