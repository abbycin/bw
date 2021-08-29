CC = gcc
CFLAGS = -std=gnu99 -Wall -Wextra -O2 -g
OBJS = $(patsubst %.c,%.o, $(wildcard *.c))

all: bw

bw: $(OBJS)
	@printf  "\e[33mlink %s\e[0m\n" $@
	@$(CC) $(CFLAGS) -o $@ $^ -libverbs

%.o: %.c
	@printf "\e[32mbuild %s\e[0m\n" $^
	@$(CC) $(CFLAGS) -c $^

.PHONY: clean
clean:
	@rm -f *.a *.o *.gch bw a.out
