CFLAGS   := -std=c11 -Wall -Wextra -Werror -g -O2 -D_POSIX_C_SOURCE=200809L -pthread
LDFLAGS  := -pthread
SANITIZE := -fsanitize=address,undefined -fno-omit-frame-pointer

SRCS := main.c server.c table.c
OBJS := $(SRCS:.c=.o)

all: kvstore

kvstore: $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

main.o:   main.c server.h table.h
server.o: server.c server.h table.h
table.o:  table.c table.h

# Builds a separate binary straight from the sources, so sanitizer-built
# objects never get mixed into the normal build.
debug:
	$(CC) $(CFLAGS) -O0 $(SANITIZE) -o kvstore-debug $(SRCS) $(LDFLAGS)

# ThreadSanitizer finds data races. It can't be combined with ASan, so it
# gets its own binary too.
tsan:
	$(CC) $(CFLAGS) -O1 -fsanitize=thread -o kvstore-tsan $(SRCS) $(LDFLAGS)

test: kvstore
	python3 test.py ./kvstore

# macOS only. Runs the tests on the normal build (leaks and ASan don't mix);
# the test fails if leaks finds anything at exit.
leaks: kvstore
	python3 test.py leaks --atExit -- ./kvstore

clean:
	rm -rf kvstore kvstore-debug kvstore-tsan *.dSYM $(OBJS)

.PHONY: all debug tsan test leaks clean
