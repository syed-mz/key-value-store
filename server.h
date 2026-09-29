#ifndef SERVER_H
#define SERVER_H

#include <stdint.h>

struct table;

// Listens on 127.0.0.1:port and serves clients one at a time, forever.
// Returns -1 only if the listening socket cannot be set up.
int server_run(uint16_t port, struct table *t);

#endif
