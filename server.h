#ifndef SERVER_H
#define SERVER_H

#include <stdint.h>

struct table;
struct server;

// Starts serving 127.0.0.1:port on background threads: one accept thread
// plus nworkers worker threads, all sharing t. Returns NULL on failure.
struct server *server_start(uint16_t port, int nworkers, struct table *t);

// Stops accepting, disconnects every client, joins all threads, and frees
// the server. The table is left alone; it belongs to the caller.
void server_stop(struct server *s);

#endif
