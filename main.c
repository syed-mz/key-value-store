#include "server.h"
#include "table.h"

#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#define DEFAULT_PORT 6380

// strtol instead of atoi so junk like "80abc" or "99999" is rejected rather
// than silently turned into some other port.
static bool parse_port(const char *s, uint16_t *out)
{
    char *end;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v < 1 || v > 65535)
        return false;
    *out = (uint16_t)v;
    return true;
}

int main(int argc, char **argv)
{
    uint16_t port = DEFAULT_PORT;
    if (argc > 2 || (argc == 2 && !parse_port(argv[1], &port))) {
        fprintf(stderr, "usage: %s [port]\n", argv[0]);
        return 1;
    }

    // Writing to a client that already disconnected raises SIGPIPE, which
    // kills the process by default. Ignoring it makes write() fail with
    // EPIPE instead, so one bad client only ends its own connection.
    signal(SIGPIPE, SIG_IGN);

    struct table *t = table_create();
    if (t == NULL) {
        fprintf(stderr, "out of memory\n");
        return 1;
    }
    int rc = server_run(port, t);
    table_destroy(t);
    return rc == 0 ? 0 : 1;
}
