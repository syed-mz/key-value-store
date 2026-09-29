#include "server.h"
#include "table.h"

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define DEFAULT_PORT 6380
#define DEFAULT_WORKERS 16
#define MAX_WORKERS 1024

static void usage(const char *prog)
{
    fprintf(stderr, "usage: %s [-p port] [-t worker_threads]\n", prog);
}

// strtol instead of atoi so junk like "80abc" or "99999" is rejected rather
// than silently turned into some other number.
static bool parse_long(const char *s, long min, long max, long *out)
{
    char *end;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v < min || v > max)
        return false;
    *out = v;
    return true;
}

static bool parse_args(int argc, char **argv, long *port, long *workers)
{
    int opt;
    while ((opt = getopt(argc, argv, "p:t:")) != -1) {
        switch (opt) {
        case 'p':
            if (!parse_long(optarg, 1, 65535, port))
                return false;
            break;
        case 't':
            if (!parse_long(optarg, 1, MAX_WORKERS, workers))
                return false;
            break;
        default:
            return false;
        }
    }
    return optind == argc; // no stray positional arguments
}

int main(int argc, char **argv)
{
    long port = DEFAULT_PORT;
    long workers = DEFAULT_WORKERS;
    if (!parse_args(argc, argv, &port, &workers)) {
        usage(argv[0]);
        return 1;
    }

    // Writing to a client that already disconnected raises SIGPIPE, which
    // kills the process by default. Ignoring it makes write() fail with
    // EPIPE instead, so one bad client only ends its own connection.
    signal(SIGPIPE, SIG_IGN);

    // Block SIGINT/SIGTERM before any thread exists. Threads inherit the
    // mask, so these signals never interrupt anyone; they wait until main
    // collects them with sigwait() below. Shutdown then runs as ordinary
    // code, instead of in a signal handler where locks, malloc, and printf
    // are all unsafe.
    sigset_t stop_signals;
    sigemptyset(&stop_signals);
    sigaddset(&stop_signals, SIGINT);
    sigaddset(&stop_signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &stop_signals, NULL);

    struct table *t = table_create();
    if (t == NULL) {
        fprintf(stderr, "out of memory\n");
        return 1;
    }
    struct server *s = server_start((uint16_t)port, (int)workers, t);
    if (s == NULL) {
        table_destroy(t);
        return 1;
    }

    int sig;
    sigwait(&stop_signals, &sig);
    fprintf(stderr, "%s received, shutting down\n",
            sig == SIGINT ? "SIGINT" : "SIGTERM");

    server_stop(s);
    table_destroy(t);
    return 0;
}
