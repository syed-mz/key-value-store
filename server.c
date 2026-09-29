#include "server.h"
#include "table.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

#define MAX_KEY_LEN 256
#define MAX_VALUE_LEN 256
#define MAX_ARGS 3 // SET <key> <value>

// The longest valid line is "SET " + key + " " + value + "\r\n" = 519 bytes.
// A fixed 1 KiB buffer leaves slack while bounding memory per client.
#define LINE_BUF_SIZE 1024

// Splits line on spaces, in place. Returns the token count, or max + 1 if
// there are more than max tokens, so argv never overflows.
static int split(char *line, char *argv[], int max)
{
    int argc = 0;
    char *save = NULL;
    for (char *tok = strtok_r(line, " ", &save); tok != NULL;
         tok = strtok_r(NULL, " ", &save)) {
        if (argc == max)
            return max + 1;
        argv[argc++] = tok;
    }
    return argc;
}

// Turns one command line into its reply text (without the newline). GET
// copies the value into value_buf and returns it; every other reply is a
// string literal. Keeping this free of I/O makes the protocol easy to follow.
static const char *execute(struct table *t, char *line, char *value_buf,
                           size_t value_buf_size)
{
    char *argv[MAX_ARGS];
    int argc = split(line, argv, MAX_ARGS);

    if (argc == 0)
        return "ERR empty command";
    if (argc > MAX_ARGS)
        return "ERR too many arguments";
    if (argc > 1 && strlen(argv[1]) > MAX_KEY_LEN)
        return "ERR key too long";
    if (argc > 2 && strlen(argv[2]) > MAX_VALUE_LEN)
        return "ERR value too long";

    // Case-insensitive, like Redis, so "get foo" typed into nc still works.
    if (strcasecmp(argv[0], "SET") == 0) {
        if (argc != 3)
            return "ERR usage: SET <key> <value>";
        return table_set(t, argv[1], argv[2]) ? "OK" : "ERR out of memory";
    }
    if (strcasecmp(argv[0], "GET") == 0) {
        if (argc != 2)
            return "ERR usage: GET <key>";
        return table_get(t, argv[1], value_buf, value_buf_size) ? value_buf
                                                                  : "NOT_FOUND";
    }
    if (strcasecmp(argv[0], "DEL") == 0) {
        if (argc != 2)
            return "ERR usage: DEL <key>";
        return table_del(t, argv[1]) ? "OK" : "NOT_FOUND";
    }
    return "ERR unknown command";
}

// write() may send fewer bytes than asked (e.g. when the socket's send
// buffer is full), so keep going until everything is out.
static bool write_all(int fd, const char *p, size_t len)
{
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return false;
        }
        p += n;
        len -= (size_t)n;
    }
    return true;
}

static bool send_reply(int fd, const char *msg)
{
    // Build the whole line first so each reply is one write(), not two.
    char out[MAX_VALUE_LEN + 64];
    int len = snprintf(out, sizeof out, "%s\n", msg);
    return write_all(fd, out, (size_t)len);
}

// Runs every complete line in buf and slides the unfinished tail (if any)
// to the front. Returns false if the client can no longer be written to.
static bool serve_lines(int fd, struct table *t, char *buf, size_t *used)
{
    char *start = buf;
    char *end = buf + *used;
    char *nl;

    while ((nl = memchr(start, '\n', (size_t)(end - start))) != NULL) {
        *nl = '\0';
        if (nl > start && nl[-1] == '\r') // tolerate telnet-style CRLF
            nl[-1] = '\0';

        char value[MAX_VALUE_LEN + 1];
        if (!send_reply(fd, execute(t, start, value, sizeof value)))
            return false;
        start = nl + 1;
    }

    *used = (size_t)(end - start);
    memmove(buf, start, *used);
    return true;
}

// TCP is a byte stream, not a message stream: one read() can return half a
// command, or several commands at once. So bytes accumulate in a buffer and
// only complete newline-terminated lines are executed.
static void handle_client(int fd, struct table *t)
{
    char buf[LINE_BUF_SIZE];
    size_t used = 0;

    for (;;) {
        ssize_t n = read(fd, buf + used, sizeof buf - used);
        if (n == 0)
            return; // client closed the connection
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return; // connection reset or similar
        }
        used += (size_t)n;

        if (!serve_lines(fd, t, buf, &used))
            return;

        // A full buffer with no newline can never become a valid command.
        if (used == sizeof buf) {
            send_reply(fd, "ERR line too long");
            return;
        }
    }
}

// Returns a socket listening on 127.0.0.1:port, or -1 on failure.
static int open_listener(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    // Without this, restarting the server right after stopping it fails with
    // "Address already in use" while old connections sit in TIME_WAIT.
    int yes = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) < 0)
        perror("setsockopt"); // not fatal

    // Loopback only: there is no authentication, so don't expose the store
    // to the rest of the network.
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
    };
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, SOMAXCONN) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }
    return fd;
}

int server_run(uint16_t port, struct table *t)
{
    int listen_fd = open_listener(port);
    if (listen_fd < 0)
        return -1;
    fprintf(stderr, "kvstore listening on 127.0.0.1:%u\n", (unsigned)port);

    for (;;) {
        int client_fd = accept(listen_fd, NULL, NULL);
        if (client_fd < 0) {
            // Failures here (client gave up, out of fds) affect one
            // connection, not the server, so log and keep accepting.
            if (errno != EINTR)
                perror("accept");
            continue;
        }
        handle_client(client_fd, t);
        close(client_fd);
    }
}
