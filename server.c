#include "server.h"
#include "table.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
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

// Accepted connections waiting for a free worker. When it is full, new
// clients are turned away instead of piling up without bound.
#define QUEUE_CAPACITY 128

struct worker {
    pthread_t thread;
    struct server *srv;
    int fd; // client being served, or -1 when idle
};

struct server {
    struct table *table;
    int listen_fd;
    int wake_pipe[2]; // server_stop() writes here to wake the accept thread
    pthread_t acceptor;
    bool acceptor_started;

    // lock guards the queue, stopping, and every worker's fd.
    pthread_mutex_t lock;
    pthread_cond_t has_work;   // signaled when an fd is queued or on stop
    int queue[QUEUE_CAPACITY]; // ring buffer of accepted fds
    size_t head;               // index of the oldest queued fd
    size_t count;
    bool stopping;

    int nworkers;            // number of worker threads actually started
    struct worker workers[]; // flexible array member, sized at startup
};

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

// ---- Worker pool ----------------------------------------------------------

// Called by the accept thread. Returns false if the queue is full.
static bool enqueue(struct server *s, int fd)
{
    pthread_mutex_lock(&s->lock);
    bool ok = s->count < QUEUE_CAPACITY;
    if (ok) {
        s->queue[(s->head + s->count) % QUEUE_CAPACITY] = fd;
        s->count++;
        pthread_cond_signal(&s->has_work);
    }
    pthread_mutex_unlock(&s->lock);
    return ok;
}

// Blocks until a connection is queued, then assigns it to w. Returns -1 once
// the server is stopping. Taking the fd off the queue and recording it in
// w->fd under one lock means shutdown can never miss a connection that is
// in between the two.
static int dequeue(struct server *s, struct worker *w)
{
    pthread_mutex_lock(&s->lock);
    // A loop, not an if: waits can wake spuriously, and another worker may
    // have taken the fd first.
    while (s->count == 0 && !s->stopping)
        pthread_cond_wait(&s->has_work, &s->lock);

    int fd = -1;
    if (!s->stopping) {
        fd = s->queue[s->head];
        s->head = (s->head + 1) % QUEUE_CAPACITY;
        s->count--;
    }
    w->fd = fd;
    pthread_mutex_unlock(&s->lock);
    return fd;
}

// Clears w->fd before closing, so shutdown never calls shutdown() on an fd
// number that was already closed and possibly reused by a new connection.
static void release(struct server *s, struct worker *w, int fd)
{
    pthread_mutex_lock(&s->lock);
    w->fd = -1;
    pthread_mutex_unlock(&s->lock);
    close(fd);
}

// A worker owns one connection at a time, for as long as it stays open.
static void *worker_main(void *arg)
{
    struct worker *w = arg;
    int fd;
    while ((fd = dequeue(w->srv, w)) >= 0) {
        handle_client(fd, w->srv->table);
        release(w->srv, w, fd);
    }
    return NULL;
}

// Idle workers are waiting on has_work; busy ones are blocked in read() or
// write() on their client. Both kinds need a nudge before they can exit.
static void wake_workers(struct server *s)
{
    pthread_mutex_lock(&s->lock);
    s->stopping = true;
    pthread_cond_broadcast(&s->has_work);
    for (int i = 0; i < s->nworkers; i++) {
        // Makes the blocked call return at once, as if the client hung up.
        if (s->workers[i].fd >= 0)
            shutdown(s->workers[i].fd, SHUT_RDWR);
    }
    pthread_mutex_unlock(&s->lock);
}

// ---- Accept thread --------------------------------------------------------

static void accept_one(struct server *s)
{
    int fd = accept(s->listen_fd, NULL, NULL);
    if (fd < 0) {
        // Failures here (client gave up, out of fds) affect one
        // connection, not the server, so log and keep accepting.
        if (errno != EINTR)
            perror("accept");
        return;
    }
    if (!enqueue(s, fd)) {
        send_reply(fd, "ERR server busy");
        close(fd);
    }
}

// The accept thread never touches client sockets beyond accept(); it just
// feeds the queue, so a slow client can't stop new ones from connecting.
static void *accept_main(void *arg)
{
    struct server *s = arg;
    // Polling the pipe alongside the listening socket is what lets
    // server_stop() interrupt a wait for the next client.
    struct pollfd fds[2] = {
        {.fd = s->listen_fd, .events = POLLIN},
        {.fd = s->wake_pipe[0], .events = POLLIN},
    };
    for (;;) {
        if (poll(fds, 2, -1) < 0) {
            if (errno == EINTR)
                continue;
            perror("poll");
            return NULL;
        }
        if (fds[1].revents != 0)
            return NULL; // server_stop() was called
        if (fds[0].revents != 0)
            accept_one(s);
    }
}

// ---- Lifecycle ------------------------------------------------------------

static bool open_wake_pipe(struct server *s)
{
    int p[2];
    if (pipe(p) < 0) {
        perror("pipe");
        return false;
    }
    s->wake_pipe[0] = p[0];
    s->wake_pipe[1] = p[1];
    return true;
}

static bool spawn(pthread_t *thread, void *(*fn)(void *), void *arg)
{
    int err = pthread_create(thread, NULL, fn, arg);
    if (err != 0)
        fprintf(stderr, "pthread_create: %s\n", strerror(err));
    return err == 0;
}

// Workers start before the accept thread so a connection is never queued
// with nobody to serve it.
static bool start_threads(struct server *s, int nworkers)
{
    for (int i = 0; i < nworkers; i++) {
        struct worker *w = &s->workers[i];
        w->srv = s;
        w->fd = -1;
        if (!spawn(&w->thread, worker_main, w))
            return false;
        s->nworkers++; // count only real threads, so stop joins exactly these
    }
    if (!spawn(&s->acceptor, accept_main, s))
        return false;
    s->acceptor_started = true;
    return true;
}

struct server *server_start(uint16_t port, int nworkers, struct table *t)
{
    struct server *s =
        calloc(1, sizeof *s + (size_t)nworkers * sizeof s->workers[0]);
    if (s == NULL)
        return NULL;
    s->table = t;
    s->listen_fd = -1;
    s->wake_pipe[0] = s->wake_pipe[1] = -1;
    pthread_mutex_init(&s->lock, NULL);
    pthread_cond_init(&s->has_work, NULL);

    // On any failure, server_stop() unwinds whatever was set up so far.
    s->listen_fd = open_listener(port);
    if (s->listen_fd < 0 || !open_wake_pipe(s) || !start_threads(s, nworkers)) {
        server_stop(s);
        return NULL;
    }
    fprintf(stderr, "kvstore listening on 127.0.0.1:%u with %d workers\n",
            (unsigned)port, nworkers);
    return s;
}

void server_stop(struct server *s)
{
    // Stop accepting first, so nothing new is queued during teardown.
    if (s->acceptor_started) {
        if (write(s->wake_pipe[1], "x", 1) < 0)
            perror("write");
        pthread_join(s->acceptor, NULL);
    }

    wake_workers(s);
    for (int i = 0; i < s->nworkers; i++)
        pthread_join(s->workers[i].thread, NULL);

    // Only this thread is left, so the queue is safe to read without the
    // lock. These connections were accepted but never reached a worker.
    for (size_t i = 0; i < s->count; i++)
        close(s->queue[(s->head + i) % QUEUE_CAPACITY]);

    if (s->listen_fd >= 0)
        close(s->listen_fd);
    if (s->wake_pipe[0] >= 0) {
        close(s->wake_pipe[0]);
        close(s->wake_pipe[1]);
    }
    pthread_cond_destroy(&s->has_work);
    pthread_mutex_destroy(&s->lock);
    free(s);
}
