# kvstore

A minimal, multithreaded, in-memory key-value store in C, in the spirit of
Redis. It speaks a plain-text protocol over TCP and is built from scratch
using only C11 and POSIX (sockets and pthreads).

## Build

```sh
make          # ./kvstore        (-O2, warnings are errors)
make debug    # ./kvstore-debug  (AddressSanitizer + UndefinedBehaviorSanitizer)
make tsan     # ./kvstore-tsan   (ThreadSanitizer: finds data races)
make clean
```

## Run

```sh
./kvstore                  # 127.0.0.1:6380, 16 worker threads
./kvstore -p 7000 -t 64    # custom port and worker count
```

Ctrl-C (SIGINT) or SIGTERM shuts the server down cleanly: every client is
disconnected, every thread is joined, and all memory is freed.

## Try it

```sh
$ nc localhost 6380
SET name alice
OK
GET name
alice
DEL name
OK
GET name
NOT_FOUND
```

## Test

```sh
make test                           # integration tests against ./kvstore
make debug && python3 test.py ./kvstore-debug
make tsan  && python3 test.py ./kvstore-tsan
make leaks                          # macOS: fails if `leaks` finds anything at exit
```

`test.py` starts the server itself (so it can run under a wrapper such as
`leaks` or `valgrind`) and covers the protocol, split and batched TCP reads,
table resizing, concurrent clients, queueing when all workers are busy,
abrupt disconnects, and a SIGTERM with clients still connected.

## Protocol

One command per line, terminated by `\n` (`\r\n` also accepted). Every line
gets exactly one reply line.

| Command             | Reply                       |
|---------------------|-----------------------------|
| `SET <key> <value>` | `OK`                        |
| `GET <key>`         | the value, or `NOT_FOUND`   |
| `DEL <key>`         | `OK`, or `NOT_FOUND`        |
| anything malformed  | `ERR <reason>`              |

Keys and values cannot contain spaces and are capped at 256 bytes each.
Command names are case-insensitive. A line longer than 1 KiB gets
`ERR line too long` and the connection is closed, as Redis does for oversized
inline requests. If the server is saturated, a new connection gets
`ERR server busy` and is closed.

## Design

### Hash table (`table.c`)

- **Separate chaining.** Each bucket is a singly linked list of entries.
  Deletes are simple: unlink one node, with no tombstones as open addressing
  would need.
- **Resize at load factor 0.75.** When `count / capacity > 0.75` the bucket
  array doubles and every entry is relinked into its new bucket. Chains stay
  short, so operations are O(1) on average. One resize costs O(n), but it
  happens only after n inserts, so inserts are amortized O(1).
- **Power-of-two capacity**, so the bucket index is `hash & (capacity - 1)`
  instead of a slower `%`.
- **FNV-1a hash.** It is short, fast, and spreads similar keys well.
- **`table_get` copies the value out** while the lock is held, instead of
  returning a pointer. Another thread's `SET` or `DEL` could free that memory
  while the reply is still being written.

### Threads (`server.c`)

```
                      +---------------------+
  accept thread ----> | queue of client fds | ----> N worker threads
  (accept, enqueue)   |  (bounded, 128)     |       (serve one client each)
                      +---------------------+
  main thread: waits for SIGINT/SIGTERM, then shuts everything down
```

- **Fixed worker pool.** N threads (`-t`, default 16) are created at startup.
  Each takes a connection off the queue and serves it until the client
  disconnects. A fixed pool bounds memory and thread count, and avoids
  creating a thread per connection.
- **Bounded queue.** The accept thread pushes each new connection onto a
  128-slot ring buffer, guarded by a mutex and a condition variable that idle
  workers sleep on. When the queue is full, new clients get `ERR server busy`
  instead of piling up without limit.
- **Line buffering.** TCP is a byte stream: one `read()` may return half a
  command or several. Bytes accumulate in a per-connection buffer, and only
  complete lines are executed.
- **Disconnects.** `read()` returning 0 or an error ends that client's
  session. The worker closes the socket and goes back to the queue.
  `SIGPIPE` is ignored, so writing to a client that has vanished returns an
  error instead of killing the process.
- **Binds to 127.0.0.1 only.** There is no authentication.

### Locking

The whole table is protected by **one `pthread_mutex_t`**. Every public
table function takes the lock for its entire body.

**Why it's correct.** Every read and write of table memory happens while
holding the same lock, so operations never overlap. Each operation takes
effect atomically, and no thread can see a half-updated chain or a resize in
progress. There is only one table lock, and it is never held together with
the server's queue lock, so a lock-ordering deadlock can't happen. The test
suite passes under ThreadSanitizer. With the table locks removed, the same
suite makes ThreadSanitizer report data races in `table_set`.

**Its bottleneck.** Only one thread can be inside the table at a time, so
table work doesn't scale with cores. Under load, workers queue on the mutex,
and its cache line bounces between CPU cores. A resize holds the lock for
O(n), which stalls every client at once. Two common fixes:

- *Lock striping:* one lock per group of buckets.
- *Reader-writer lock:* GETs run in parallel. This stops helping once LRU
  arrives (Stage 3), because every GET then updates recency and becomes a
  write.

In this server each request also costs a `read()` and a `write()` system
call, both made outside the lock. That work runs in parallel, which softens
the bottleneck; the benchmark measures how much.

### Shutdown

A multithreaded server can't just `exit()` if leak checkers are to see clean
memory. It has to stop and join every thread first.

1. `main` blocks SIGINT and SIGTERM *before* creating any thread (threads
   inherit the mask) and then waits in `sigwait()`. Signals are received as
   ordinary code, not inside an async signal handler where locks, `malloc`,
   and `printf` are unsafe.
2. The accept thread `poll()`s the listening socket *and* a pipe. Shutdown
   writes one byte to the pipe, which wakes it, and it exits.
3. `stopping` is set and the condition variable is broadcast, so idle
   workers wake up and exit. Busy workers are blocked in `read()` on a
   client. Calling `shutdown()` on that socket makes the `read()` return 0,
   as if the client had hung up. Each worker's current fd is recorded under
   the queue lock, so shutdown can't miss a connection that is between the
   queue and a worker.
4. All threads are joined, leftover queued connections are closed, and the
   server and table are freed.

## Limitations

- **A worker is tied to one connection for as long as it's open.** At most
  N clients are served at once, and others wait in the queue even if the
  connected clients are idle. N idle clients occupy the whole pool. Redis
  avoids this with a single-threaded event loop (`epoll`/`kqueue`) that
  multiplexes thousands of connections.
- **Clients that stop reading.** A client that sends commands but never
  reads replies eventually blocks its worker in `write()`.
- **The table never shrinks** after deletes.
- **A resize rehashes everything at once,** so one insert can take O(n)
  while holding the lock. Redis rehashes incrementally to avoid this.
