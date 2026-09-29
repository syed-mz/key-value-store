# kvstore

A minimal in-memory key-value store in C, in the spirit of Redis. It speaks a
plain-text protocol over TCP and is built from scratch using only C11 and POSIX.

## Build

```sh
make          # ./kvstore        (-O2, warnings are errors)
make debug    # ./kvstore-debug  (AddressSanitizer + UndefinedBehaviorSanitizer)
make clean
```

## Run

```sh
./kvstore            # listens on 127.0.0.1:6380
./kvstore 7000       # custom port
```

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
inline requests.

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
- **`table_get` copies the value out** instead of returning a pointer into
  the table, so a caller never holds memory that a later `SET` or `DEL` frees.

### Server (`server.c`)

- **Line buffering.** TCP is a byte stream: one `read()` may return half a
  command or several. Bytes accumulate in a per-connection buffer, and only
  complete lines are executed.
- **Parsing is separate from I/O.** `execute()` maps a line to reply text and
  never touches the socket.
- **`SIGPIPE` is ignored.** A client that disconnects before reading its reply
  would otherwise kill the whole server.
- **Binds to 127.0.0.1 only.** There is no authentication.

## Limitations

- Serves one client at a time (Stage 1).
- The table never shrinks after deletes.
- A resize rehashes everything at once, so one insert can take O(n).
