#!/usr/bin/env python3
"""Integration tests for kvstore.

    python3 test.py                             # tests ./kvstore
    python3 test.py leaks --atExit -- ./kvstore # any wrapper command works

Starts the server on a free port with a small worker pool, runs protocol,
concurrency, and disconnect tests against it, then sends SIGTERM while
clients are still connected and checks that the server exits with status 0.
Leak checkers report leaks through that exit status, so a leak fails it.
"""
import os
import select
import signal
import socket
import struct
import subprocess
import sys
import threading
import time

WORKERS = 4
QUEUE_CAPACITY = 128  # must match QUEUE_CAPACITY in server.c
PORT = None
failures = 0


class Client:
    def __init__(self):
        self.sock = socket.create_connection(("127.0.0.1", PORT), timeout=30)
        self.reader = self.sock.makefile("rb")

    def send(self, data):
        self.sock.sendall(data)

    def readline(self):
        """Next reply line, or None once the server has closed the connection."""
        try:
            line = self.reader.readline()
        except ConnectionResetError:
            return None
        return line.rstrip(b"\n").decode() if line else None

    def cmd(self, line):
        self.send(line.encode() + b"\n")
        return self.readline()

    def has_reply(self, wait):
        return bool(select.select([self.sock], [], [], wait)[0])

    def close(self):
        self.reader.close()
        self.sock.close()

    def reset(self):
        """Closes with a TCP RST, the way a crashed client would."""
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                             struct.pack("ii", 1, 0))
        self.close()


def check(name, got, want):
    global failures
    if got == want:
        print(f"  ok    {name}")
    else:
        failures += 1
        print(f"  FAIL  {name}\n        got  {got!r}\n        want {want!r}")


def run_threads(n, fn):
    """Runs fn(0..n-1) on n threads at once and returns their results."""
    results = [None] * n

    def run(i):
        try:
            results[i] = fn(i)
        except Exception as e:  # report instead of dying silently in a thread
            results[i] = repr(e)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def test_protocol():
    c = Client()
    check("GET missing key", c.cmd("GET a"), "NOT_FOUND")
    check("SET", c.cmd("SET a 1"), "OK")
    check("GET", c.cmd("GET a"), "1")
    check("SET overwrites", c.cmd("SET a 22"), "OK")
    check("GET sees overwrite", c.cmd("GET a"), "22")
    check("commands are case-insensitive", c.cmd("get a"), "22")
    check("DEL", c.cmd("DEL a"), "OK")
    check("DEL missing key", c.cmd("DEL a"), "NOT_FOUND")
    check("GET after DEL", c.cmd("GET a"), "NOT_FOUND")
    check("empty line", c.cmd(""), "ERR empty command")
    check("unknown command", c.cmd("FOO x"), "ERR unknown command")
    check("SET without value", c.cmd("SET a"), "ERR usage: SET <key> <value>")
    check("GET without key", c.cmd("GET"), "ERR usage: GET <key>")
    check("too many arguments", c.cmd("SET a b c"), "ERR too many arguments")
    check("256-byte key allowed", c.cmd("SET " + "k" * 256 + " v"), "OK")
    check("257-byte key rejected", c.cmd("SET " + "k" * 257 + " v"),
          "ERR key too long")
    check("256-byte value allowed", c.cmd("SET big " + "v" * 256), "OK")
    check("256-byte value returned intact", c.cmd("GET big"), "v" * 256)
    check("257-byte value rejected", c.cmd("SET big " + "v" * 257),
          "ERR value too long")
    check("extra spaces are ignored", c.cmd("SET   d    4  "), "OK")
    c.close()


def test_framing():
    """TCP is a byte stream: commands can arrive split up or batched."""
    c = Client()
    c.send(b"SET p1 a\nSET p2 b\nGET p1\nGET p2\n")
    check("several commands in one packet",
          [c.readline() for _ in range(4)], ["OK", "OK", "a", "b"])
    for byte in b"SET slow value\n":
        c.send(bytes([byte]))
        time.sleep(0.001)
    check("one command split into 1-byte packets", c.readline(), "OK")
    check("CRLF line endings", c.cmd("GET slow\r"), "value")
    c.close()

    c = Client()
    c.send(b"x" * 2000)
    check("line over 1 KiB is rejected", c.readline(), "ERR line too long")
    check("...and the connection is closed", c.readline(), None)
    c.close()


def test_resize():
    n = 10000
    c = Client()
    c.send(b"".join(b"SET key%d val%d\n" % (i, i) for i in range(n)))
    check(f"{n} SETs (many table resizes)",
          all(c.readline() == "OK" for _ in range(n)), True)
    c.send(b"".join(b"GET key%d\n" % i for i in range(n)))
    check(f"all {n} keys survive resizing",
          all(c.readline() == f"val{i}" for i in range(n)), True)
    c.close()


def test_concurrency():
    def own_keys(i):
        c = Client()
        for j in range(200):
            key, val = f"t{i}-k{j}", f"v{i}-{j}"
            if c.cmd(f"SET {key} {val}") != "OK" or c.cmd(f"GET {key}") != val:
                return f"client {i} saw a wrong value for {key}"
        c.close()
        return "ok"

    n = 4 * WORKERS  # more clients than workers, so some wait in the queue
    check(f"{n} concurrent clients each read back their own writes",
          run_threads(n, own_keys), ["ok"] * n)

    values = [ch * 256 for ch in "abcd"]

    def same_key(i):
        c = Client()
        for _ in range(300):
            c.cmd(f"SET shared {values[i]}")
            got = c.cmd("GET shared")
            if got not in values:
                return f"torn value: {got!r}"
        c.close()
        return "ok"

    check("clients racing on one key never see a torn value",
          run_threads(WORKERS, same_key), ["ok"] * WORKERS)


def test_worker_pool():
    held = [Client() for _ in range(WORKERS)]
    check(f"{WORKERS} clients each get a worker",
          [c.cmd("GET nothing") for c in held], ["NOT_FOUND"] * WORKERS)

    waiting = Client()
    waiting.send(b"GET nothing\n")
    check("a client beyond the pool waits in the queue",
          waiting.has_reply(0.5), False)
    held.pop().close()
    check("...and is served once a worker frees up",
          waiting.readline(), "NOT_FOUND")
    held.append(waiting)

    queued = [Client() for _ in range(QUEUE_CAPACITY)]
    extra = Client()
    check("a client beyond the full queue is turned away",
          extra.readline(), "ERR server busy")
    for c in held + queued + [extra]:
        c.close()


def test_disconnects():
    for _ in range(50):
        c = Client()
        c.send(b"SET half")  # vanish in the middle of a command
        c.reset()
    for _ in range(50):
        c = Client()
        c.send(b"GET key1\n")  # vanish before reading the reply
        c.reset()
    c = Client()
    check("server still healthy after 100 abrupt disconnects",
          c.cmd("GET key1"), "val1")
    c.close()


def test_shutdown(server):
    idle = Client()
    check("idle client connected", idle.cmd("GET x"), "NOT_FOUND")
    partial = Client()
    partial.send(b"SET half")

    os.kill(server_pid(server), signal.SIGTERM)
    try:
        status = server.wait(timeout=60)
    except subprocess.TimeoutExpired:
        server.kill()
        status = "still running after 60s"
    check("SIGTERM with clients connected exits with status 0", status, 0)
    check("connected clients are disconnected", idle.readline(), None)
    idle.close()
    partial.close()


def server_pid(proc):
    """The pid to signal. `leaks --atExit` runs the server as a child process
    and dies without reporting if signaled itself; valgrind runs it in-process."""
    children = subprocess.run(["pgrep", "-P", str(proc.pid)],
                              capture_output=True, text=True).stdout.split()
    return int(children[0]) if children else proc.pid


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_server(cmd):
    server = subprocess.Popen(cmd + ["-p", str(PORT), "-t", str(WORKERS)])
    deadline = time.time() + 30  # valgrind can be slow to start
    while time.time() < deadline:
        if server.poll() is not None:
            sys.exit(f"server exited early with status {server.returncode}")
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=1).close()
            return server
        except OSError:
            time.sleep(0.1)
    server.kill()
    sys.exit("server never started listening")


def main():
    global PORT
    PORT = free_port()
    server = start_server(sys.argv[1:] or ["./kvstore"])
    try:
        for test in [test_protocol, test_framing, test_resize,
                     test_concurrency, test_worker_pool, test_disconnects]:
            print(test.__name__)
            test()
        print("test_shutdown")
        test_shutdown(server)
    finally:
        if server.poll() is None:
            server.kill()
    print("PASSED" if failures == 0 else f"FAILED: {failures} check(s)")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
