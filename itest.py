#!/usr/bin/env python3
"""End to end tests: start the real server, talk to it over sockets.

Usage: python3 itest.py [test names]

The binaries are looked up in $HW1_DIR, or in the parent of this directory.
./hw1 is required. ./hw1_fast (built with -DMAX=300) is needed by the
starvation tests, which are skipped without it. ./hw1_dbg is optional.
On equal domination counts the smallest wait - budget starts first.
"""
import os
import random
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

ROOT = os.environ.get("HW1_DIR") or os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))
TERMINATED = "Active job is terminated by completion time"
# Killing agents at random can hit one that holds the shared lock, which
# would deadlock the others.
KILL_AGENTS = False


class Timeout(Exception):
    pass


class Skip(Exception):
    pass


class Client:
    def __init__(self, sock):
        self.s = sock
        self.buf = b""
        self.pid = int(self.line())

    def fileno(self):
        return self.s.fileno()

    def send(self, text):
        self.s.sendall(text.encode() + b"\n")

    def raw(self, data):
        self.s.sendall(data)

    def line(self, timeout=3.0):
        end = time.time() + timeout
        while b"\n" not in self.buf:
            left = end - time.time()
            if left <= 0:
                raise Timeout("no line within %.1fs" % timeout)
            r, _, _ = select.select([self.s], [], [], left)
            if not r:
                continue
            data = self.s.recv(65536)
            if not data:
                raise Timeout("connection closed by server")
            self.buf += data
        line, _, self.buf = self.buf.partition(b"\n")
        return line.decode()

    def expect(self, text, timeout=3.0):
        got = self.line(timeout)
        assert got == text, "expected %r, got %r" % (text, got)

    def silent(self, duration):
        try:
            got = self.line(duration)
        except Timeout:
            return
        raise AssertionError("expected silence, got %r" % got)

    def report(self):
        self.send("REPORT")
        lines = []
        while True:
            lines.append(self.line())
            if lines[-1].startswith("waiting list:"):
                return lines

    def close(self):
        self.s.close()


class Server:
    def __init__(self, config, binary="hw1", tcp=False):
        path = os.path.join(ROOT, binary)
        if not os.path.exists(path):
            if binary == "hw1_fast":
                raise Skip("needs hw1_fast")
            path = os.path.join(ROOT, "hw1")
        self.dir = tempfile.mkdtemp(prefix="hw1t")
        if tcp:
            probe = socket.socket()
            probe.bind(("127.0.0.1", 0))
            self.addr = ("127.0.0.1", probe.getsockname()[1])
            probe.close()
            arg = "%s:%d" % self.addr
        else:
            self.addr = os.path.join(self.dir, "s")
            arg = "@" + self.addr
        self.tcp = tcp
        self.proc = subprocess.Popen([path, arg], stdin=subprocess.PIPE)
        self.proc.stdin.write(config.encode())
        self.proc.stdin.close()
        self.connect().close()  # waits until it listens

    def connect(self):
        end = time.time() + 3
        while True:
            s = socket.socket(socket.AF_INET if self.tcp else socket.AF_UNIX)
            try:
                s.connect(self.addr)
                return Client(s)
            except OSError:
                s.close()
                if time.time() > end or self.proc.poll() is not None:
                    raise
                time.sleep(0.02)

    def children(self):
        """(pid, state) of the server's child processes"""
        out = subprocess.run(["ps", "-axo", "pid=,ppid=,stat="],
                             capture_output=True, text=True).stdout
        res = []
        for l in out.splitlines():
            f = l.split()
            if len(f) >= 3 and f[1] == str(self.proc.pid):
                res.append((int(f[0]), f[2]))
        return res

    def stop(self):
        kids = self.children()
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        for pid, _ in kids:  # whatever survived the master
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        shutil.rmtree(self.dir, ignore_errors=True)


def next_event(clients, timeout=3.0):
    """first line any of the clients receives: (client, line)"""
    end = time.time() + timeout
    while True:
        for c in clients:
            if b"\n" in c.buf:
                return c, c.line()
        left = end - time.time()
        if left <= 0:
            raise Timeout("no event within %.1fs" % timeout)
        r, _, _ = select.select(clients, [], [], left)
        for c in r:
            data = c.s.recv(65536)
            if not data:
                raise Timeout("connection closed by server")
            c.buf += data


def started(nodes, t):
    return "Job started at: <%s>, completion in %d" % (nodes, t)


def submit(c, duration, *req):
    c.send("SUBMIT %d %s" % (duration, " ".join(str(x) for x in req)))
    c.expect("OK")


def parse_report(r):
    head = r[0].split()
    out = {"nrun": int(head[2]), "nwait": int(head[5]),
           "nodes": [], "active": [], "waiting": []}
    i = 3
    while r[i]:
        f = r[i].replace("/", " ").split()
        out["nodes"].append((int(f[1]), int(f[2])))
        i += 1
    i += 4
    while r[i]:
        f = r[i].split()
        out["active"].append((int(f[0]), int(f[1]), int(f[2]), f[3:]))
        i += 1
    i += 4
    while r[i]:
        f = r[i].split()
        out["waiting"].append((int(f[0]), int(f[1]), [int(x) for x in f[2:]]))
        i += 1
    out["avg"] = float(r[i + 1].split(":")[1])
    return out


def start_order(clients, names, count, timeout=5.0):
    order = []
    while len(order) < count:
        who, line = next_event(clients, timeout)
        if line.startswith("Job started"):
            order.append(names[clients.index(who)])
    return order


ONE = "1 1\n2\n"  # one type, one node with 2 processors
SOLO = "1 1\n1\n"
QUAD = "1 1\n4\n"


def t_connect_unix(S):
    srv = S(ONE)
    a, b = srv.connect(), srv.connect()
    kids = [p for p, _ in srv.children()]
    assert a.pid in kids and b.pid in kids, (a.pid, b.pid, kids)
    assert a.pid != b.pid


def t_connect_tcp(S):
    srv = S(ONE, tcp=True)
    a = srv.connect()
    assert a.pid in [p for p, _ in srv.children()]
    a.send("SUBMIT 100 1")
    a.expect("OK")
    a.expect(started("0", 100))
    a.expect(TERMINATED)


def t_config_comments(S):
    srv = S("2 2  # types, nodes\n1 0   # node 0\n0 1   # node 1\n")
    a = srv.connect()
    a.send("SUBMIT 100 1 1")
    a.expect("OK")
    a.expect(started("0 1", 100))


def t_single_job(S):
    srv = S(ONE)
    a = srv.connect()
    a.send("SUBMIT 300 2")
    a.expect("OK")
    a.expect(started("0", 300))
    t0 = time.time()
    a.expect(TERMINATED)
    dt = (time.time() - t0) * 1000
    assert 250 <= dt <= 600, "terminated after %.0f ms" % dt
    # the connection can submit again afterwards
    a.send("SUBMIT 50 1")
    a.expect("OK")
    a.expect(started("0", 50))
    a.expect(TERMINATED)


def t_stop_starts_next(S):
    srv = S(ONE)
    a, b = srv.connect(), srv.connect()
    a.send("SUBMIT 5000 2")
    a.expect("OK")
    a.expect(started("0", 5000))
    b.send("SUBMIT 200 2")
    b.expect("OK")
    b.silent(0.3)
    a.send("STOP")
    a.expect("OK")
    b.expect(started("0", 200), timeout=0.5)
    b.expect(TERMINATED)
    a.silent(0.2)


def t_stop_waiting_job(S):
    srv = S(ONE)
    a, b = srv.connect(), srv.connect()
    a.send("SUBMIT 400 2")
    a.expect("OK")
    a.expect(started("0", 400))
    b.send("SUBMIT 200 2")
    b.expect("OK")
    b.send("STOP")
    b.expect("OK")
    a.expect(TERMINATED)
    b.silent(0.3)  # its job was deleted, must not start


def t_disconnect_starts_next(S):
    srv = S(ONE)
    a, b = srv.connect(), srv.connect()
    a.send("SUBMIT 5000 2")
    a.expect("OK")
    a.expect(started("0", 5000))
    b.send("SUBMIT 200 2")
    b.expect("OK")
    b.silent(0.2)
    a.close()
    b.expect(started("0", 200), timeout=0.5)


def t_quit_starts_next(S):
    srv = S(ONE)
    a, b = srv.connect(), srv.connect()
    a.send("SUBMIT 5000 2")
    a.expect("OK")
    a.expect(started("0", 5000))
    b.send("SUBMIT 200 2")
    b.expect("OK")
    a.send("QUIT")
    b.expect(started("0", 200), timeout=0.5)
    try:
        a.line(1.0)
        raise AssertionError("server did not close the connection on QUIT")
    except Timeout as e:
        assert "closed" in str(e), e


def t_timeout_starts_next(S):
    srv = S(ONE)
    a, b = srv.connect(), srv.connect()
    a.send("SUBMIT 300 2")
    a.expect("OK")
    a.expect(started("0", 300))
    b.send("SUBMIT 100 2")
    b.expect("OK")
    b.silent(0.15)
    a.expect(TERMINATED)
    b.expect(started("0", 100), timeout=0.5)


def t_errors(S):
    srv = S(ONE)
    a = srv.connect()
    for bad in ["HELLO", "SUBMIT", "SUBMIT 100",
                "SUBMIT 0 1", "SUBMIT 100 -1", "SUBMIT x 1",
                "SUBMIT 100 3",  # more than any node has
                "STOP"]:         # no job
        a.send(bad)
        got = a.line()
        assert got.startswith("ERROR"), (bad, got)
    a.send("SUBMIT 5000 1")
    a.expect("OK")
    a.expect(started("0", 5000))
    a.send("SUBMIT 100 1")  # one job per connection
    assert a.line().startswith("ERROR")
    a.send("STOP")
    a.expect("OK")
    a.send("SUBMIT 100 1")
    a.expect("OK")


def t_ok_before_started(S):
    srv = S(ONE)
    a = srv.connect()
    for _ in range(300):
        a.send("SUBMIT 60000 2")
        a.expect("OK")
        a.expect(started("0", 60000))
        a.send("STOP")
        a.expect("OK")


def t_no_stale_termination(S):
    srv = S(ONE)
    a = srv.connect()
    a.send("SUBMIT 200 2")
    a.expect("OK")
    a.expect(started("0", 200))
    a.send("STOP")
    a.expect("OK")
    a.send("SUBMIT 1000 2")
    a.expect("OK")
    a.expect(started("0", 1000))
    a.silent(0.6)  # the first job's deadline passes here
    a.expect(TERMINATED)


def t_partial_lines(S):
    srv = S(ONE)
    a = srv.connect()
    a.raw(b"SUB")
    time.sleep(0.1)
    a.raw(b"MIT 100 2\r\n\0")
    a.expect("OK")
    a.expect(started("0", 100))
    a.expect(TERMINATED)
    a.raw(b"SUBMIT 100 1\nSTOP\nSTOP\n")
    a.expect("OK")
    lines = [a.line(), a.line()]
    if lines[0].startswith("Job started"):
        lines = [lines[1], a.line()]
    assert lines[0] == "OK" and lines[1].startswith("ERROR"), lines


def t_multi_type(S):
    srv = S("4 2\n5 0 3 3\n5 0 3 3\n")
    a, b, c, d = [srv.connect() for _ in range(4)]
    a.send("SUBMIT 100 6 0 0 0")  # 5+5 can not be combined
    assert a.line().startswith("ERROR")
    a.send("SUBMIT 100 0 1 0 0")  # nobody has type 1
    assert a.line().startswith("ERROR")
    a.send("SUBMIT 5000 2 0 2 0")
    a.expect("OK")
    a.expect(started("0 - 0 -", 5000))
    b.send("SUBMIT 5000 0 0 3 1")
    b.expect("OK")
    b.expect(started("- - 1 0", 5000))
    c.send("SUBMIT 5000 4 0 0 0")
    c.expect("OK")
    c.expect(started("1 - - -", 5000))
    d.send("SUBMIT 5000 4 0 0 0")  # 3 left on node 0, 1 on node 1
    d.expect("OK")
    d.silent(0.2)
    a.send("STOP")
    a.expect("OK")
    d.expect(started("0 - - -", 5000))


def t_selection_order(S):
    srv = S(ONE)
    a, b, c, d = [srv.connect() for _ in range(4)]
    a.send("SUBMIT 500 2")
    a.expect("OK")
    a.expect(started("0", 500))
    b.send("SUBMIT 300 2")  # budget 600
    b.expect("OK")
    time.sleep(0.05)
    c.send("SUBMIT 100 1")  # budget 100
    c.expect("OK")
    time.sleep(0.05)
    d.send("SUBMIT 200 2")  # budget 400
    d.expect("OK")
    # A ends: c dominates d and starts; b and d need both processors.
    # c ends: nobody dominates, wait-budget is ~0 for b and ~100 for d.
    order = []
    names = {a: "a", b: "b", c: "c", d: "d"}
    while len(order) < 3:
        who, line = next_event([a, b, c, d])
        if line.startswith("Job started"):
            order.append(names[who])
    assert order == ["c", "b", "d"], order


def t_maxwait_blocks(S):
    srv = S(ONE, binary="hw1_fast")  # MAX = 300 ms
    a, b, c = [srv.connect() for _ in range(3)]
    a.send("SUBMIT 60000 1")
    a.expect("OK")
    a.expect(started("0", 60000))
    b.send("SUBMIT 60000 2")  # does not fit while a runs
    b.expect("OK")
    time.sleep(0.4)           # b is now starving
    c.send("SUBMIT 100 1")    # would fit, but b has priority
    c.expect("OK")
    c.silent(0.5)
    r = c.report()
    assert r[0] == "1 nodes, 1 jobs running, 2 jobs waiting", r[0]
    a.send("STOP")
    a.expect("OK")
    b.expect(started("0", 60000), timeout=0.5)
    c.silent(0.3)             # b took both processors
    b.send("STOP")
    b.expect("OK")
    c.expect(started("0", 100), timeout=0.5)


def t_maxwait_removed_blocker(S):
    srv = S(ONE, binary="hw1_fast")
    a, b, c = [srv.connect() for _ in range(3)]
    a.send("SUBMIT 60000 1")
    a.expect("OK")
    a.expect(started("0", 60000))
    b.send("SUBMIT 60000 2")
    b.expect("OK")
    time.sleep(0.4)
    c.send("SUBMIT 60000 1")
    c.expect("OK")
    c.silent(0.2)
    b.send("STOP")            # the starving job leaves the queue
    b.expect("OK")
    c.expect(started("0", 60000), timeout=0.5)


def t_no_block_before_maxwait(S):
    srv = S(ONE)  # MAX = 10000 ms: same scenario, nobody is starving
    a, b, c = [srv.connect() for _ in range(3)]
    a.send("SUBMIT 60000 1")
    a.expect("OK")
    a.expect(started("0", 60000))
    b.send("SUBMIT 60000 2")
    b.expect("OK")
    time.sleep(0.4)
    c.send("SUBMIT 100 1")
    c.expect("OK")
    c.expect(started("0", 100), timeout=0.5)


def t_report_empty(S):
    srv = S(ONE)
    a = srv.connect()
    assert a.report() == [
        "1 nodes, 0 jobs running, 0 jobs waiting",
        "Node  Assignment",
        "----------------",
        "   0      0/   2",
        "",
        "Active Jobs:",
        "Conn.     Running    Remaining  Assigned Nodes",
        "------------------------------------",
        "",
        "Waiting Jobs:",
        "Conn.     Waiting   Requested Nodes",
        "------------------------------------",
        "",
        "average wait time: 0.00",
        "waiting list:",
    ]


def t_report_jobs(S):
    srv = S("2 2\n4 2\n4 0\n")
    a, b, c, d = [srv.connect() for _ in range(4)]
    a.send("SUBMIT 5000 3 1")
    a.expect("OK")
    a.expect(started("0 0", 5000))
    b.send("SUBMIT 3000 3 0")
    b.expect("OK")
    b.expect(started("1 -", 3000))
    c.send("SUBMIT 100 4 0")  # 1 free on each node
    c.expect("OK")
    time.sleep(0.2)
    d.send("SUBMIT 100 0 2")  # 1 free on node 0
    d.expect("OK")
    time.sleep(0.1)
    r = d.report()
    assert r[0] == "2 nodes, 2 jobs running, 2 jobs waiting", r[0]
    assert r[3] == "%4d   %4d/%4d" % (0, 4, 6), r[3]
    assert r[4] == "%4d   %4d/%4d" % (1, 3, 4), r[4]
    assert r[5:9] == ["", "Active Jobs:",
                      "Conn.     Running    Remaining  Assigned Nodes",
                      "------------------------------------"], r[5:9]
    # least time left first: b (3000) then a (5000)
    act = [l.split() for l in r[9:11]]
    assert [int(x[0]) for x in act] == [b.pid, a.pid], act
    assert act[0][3:] == ["1", "-"] and act[1][3:] == ["0", "0"], act
    for x, t in zip(act, (3000, 5000)):
        run, rem = int(x[1]), int(x[2])
        assert 250 <= run <= 900 and abs(run + rem - t) <= 1, (run, rem)
    assert r[9] == "%10d      %10d       %10d       %3d %3s" % (
        b.pid, int(act[0][1]), int(act[0][2]), 1, "-"), repr(r[9])
    assert r[11:15] == ["", "Waiting Jobs:",
                        "Conn.     Waiting   Requested Nodes",
                        "------------------------------------"], r[11:15]
    # longest wait first: c then d
    wt = [l.split() for l in r[15:17]]
    assert [int(x[0]) for x in wt] == [c.pid, d.pid], wt
    assert wt[0][2:] == ["4", "0"] and wt[1][2:] == ["0", "2"], wt
    wc, wd = int(wt[0][1]), int(wt[1][1])
    assert wc > wd >= 80, (wc, wd)
    assert r[15] == "%10d      %10d       %3d %3d" % (c.pid, wc, 4, 0), repr(r[15])
    assert r[17] == ""
    assert r[18] == "average wait time: %.2f" % ((wc + wd) / 2), r[18]
    assert r[19] == "waiting list:", r[19]


def t_kill_agent(S):
    srv = S(ONE)
    a, b = srv.connect(), srv.connect()
    a.send("SUBMIT 60000 2")
    a.expect("OK")
    a.expect(started("0", 60000))
    b.send("SUBMIT 200 2")
    b.expect("OK")
    os.kill(a.pid, signal.SIGKILL)
    b.expect(started("0", 200), timeout=1.0)
    r = b.report()
    assert r[0] == "1 nodes, 1 jobs running, 0 jobs waiting", r[0]
    assert r[3] == "%4d   %4d/%4d" % (0, 2, 2), r[3]


def t_many_connections(S):
    srv = S(ONE)
    for _ in range(3):
        cs = [srv.connect() for _ in range(100)]
        assert len(set(c.pid for c in cs)) == 100
        for c in cs[:50]:
            c.send("SUBMIT 60000 1")
            c.expect("OK")
        for c in cs:
            c.close()
    time.sleep(0.5)
    a = srv.connect()
    r = a.report()
    assert r[0] == "1 nodes, 0 jobs running, 0 jobs waiting", r[0]
    assert r[3] == "%4d   %4d/%4d" % (0, 0, 2), r[3]
    kids = srv.children()
    assert [p for p, _ in kids] == [a.pid], kids  # no zombies, no leftovers


def t_stress(S):
    srv = S("2 3\n3 1\n2 2\n1 0\n", binary="hw1_dbg")
    stop = time.time() + 4
    errors = []

    def worker(seed):
        rnd = random.Random(seed)
        try:
            c = srv.connect()
            while time.time() < stop:
                x = rnd.random()
                if x < 0.45:
                    c.send("SUBMIT %d %d %d" % (rnd.randint(5, 150),
                                                rnd.randint(0, 3),
                                                rnd.randint(0, 2)))
                elif x < 0.70:
                    c.send("STOP")
                elif x < 0.85:
                    c.send("REPORT")
                elif x < 0.95:
                    c.close()
                    c = srv.connect()
                elif KILL_AGENTS:
                    os.kill(c.pid, signal.SIGKILL)
                    c.close()
                    c = srv.connect()
                time.sleep(rnd.random() * 0.02)
                # drain whatever arrived
                while select.select([c.s], [], [], 0)[0]:
                    if not c.s.recv(65536):
                        break
            c.close()
        except Exception as e:  # noqa
            errors.append(repr(e))

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(40)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors, errors[:3]
    time.sleep(0.5)
    assert srv.proc.poll() is None, "server died (invariant violated?)"
    a = srv.connect()
    r = a.report()
    assert r[0] == "3 nodes, 0 jobs running, 0 jobs waiting", r[0]
    assert r[3:6] == ["%4d   %4d/%4d" % (0, 0, 4), "%4d   %4d/%4d" % (1, 0, 4),
                      "%4d   %4d/%4d" % (2, 0, 1)], r[3:6]
    kids = srv.children()
    assert [p for p, _ in kids] == [a.pid], kids
    # and it still schedules
    a.send("SUBMIT 50 3 2")
    a.expect("OK")
    a.expect(started("0 1", 50))
    a.expect(TERMINATED)


def t_shutdown(S):
    srv = S(ONE)
    a = srv.connect()
    path = srv.addr
    assert os.path.exists(path)
    srv.proc.terminate()
    srv.proc.wait(timeout=5)
    assert not os.path.exists(path), "socket file left behind"
    try:
        a.line(2.0)
        raise AssertionError("agent survived the master")
    except Timeout as e:
        assert "closed" in str(e), e


def t_duration_accuracy(S):
    srv = S(ONE)
    a = srv.connect()
    for d in (100, 400, 900, 1500):
        submit(a, d, 1)
        a.expect(started("0", d))
        t0 = time.time()
        a.expect(TERMINATED, timeout=d / 1000 + 2)
        dt = (time.time() - t0) * 1000
        assert d - 60 <= dt <= d + 400, (d, dt)


def t_parallel_timeouts(S):
    srv = S("1 1\n6\n")
    cs = [srv.connect() for _ in range(3)]
    durs = [700, 250, 450]
    t0 = time.time()
    for c, d in zip(cs, durs):
        submit(c, d, 2)
        c.expect(started("0", d))
    order = []
    while len(order) < 3:
        who, line = next_event(cs)
        assert line == TERMINATED, line
        order.append((cs.index(who), (time.time() - t0) * 1000))
    assert [i for i, _ in order] == [1, 2, 0], order
    for i, at in order:
        assert durs[i] - 60 <= at <= durs[i] + 500, order


def t_fifo_equal_jobs(S):
    srv = S(SOLO)
    cs = [srv.connect() for _ in range(5)]
    t0 = time.time()
    for c in cs:
        submit(c, 200, 1)
        time.sleep(0.03)
    starts, ends = [], []
    while len(ends) < 5:
        who, line = next_event(cs)
        kind = starts if line.startswith("Job started") else ends
        kind.append((cs.index(who), (time.time() - t0) * 1000))
    assert [i for i, _ in starts] == [0, 1, 2, 3, 4], starts
    assert [i for i, _ in ends] == [0, 1, 2, 3, 4], ends
    gaps = [b[1] - a[1] for a, b in zip(starts[1:], starts[2:])]
    assert all(140 <= g <= 500 for g in gaps), gaps
    assert 940 <= ends[-1][1] <= 2000, ends


def t_no_preemption(S):
    srv = S(ONE)
    a, b = srv.connect(), srv.connect()
    submit(a, 600, 2)
    a.expect(started("0", 600))
    t0 = time.time()
    submit(b, 50, 1)
    b.silent(0.3)
    a.expect(TERMINATED)
    dt = (time.time() - t0) * 1000
    assert 540 <= dt <= 1000, dt
    b.expect(started("0", 50), timeout=0.5)
    b.expect(TERMINATED)


def t_release_starts_many(S):
    srv = S(QUAD)
    a = srv.connect()
    ws = [srv.connect() for _ in range(4)]
    submit(a, 300, 4)
    a.expect(started("0", 300))
    for w in ws:
        submit(w, 5000, 1)
    a.expect(TERMINATED)
    for w in ws:
        w.expect(started("0", 5000), timeout=0.5)
    rep = parse_report(a.report())
    assert rep["nrun"] == 4 and rep["nwait"] == 0, rep
    assert rep["nodes"] == [(4, 4)], rep


def t_capacity_never_exceeded(S):
    srv = S("1 1\n3\n")
    obs = srv.connect()
    cs = [srv.connect() for _ in range(10)]
    for c in cs:
        submit(c, 150, 1)
    got = dict((c, []) for c in cs)
    end = time.time() + 10
    while any(len(v) < 2 for v in got.values()):
        assert time.time() < end, "jobs did not finish"
        rep = parse_report(obs.report())
        assert rep["nrun"] <= 3 and rep["nodes"][0][0] <= 3, rep
        assert rep["nrun"] + rep["nwait"] <= 10, rep
        try:
            while True:
                who, line = next_event(cs, timeout=0.02)
                got[who].append(line)
        except Timeout:
            pass
    for c in cs:
        assert got[c] == [started("0", 150), TERMINATED], got[c]
    rep = parse_report(obs.report())
    assert rep["nrun"] == 0 and rep["nwait"] == 0, rep
    assert rep["nodes"] == [(0, 3)], rep


def t_everyone_finishes(S):
    srv = S("2 2\n3 2\n2 2\n", binary="hw1_dbg")
    rnd = random.Random(7)
    cs = [srv.connect() for _ in range(15)]
    want = {}
    for c in cs:
        d = rnd.randint(50, 250)
        req = (rnd.randint(0, 3), rnd.randint(0, 2))
        if req == (0, 0):
            req = (1, 0)
        submit(c, d, *req)
        want[c] = d
    got = dict((c, []) for c in cs)
    end = time.time() + 15
    while any(len(v) < 2 for v in got.values()):
        who, line = next_event(cs, timeout=max(0.1, end - time.time()))
        got[who].append(line)
    for c in cs:
        assert got[c][0].startswith("Job started at: <"), got[c]
        assert got[c][0].endswith("completion in %d" % want[c]), got[c]
        assert got[c][1] == TERMINATED, got[c]
    assert srv.proc.poll() is None
    rep = parse_report(cs[0].report())
    assert rep["nrun"] == 0 and rep["nwait"] == 0, rep
    assert rep["nodes"] == [(0, 5), (0, 4)], rep


def t_oldest_and_cheapest_first(S):
    srv = S(ONE)
    a, b, c, d = [srv.connect() for _ in range(4)]
    submit(a, 400, 2)
    a.expect(started("0", 400))
    submit(b, 100, 1)  # oldest and cheapest
    time.sleep(0.05)
    submit(c, 300, 2)
    time.sleep(0.05)
    submit(d, 200, 1)
    a.expect(TERMINATED)
    b.expect(started("0", 100), timeout=0.5)
    d.expect(started("0", 200), timeout=0.5)  # fits next to b, c does not
    c.silent(0.1)
    b.expect(TERMINATED)
    d.expect(TERMINATED)
    c.expect(started("0", 300), timeout=0.5)


def t_domination_picks_middle(S):
    srv = S(ONE)
    a, x, y, z = [srv.connect() for _ in range(4)]
    submit(a, 400, 2)
    a.expect(started("0", 400))
    submit(x, 400, 2)  # oldest, most expensive
    time.sleep(0.05)
    submit(y, 100, 2)  # dominates z
    time.sleep(0.05)
    submit(z, 200, 2)
    order = start_order([x, y, z], "xyz", 3)
    assert order == ["y", "x", "z"], order


def t_tie_wait_minus_budget(S):
    srv = S(QUAD)
    a, b, c = [srv.connect() for _ in range(3)]
    submit(a, 300, 4)
    a.expect(started("0", 300))
    submit(b, 200, 3)  # budget 600
    time.sleep(0.05)
    submit(c, 200, 2)  # budget 400
    a.expect(TERMINATED)
    b.expect(started("0", 200), timeout=0.5)
    c.silent(0.1)
    b.expect(TERMINATED)
    c.expect(started("0", 200), timeout=0.5)


def old_and_cheap(srv):
    a, b, c = [srv.connect() for _ in range(3)]
    submit(a, 60000, 2)
    a.expect(started("0", 60000))
    submit(b, 150, 2)  # budget 300
    time.sleep(0.25)
    submit(c, 100, 2)  # budget 200, 250 ms younger
    time.sleep(0.4)
    a.send("STOP")
    a.expect("OK")
    return b, c


def t_newer_cheaper_can_win(S):
    b, c = old_and_cheap(S(ONE))
    c.expect(started("0", 100), timeout=0.5)
    c.expect(TERMINATED)
    b.expect(started("0", 150), timeout=0.5)


def t_maxwait_oldest_first(S):
    b, c = old_and_cheap(S(ONE, binary="hw1_fast"))
    b.expect(started("0", 150), timeout=0.5)
    b.expect(TERMINATED)
    c.expect(started("0", 100), timeout=0.5)


def t_multi_node_first_fit(S):
    srv = S("1 3\n2\n2\n2\n")
    a, b, c, d, e, f = [srv.connect() for _ in range(6)]
    for cl, node in ((a, "0"), (b, "1"), (c, "2")):
        submit(cl, 5000, 2)
        cl.expect(started(node, 5000))
    submit(d, 5000, 2)
    d.silent(0.2)
    b.send("STOP")
    b.expect("OK")
    d.expect(started("1", 5000), timeout=0.5)
    submit(e, 5000, 1)
    e.silent(0.2)
    a.send("STOP")
    a.expect("OK")
    e.expect(started("0", 5000), timeout=0.5)
    submit(f, 5000, 1)
    f.expect(started("0", 5000), timeout=0.5)
    rep = parse_report(f.report())
    assert rep["nrun"] == 4 and rep["nwait"] == 0, rep
    assert rep["nodes"] == [(2, 2), (2, 2), (2, 2)], rep


def t_no_partial_hold(S):
    srv = S("2 2\n2 1\n2 0\n")
    a, b, c, d = [srv.connect() for _ in range(4)]
    submit(a, 5000, 0, 1)
    a.expect(started("- 0", 5000))
    submit(b, 5000, 2, 1)  # type 1 is taken, must not keep type 0 busy
    submit(c, 5000, 2, 0)
    c.expect(started("0 -", 5000))
    submit(d, 5000, 2, 0)
    d.expect(started("1 -", 5000))
    rep = parse_report(b.report())
    assert rep["nodes"] == [(3, 3), (2, 2)], rep
    assert [w[0] for w in rep["waiting"]] == [b.pid], rep
    a.send("STOP")
    a.expect("OK")
    b.silent(0.2)
    c.send("STOP")
    c.expect("OK")
    b.expect(started("0 0", 5000), timeout=0.5)


def t_report_lifecycle(S):
    srv = S(ONE)
    obs, a, b = [srv.connect() for _ in range(3)]
    rep = parse_report(obs.report())
    assert (rep["nrun"], rep["nwait"], rep["nodes"]) == (0, 0, [(0, 2)]), rep
    submit(a, 400, 2)
    a.expect(started("0", 400))
    rep = parse_report(obs.report())
    assert (rep["nrun"], rep["nwait"], rep["nodes"]) == (1, 0, [(2, 2)]), rep
    assert rep["active"][0][0] == a.pid and rep["active"][0][3] == ["0"], rep
    submit(b, 300, 1)
    rep = parse_report(obs.report())
    assert (rep["nrun"], rep["nwait"]) == (1, 1), rep
    assert rep["waiting"][0][0] == b.pid and rep["waiting"][0][2] == [1], rep
    a.expect(TERMINATED)
    b.expect(started("0", 300), timeout=0.5)
    rep = parse_report(obs.report())
    assert (rep["nrun"], rep["nwait"], rep["nodes"]) == (1, 0, [(1, 2)]), rep
    assert rep["active"][0][0] == b.pid, rep
    b.expect(TERMINATED)
    rep = parse_report(obs.report())
    assert (rep["nrun"], rep["nwait"], rep["nodes"]) == (0, 0, [(0, 2)]), rep
    obs.silent(0.1)


def t_report_times_advance(S):
    srv = S(ONE)
    a, b = srv.connect(), srv.connect()
    submit(a, 3000, 2)
    a.expect(started("0", 3000))
    submit(b, 100, 1)
    r1 = parse_report(b.report())
    t1 = time.time()
    time.sleep(0.5)
    r2 = parse_report(b.report())
    dt = (time.time() - t1) * 1000
    run1, rem1 = r1["active"][0][1:3]
    run2, rem2 = r2["active"][0][1:3]
    assert abs((run2 - run1) - dt) <= 150, (run1, run2, dt)
    assert abs((rem1 - rem2) - dt) <= 150, (rem1, rem2, dt)
    assert abs(run2 + rem2 - 3000) <= 1, (run2, rem2)
    w1, w2 = r1["waiting"][0][1], r2["waiting"][0][1]
    assert abs((w2 - w1) - dt) <= 150, (w1, w2, dt)
    assert abs(r2["avg"] - w2) < 0.01, r2


def t_report_ordering(S):
    srv = S(QUAD)
    run = [srv.connect() for _ in range(4)]
    wait = [srv.connect() for _ in range(3)]
    for c, d in zip(run, (5000, 2000, 8000, 3000)):
        submit(c, d, 1)
        c.expect(started("0", d))
    for c in wait:
        submit(c, 100, 1)
        time.sleep(0.1)
    rep = parse_report(wait[0].report())
    assert [x[0] for x in rep["active"]] == [run[1].pid, run[3].pid,
                                             run[0].pid, run[2].pid], rep
    left = [x[2] for x in rep["active"]]
    assert left == sorted(left), left
    assert [x[0] for x in rep["waiting"]] == [c.pid for c in wait], rep
    waits = [x[1] for x in rep["waiting"]]
    assert waits == sorted(waits, reverse=True) and waits[0] > waits[2], waits
    assert abs(rep["avg"] - sum(waits) / 3.0) < 0.01, rep


def t_resubmit_cycles(S):
    srv = S(ONE)
    a = srv.connect()
    for i in range(8):
        d = 40 + 20 * i
        submit(a, d, 1 + i % 2)
        a.expect(started("0", d))
        a.expect(TERMINATED)
    rep = parse_report(a.report())
    assert (rep["nrun"], rep["nwait"], rep["nodes"]) == (0, 0, [(0, 2)]), rep


def t_resubmit_resets_wait(S):
    srv = S(ONE)
    a, b = srv.connect(), srv.connect()
    submit(a, 60000, 2)
    a.expect(started("0", 60000))
    submit(b, 100, 1)
    time.sleep(0.5)
    w1 = parse_report(b.report())["waiting"][0][1]
    assert w1 >= 450, w1
    b.send("STOP")
    b.expect("OK")
    submit(b, 100, 1)
    w2 = parse_report(b.report())["waiting"][0][1]
    assert w2 < 250, w2


def t_waiting_client_leaves(S):
    srv = S(ONE)
    a, b, c = [srv.connect() for _ in range(3)]
    submit(a, 500, 2)
    a.expect(started("0", 500))
    submit(b, 100, 2)
    submit(c, 100, 2)
    b.close()
    time.sleep(0.15)
    rep = parse_report(c.report())
    assert [w[0] for w in rep["waiting"]] == [c.pid], rep
    a.expect(TERMINATED)
    c.expect(started("0", 100), timeout=0.5)


def t_messages_only_to_owner(S):
    srv = S(QUAD)
    a, b, obs = [srv.connect() for _ in range(3)]
    submit(a, 200, 1)
    a.expect(started("0", 200))
    submit(b, 600, 1)
    b.expect(started("0", 600))
    a.expect(TERMINATED)
    b.silent(0.15)
    b.expect(TERMINATED)
    a.silent(0.1)
    obs.silent(0.1)


def t_stop_frees_immediately(S):
    srv = S(ONE)
    a = srv.connect()
    submit(a, 60000, 2)
    a.expect(started("0", 60000))
    assert parse_report(a.report())["nodes"] == [(2, 2)]
    a.send("STOP")
    a.expect("OK")
    rep = parse_report(a.report())
    assert (rep["nrun"], rep["nodes"]) == (0, [(0, 2)]), rep
    a.silent(0.2)


def t_quit_idle(S):
    srv = S(ONE)
    a, b = srv.connect(), srv.connect()
    a.send("QUIT")
    try:
        a.line(1.0)
        raise AssertionError("connection still open after QUIT")
    except Timeout as e:
        assert "closed" in str(e), e
    end = time.time() + 3
    while a.pid in [p for p, _ in srv.children()]:
        assert time.time() < end, "agent still there after QUIT"
        time.sleep(0.05)
    assert b.pid in [p for p, _ in srv.children()]
    submit(b, 50, 2)
    b.expect(started("0", 50))
    b.expect(TERMINATED)


def t_tcp_two_clients(S):
    srv = S(ONE, tcp=True)
    a, b = srv.connect(), srv.connect()
    submit(a, 5000, 2)
    a.expect(started("0", 5000))
    submit(b, 150, 2)
    b.silent(0.2)
    a.send("STOP")
    a.expect("OK")
    b.expect(started("0", 150), timeout=0.5)
    b.expect(TERMINATED)
    rep = parse_report(a.report())
    assert (rep["nrun"], rep["nwait"]) == (0, 0), rep


def t_long_and_short_mix(S):
    srv = S(ONE)
    a, b = srv.connect(), srv.connect()
    submit(a, 1200, 1)
    a.expect(started("0", 1200))
    t0 = time.time()
    for d in (150, 200, 150):
        submit(b, d, 1)
        b.expect(started("0", d))
        b.expect(TERMINATED)
    a.expect(TERMINATED)
    dt = (time.time() - t0) * 1000
    assert 1140 <= dt <= 1800, dt


def t_burst_submits(S):
    srv = S("1 1\n20\n")
    cs = [srv.connect() for _ in range(20)]
    t0 = time.time()
    for c in cs:
        c.send("SUBMIT 300 1")
    for c in cs:
        c.expect("OK")
        c.expect(started("0", 300))
    for c in cs:
        c.expect(TERMINATED)
    dt = (time.time() - t0) * 1000
    assert 240 <= dt <= 1200, dt
    rep = parse_report(cs[0].report())
    assert (rep["nrun"], rep["nodes"]) == (0, [(0, 20)]), rep


def main():
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("t_") and callable(f)]
    only = sys.argv[1:]
    failed = skipped = 0
    for name, fn in tests:
        if only and name not in only:
            continue
        servers = []

        def S(*args, **kw):
            servers.append(Server(*args, **kw))
            return servers[-1]

        t0 = time.time()
        try:
            fn(S)
            print("PASS %-28s %.1fs" % (name, time.time() - t0))
        except Skip as e:
            skipped += 1
            print("SKIP %-28s %s" % (name, e))
        except Exception as e:  # noqa
            failed += 1
            print("FAIL %-28s %s: %s" % (name, type(e).__name__, e))
        finally:
            for s in servers:
                s.stop()
    if skipped:
        print("%d skipped" % skipped)
    print("%d failed" % failed)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
