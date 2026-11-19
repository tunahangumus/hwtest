# hw1 job manager tests

Black-box tests for the CENG 536 HW1 job manager. The script starts your
`hw1` binary, connects to it over Unix and TCP sockets like a client would,
and checks the replies and their timing. It does not look at your source.

50 tests: connections, SUBMIT / STOP / QUIT / REPORT, completion times,
job selection order, the MAX wait rule, node assignment, the REPORT format.

## Running

Python 3.7 or newer, no extra packages.

```
make hw1
HW1_DIR=/path/to/your/hw1 python3 itest.py
```

Run a few tests only:

```
HW1_DIR=/path/to/your/hw1 python3 itest.py t_single_job t_report_jobs
```

Without `HW1_DIR` the binaries are looked up in the parent directory of the
script, so you can also copy `itest.py` into `hw1/tests/`.

### Starvation tests

Three tests need the MAX wait to pass quickly. They use a second binary,
`hw1_fast`, built from the same sources with MAX set to 300 ms:

```
gcc -DMAX=300 -o hw1_fast *.c -lpthread -lrt
```

This only works if your code has `#ifndef MAX` around the definition. If
`hw1_fast` does not exist these tests are skipped.

`hw1_dbg` is used by two tests when it exists; otherwise they run `hw1`.

## What the tests assume

Some of these are not fixed by the homework text. A failing test may just
mean your reading is different.

- `STOP` answers `OK`; a wrong command or a `STOP` without a job answers
  with a line starting with `ERROR`.
- A request that no node can ever satisfy is answered with `ERROR`.
- A second `SUBMIT` while the connection has a job is answered with `ERROR`.
- For each type the lowest numbered node with enough free processors is used.
- "Dominates" is strict: longer wait and smaller budget.
- On equal domination counts the smallest `wait - budget` wins.
- In `REPORT` the node line is `used/total`, summed over the types.
- `average wait time` is the average over the jobs waiting now.
- `waiting list:` is printed with nothing after it.
- `\n`, `\r` and `\0` all end a command line.
- If an agent process is killed, the master releases its job and slot.
- On SIGTERM the master closes the connections and removes the socket file.

Timing checks allow a few hundred milliseconds of slack, but a heavily
loaded machine can still fail them.
