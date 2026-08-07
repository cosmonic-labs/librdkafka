# Building librdkafka for WASI

Single-threaded (cooperative) build of librdkafka for `wasm32-wasip2`.

WASI components have no threads: `thrd_create` resolves to a wasi-libc stub
returning `ENOTSUP`, so upstream `rd_kafka_new()` fails before returning a
handle. This build replaces preemptive threads with cooperative tasks. It works
because librdkafka is already timeout-driven end to end — every layer from
`rd_kafka_broker_thread_main` down to `poll()` takes a deadline and returns when
it expires, so a zero timeout turns the whole stack into one non-blocking pass.

Each former thread main is split into `setup` / `serve` / `teardown`. The
threaded path calls them in the original order and is unchanged.

## Build

```sh
WASI_SDK=/path/to/wasi-sdk-34
cmake -S . -B build-wasi \
  -DCMAKE_TOOLCHAIN_FILE=$WASI_SDK/share/cmake/wasi-sdk-p2.cmake \
  -DCMAKE_C_FLAGS="-D_WASI_EMULATED_SIGNAL -D_WASI_EMULATED_PROCESS_CLOCKS -D_WASI_EMULATED_GETPID" \
  -DRDKAFKA_BUILD_STATIC=ON -DRDKAFKA_BUILD_EXAMPLES=OFF -DRDKAFKA_BUILD_TESTS=OFF \
  -DWITH_SSL=OFF -DWITH_SASL=OFF -DWITH_ZLIB=OFF -DWITH_ZSTD=OFF -DWITH_CURL=OFF \
  -DENABLE_LZ4_EXT=OFF -DWITH_LIBDL=OFF -DWITH_PLUGINS=OFF
cmake --build build-wasi -j8
```

## Linking an application — REQUIRED flags

```sh
$WASI_SDK/bin/clang --target=wasm32-wasip2 \
  -D_WASI_EMULATED_SIGNAL -D_WASI_EMULATED_PROCESS_CLOCKS -D_WASI_EMULATED_GETPID \
  -Wl,-z,stack-size=8388608 \
  -I librdkafka/src -o app.wasm app.c \
  build-wasi/src/librdkafka.a \
  -lwasi-emulated-signal -lwasi-emulated-process-clocks -lwasi-emulated-getpid -lm
```

**`-Wl,-z,stack-size` is not optional.** wasi-sdk defaults the shadow stack to
64 KB. On a threaded build each handler has its own multi-megabyte thread stack;
here every handler runs nested inside the application's own call, so one stack
carries `app -> consumer_poll -> q_pop -> cnd_timedwait -> pump -> broker_step
-> ... -> msgset_reader`. 64 KB overflows partway through fetch parsing.

The overflow does not look like a stack overflow. The shadow stack pointer wraps
to `~0xFFFFFFFF` and the *next function prologue* faults storing locals, so the
trap reads as `out of bounds memory access` at an address just above 4 GiB
(e.g. `0x1000019b8`), attributed to whatever function was being entered. If you
see that shape, raise the stack before looking anywhere else.

## What is verified

Against Apache Kafka 4.3.1, running under `wasmtime run -S inherit-network=y`:

| Area | Status |
|---|---|
| Producer: connect, metadata, leader failover, produce, delivery reports | works |
| Consumer groups: FindCoordinator, JoinGroup, SyncGroup, rebalance callbacks | works |
| Fetch, consume, OffsetCommit, LeaveGroup, clean close | works |
| SASL PLAIN over `SASL_PLAINTEXT` | works |
| TLS over `SSL`, with broker certificate verification | works |

## TLS

OpenSSL 3.5.4 cross-compiles to `wasm32-wasip2` with four features turned off,
each because WASI genuinely lacks the primitive:

```sh
CC=$WASI_SDK/bin/clang AR=$WASI_SDK/bin/llvm-ar RANLIB=$WASI_SDK/bin/llvm-ranlib \
./Configure linux-generic32 --target=wasm32-wasip2 \
  no-asm no-threads no-shared no-dso no-engine no-tests \
  no-secure-memory `# needs mmap/mprotect` \
  no-ui-console   `# needs termios.h + signals` \
  no-quic         `# needs socketpair` \
  -DOPENSSL_NO_UNIX_SOCK `# wasi-libc has AF_UNIX and sys/un.h but an incomplete
                            sockaddr_un, so OpenSSL's own autodetect misses it` \
  -D_WASI_EMULATED_SIGNAL -D_WASI_EMULATED_PROCESS_CLOCKS -D_WASI_EMULATED_GETPID
make build_generated && make -j8 libcrypto.a libssl.a
```

Run `make clean` before re-Configuring. Changing options without it leaves stale
objects, and `no-quic` in particular then fails at link with undefined
`ossl_quic_obj_get0_handshake_layer` from `s3_lib.o`.

Then build librdkafka with `-DWITH_SSL=ON -DOPENSSL_ROOT_DIR=<openssl>` and link
the app against `librdkafka.a libssl.a libcrypto.a` in that order.

Enabling SSL also brings up SASL SCRAM and OAUTHBEARER, which need OpenSSL for
their crypto. `builtin.features` then reports
`ssl,sasl_plain,sasl_scram,sasl_oauthbearer`.

Verified against Kafka 4.3.1's SSL listener:

```
Using statically linked OpenSSL version OpenSSL 3.5.4
Loading CA certificate(s) from file /certs/ca.pem
ssl://127.0.0.1:9093/bootstrap: Broker SSL certificate verified
```

The CA file must be reachable through a wasmtime preopen
(`--dir /host/certs::/certs` plus `ssl.ca.location=/certs/ca.pem`); there is no
ambient filesystem.

librdkafka's own test suite passes **133 tests, 0 failures** on the native build
of this tree, so the loop refactor is regression-free on the threaded path.

## Design notes for anyone extending this

The scheduler lives in `src/rdwasm.{c,h}`: a fixed task table, a round-robin
pump, and a re-entrancy guard. Tasks are registered in place of `thrd_create`
and retire by returning non-zero from their step.

`cnd_wait` / `cnd_timedwait` are the pump points. Because every librdkafka wait
re-tests its predicate in a loop, each wait site becomes a scheduler tick for
free. Two details are load-bearing:

- **The mutex is dropped across the pump**, exactly as a real condition variable
  does. Without it `rd_kafka_new()` deadlocks: it waits on `rk_init_lock` for a
  handler that must take `rk_init_lock` to report itself started.
- **`cnd_timedwait` honours its deadline** rather than always reporting a
  wakeup, so a genuinely failed init times out instead of spinning forever.

The hazard to keep in mind when touching this: **a lock held across a pump point
provides no mutual exclusion.** There is one thread, so nothing blocks the
pumped task from entering the same critical section. Two bugs came from this and
both are fixed — `rd_kafka_timers_run` re-entering its own timer wheel, and
tasks outliving the client handle they point into (`rd_wasm_join_owner()` now
drains a handle's tasks before it is freed). Expect more of the same shape if
you widen coverage.

`thrd_is_current()` returns true unconditionally, since the application and
every handler genuinely share one thread. Two assertions that depend on it
distinguishing threads are disabled for WASI: the `rd_kafka_destroy()` and
`rd_kafka_cgrp_terminate()` "called from a librdkafka thread" guards. Both
protect against a handler waiting on itself, which cannot arise here because the
wait itself pumps the scheduler.
