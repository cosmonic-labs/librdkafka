# Carrying this fork

Two long-lived branches, both rebased onto upstream rather than merged:

| Branch | Contents | Destination |
|---|---|---|
| `wasi` | Single-threaded WASI support. All `#ifdef __wasi__`, ~645 lines across 14 files in `src/`. | Upstreamable to confluentinc/librdkafka |
| `cosmonic` | `wasi` plus `wit/`, `wasi/`, `.github/`, `WASI.md`. | Ours permanently |

They are split because they have opposite lifecycles. Confluent will not carry a
`cosmonic:kafka` WIT package or GitHub Actions in a Semaphore repo, and if the
two shared a branch the upstreamable part would be hostage to the part that
cannot be upstreamed — you could not open a pull request without hand-extracting
it, under whatever time pressure prompted the PR. Kept apart, the PR is one
`git format-patch upstream/master..wasi` away, permanently.

Rebase rather than merge so `wasi` stays a reviewable series against current
upstream. The cost is rewritten SHAs; see [Tagging](#tagging).

## Why this is cheap

Upstream churn on the files this fork touches, measured over 12 months:

```
src/rdkafka.c            6 commits     src/rdkafka_cgrp.c       6
src/rdkafka_timer.c      2             src/rdkafka_int.h        2
everything else          1 each
```

Narrowed to the regions actually patched, it is smaller still: 2 commits touched
the `rd_kafka_thread_main` region and 1 touched the broker thread-main region in
a year. Upstream runs at roughly 7 commits/month.

The patch is mostly `#ifdef __wasi__` conditions widening existing `#ifndef
_WIN32` blocks — adding conditions to lines upstream rarely edits, rather than
restructuring its logic. Monthly rebases should be uneventful.

## Rebasing

```sh
git fetch upstream

# 1. Rebase the upstreamable branch first.
git checkout wasi
git tag "wasi-$(date +%Y.%m)-pre"          # escape hatch, see Tagging
git rebase upstream/master

# 2. Then carry the tooling on top.
git checkout cosmonic
git rebase wasi
```

Conflicts land almost entirely in `src/rdkafka.c`, `src/rdkafka_broker.c` and
`src/rdkafka_background.c`, and almost always because upstream edited a
`#ifndef _WIN32` block this fork widened to `#if !defined(_WIN32) &&
!defined(__wasi__)`. Keep upstream's body, re-widen the guard.

## Verify before force-pushing, not after

**A clean rebase is not evidence of a correct one.** The `#ifdef __wasi__`
guards rebase silently even when upstream changes the logic *inside* the blocks
they wrap: git reports no conflict and you get a fork that compiles and behaves
differently. Nothing about the diff will tell you.

What catches it:

```sh
# Threaded path — the refactor split three thread mains, so this is the
# regression that matters. Expect 0 failures.
cmake -S . -B build-native -DRDKAFKA_BUILD_TESTS=ON -DWITH_SASL=OFF -DWITH_CURL=OFF
cmake --build build-native -j"$(nproc)"
cd tests && TESTS_SKIP_UNSUPPORTED=y RDKAFKA_TEST_CONF=/dev/null \
  ../build-native/tests/test-runner -l -Q

# Cooperative path — against a real broker.
bash wasi/ci/start-kafka.sh plaintext
bash wasi/ci/run-test.sh producer_test.wasm wasi-producer-topic
bash wasi/ci/run-test.sh consumer_test.wasm wasi-consumer-topic grp-check
```

Both run in CI (`.github/workflows/wasi.yml`) on every push, so pushing the
rebase to a branch and reading the result is equally good — just do it before
force-pushing over the shared branch.

## Tagging

Rebasing rewrites SHAs, so anything pinning this fork by commit — a CMake
`FetchContent` `GIT_TAG`, a submodule — breaks silently on every rebase.

Tag before each rebase and point consumers at tags, never at branch tips:

```sh
git tag wasi-2026.08          # or wasi-v2.15.0-1, tracking the upstream base
```

Consumers of the *interface* rather than the library are already insulated:
`cosmonic:kafka` publishes to OCI by semver
(`.github/workflows/wit-publish.yml`), and those references are unaffected by
fork history. Prefer pointing people at the OCI package wherever possible.

## Upstreaming

`wasi` is one commit today. That is deliberate — a coherent feature is easier to
review than six micro-commits, and the parts are not cleanly separable by file
(the portability fixes and the cooperative scheduling both touch `rdkafka.c`,
`rdkafka_broker.c` and `rdkafka_background.c`, so splitting means hunk-level
surgery).

Worth splitting with `git rebase -i` *at PR time*, into roughly:

1. Portability fixes for `wasm32-wasip2` — signal/`mkstemp`/`sendmsg` guards.
   Independently useful and the easiest to get accepted.
2. The cooperative scheduler and tinycthread shim.
3. The thread-main splits, timer guard, and relaxed thread-identity assertions.

Do it then, not now: the split is only worth its cost once someone is actually
reading it, and until then it is a series to keep rebasing.
