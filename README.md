# Kiln

[![Build](https://img.shields.io/badge/build-passing-brightgreen)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go](https://img.shields.io/badge/Go-1.21+-blue.svg)](https://golang.org)
[![Part of Agentic_](https://img.shields.io/badge/Part_of-Agentic__Super__OS-8A2BE2.svg)](#)

A storage engine built from scratch in Go — one verified stage at a time.

The goal isn't to replace SQLite or DuckDB. It's to stop treating them as
black boxes by building the same primitives yourself: pages, a B-tree, a
buffer pool, crash recovery, and (eventually) concurrent transactions.

## Roadmap

- [x] **Stage 1 — Page storage.** Fixed-size pages, raw byte layout, a
      single backing file.
- [x] **Stage 2 — B+tree indexing.** Insert / Get / Delete / All, with node
      splitting on overflow and merge-driven collapse on underflow.
- [x] **Stage 3 — Buffer pool.** LRU page cache with dirty tracking,
      sitting transparently under BTree via a small interface.
- [x] **Stage 4 — Crash recovery via rollback journal.** `Insert`/`Delete`
      are transactions now: all-or-nothing even across a real process
      kill.
- [x] **Stage 5 — Multi-operation transactions & concurrency control.**
      `WithTxn` groups several `Insert`/`Delete` calls into one atomic
      unit; a single coarse lock makes sharing one `*BTree` across
      goroutines genuinely safe. *(this is what's in the repo right now)*
- [ ] Stage 6 *(stretch)* — minimal SQL layer
- [ ] Stage 7 *(stretch)* — Raft replication

Each stage is meant to be independently correct and independently tested
before the next one is built on top of it.

## Design decisions so far

- **Page size: 4096 bytes.** Matches the common OS page size; the same
  default modern SQLite uses.
- **File layout:** page 0 is a reserved meta page (magic number, page
  size, page count, B-tree root pointer, free-list head). Pages 1+ are
  opaque data pages as far as this layer is concerned.
- **B-tree over LSM-tree.** Mirrors what SQLite actually does under the
  hood, and has the best beginner-friendly reference material for a
  first build. (Easy to revisit if an LSM-tree version becomes
  interesting later — different project, same page layer underneath.)
- **B+tree, not a plain B-tree.** Only leaves hold values; internal nodes
  are pure routing (separator keys + child pointers). Leaves are linked
  left-to-right (`nextLeaf`), so an ordered full scan (`All()`) never has
  to touch an internal node.
- **Slotted page layout, rebuilt whole on every write.** Each node page is
  a header, a cell-pointer array, and packed cell data growing from the
  opposite end. Rather than patch pages in place (and have to manage
  fragmentation from deletes/updates), every write re-encodes the entire
  node from its in-memory cell list. Simpler and provably free of
  fragmentation bugs, at the cost of O(page size) work per write — a cost
  Stage 3's buffer pool exists to hide.
- **Delete rebalances by merge/collapse only, no borrow-from-sibling.**
  Textbook B-trees keep nodes at least half full by borrowing a cell from
  a neighbor before resorting to a merge. This implementation skips that
  optimization: a node only triggers rebalancing when it's *completely*
  empty, at which point it's merged/spliced out of its parent. Simpler to
  get right, at the cost of tolerating sparser (but never structurally
  unbalanced) nodes. Height-balance is unaffected — the tradeoff is
  purely on fill-factor.
- **BTree depends on an interface (`pager.Store`), not the concrete
  `Pager`.** `Store` is `ReadPage`/`WritePage`/`AllocatePage`/`ReadMeta`/
  `WriteMeta`/`PageCount`/`Close`/`Begin`/`Commit`. This is the entire
  reason the buffer pool (Stage 3) and the journal (Stage 4) could both
  be added with **zero changes to Stage 2's B-tree algorithms** —
  `bufferpool.BufferPool` and `journal.Journal` both implement `Store`
  too, so `btree.Open` can't tell which one it has, or how many of them
  are stacked.
- **LRU eviction, one coarse mutex.** `container/list` + a map gives O(1)
  get/put/evict. A single mutex around the whole pool is simple and
  correct, at the cost of serializing access to unrelated pages —
  per-page locking is a reasonable future refinement, not attempted here.
- **`AllocatePage`/`ReadMeta`/`WriteMeta` pass straight through, uncached.**
  Allocation only happens on a node split (rare compared to every
  read/write), and BTree only touches the meta page once, at root
  creation. Caching either would add real complexity (meta-page
  invalidation, allocation bookkeeping) for a path that was never the
  bottleneck.
- **Crash recovery is a rollback journal, not full WAL-with-checkpointing.**
  SQLite historically supports both: a rollback journal (log a page's OLD
  content before overwriting it; undo on recovery) and WAL mode (log full
  NEW pages to a separate file; readers check that file first; a
  checkpoint folds it back into the main one). WAL mode needs a WAL index
  so readers can find a page's latest version, plus read-path changes
  throughout the stack. A rollback journal gives the same all-or-nothing
  guarantee with a much smaller surface: it only intercepts *writes*,
  never reads, so BufferPool and BTree above it stay completely
  unaware it exists — see how little btree.go had to change (below).
- **Layering is `BTree → BufferPool → Journal → Pager`, and the order is
  load-bearing.** `Commit()` on the buffer pool flushes every dirty page
  down to the journal *before* telling the journal to finalize. Get that
  backwards and a write could sit in the buffer pool, never reach the
  journal at all, and a crash would silently lose it despite `Commit()`
  having "succeeded."
- **Fixed-size, checksummed journal records.** Each record is
  `[pageNum][old page bytes][crc32]`. Fixed size means a torn trailing
  record (from a crash mid-*append to the journal itself*) is caught by a
  simple length check; the checksum catches a same-length-but-corrupted
  one. Either way, recovery stops there and discards it rather than
  trusting garbage — everything valid before it still applies.
- **A page is only journaled the first time it's touched per transaction.**
  Writing the same page twice in one operation (common during a split)
  must not re-journal the intermediate value as if it were the
  pre-transaction one — that would make rollback restore the wrong state.
- **`Begin()` self-heals a dangling transaction lazily; Stage 5 later adds
  an immediate `Rollback()` too.** Stage 4 shipped without an explicit
  `Rollback` — if the caller hits an error and never calls `Commit`, the
  *next* `Begin()` detects the leftover journal and replays it, whether
  that's the next operation in the same process or a fresh process after
  a restart. That's still true and still the only mechanism across a real
  crash. But it's not enough once operations can run concurrently — see
  below for why Stage 5 has to add an explicit, immediate `Rollback`.
- **`AllocatePage` itself isn't journaled.** A crash between allocating a
  page and committing the transaction that requested it leaves at worst
  one leaked page (nothing ever references it, since whatever would have
  referenced it is itself rolled back) — not corruption. Same tradeoff as
  Stage 2's unreclaimed dead leaves; the free-list is still the fix,
  still unbuilt.
- **`WithTxn` takes a closure, not `Begin`/`Commit` calls the caller
  makes directly.** `func (bt *BTree) WithTxn(fn func(*Txn) error) error`
  acquires `bt.mu` once, for fn's entire duration, and passes fn a `*Txn`
  whose `Insert`/`Delete`/`Get` call BTree's core logic directly —
  bypassing the normal locking and per-call `Begin`/`Commit` that
  `Insert`/`Delete` do on their own. An explicit `BeginTxn`/`CommitTxn`
  pair looked simpler at first, but `sync.Mutex` isn't reentrant in Go: if
  `Insert` always locked `bt.mu` itself, calling it from inside an
  already-locked explicit transaction would deadlock. The closure sidesteps
  this entirely — lock once, no method call inside fn ever needs to
  acquire it again.
- **Concurrency is one coarse `sync.RWMutex`, not per-page locking or
  MVCC.** `Get`/`All` take a read lock (concurrent reads never block each
  other); `Insert`, `Delete`, and `WithTxn` take the full write lock for
  their entire duration. That's a real, tested guarantee — many
  goroutines can safely share one `*BTree` — just not a maximally
  parallel one. Finer-grained locking or snapshot isolation would allow
  concurrent writers; genuinely future work, not attempted here.
- **A real bug this stage caught before it shipped, by reasoning through
  a test before writing it:** the original `Rollback` only evicted pages
  still marked *dirty*. But a multi-page flush that partially succeeds
  marks each flushed page clean *before* moving to the next one — so if
  page 2 of 3 then fails, page 1 is already clean-but-still-part-of-the-
  failed-transaction, and a dirty-only check would leave its wrong,
  uncommitted content sitting in the cache for the next reader to see.
  Fixed by tracking every page touched *since the last Begin* (`txnTouched`),
  not just what's currently dirty, and evicting all of it on Rollback.
  `TestFailedCommitDoesNotLeakPartialState` exists specifically to prove
  this with a real, controlled multi-page partial failure — see "testing
  a crash, for real" below for why that phrase means something specific
  in this project.
- **Why `Rollback` had to come back.** Stage 4 deliberately shipped
  without one — lazy, next-`Begin` recovery was enough when only one
  operation could ever be in flight. Concurrency breaks that: if a writer
  fails and just releases `bt.mu` without rolling back, a reader that
  acquires the lock next could observe the buffer pool's uncommitted,
  partially-applied pages, and nothing would fix that until some later
  `Begin` happened to run. `Insert`, `Delete`, and `WithTxn` all now defer
  an immediate `Rollback` on any non-`Commit` exit — including a panic
  unwinding through `WithTxn`'s closure.

### Testing a crash, for real

The two tests that actually matter for this stage don't simulate a
failure — they spawn a real subprocess and send it a real `SIGKILL`:

- `TestJournalRecoversFromRealCrash` kills a process mid-write to a single
  page and confirms the journal rolls it back on the next open.
- `TestBTreeSurvivesRealCrash` runs the *entire* stack — `BTree` over
  `BufferPool` over `Journal` over `Pager` — through thousands of real
  `Insert` calls (real splits happening along the way), kills the process
  right after a specific number of confirmed commits, then reopens and
  checks: every confirmed key is present and correct, the tree is still
  fully sorted with no duplicates, and — importantly — the recovered
  database still works for a brand new operation afterward, not just for
  reading back what survived.
- `TestWithTxnSurvivesRealCrash` extends this to Stage 5's multi-operation
  transactions specifically: three small batches commit normally first,
  then one large (500-operation) batch gets killed partway through, right
  after confirming progress at operation 250. Recovery must show all
  three committed batches intact and *zero* keys from the killed batch —
  not even the 250 that were individually inserted before the kill
  signal arrived, since the whole batch was one uncommitted transaction.

`SIGKILL` can't be caught or handled, so nothing gets a chance to clean
up — no deferred function runs, no goroutine finishes a write in flight.
That's the actual scenario a rollback journal exists for, so it's the one
actually being tested, not a stand-in for it.

### The buffer pool's benchmark story

The honest result: buffering barely moved the needle. Plain sequential
inserts were a wash (even marginally slower — 35.7µs/op vs 33.6µs/op),
and a workload specifically designed to hammer the same one or two pages
over and over (`BenchmarkInsertHotKeys*`) only improved by about 14%. For
something whose whole job is "stop hitting disk so much," that's a
disappointing headline number, and it's worth understanding rather than
hiding.

The reason: Stage 1 never calls `fsync`, and this container's filesystem
already makes a `WriteAt` to the OS page cache cheap. So the disk I/O the
buffer pool exists to avoid wasn't the bottleneck to begin with — the
dominant cost is Stage 2's own `encode()`/`decode()`, which rebuilds or
re-parses the *entire page* from its cell list on every single access
(a deliberate Stage 2 tradeoff — see above). The buffer pool caches raw
bytes, not decoded nodes, so it has no effect on that cost at all.

This sets up exactly why Stage 3 still matters going forward: once Stage
4 adds real `fsync` calls for crash durability, the gap should open up
for real — `fsync` is genuinely expensive, and batching many logical
writes into one flush before one `fsync` is the actual reason buffer
pools exist in real databases. Measuring this again after Stage 4 lands
should be a much more interesting comparison.

**Update, having actually measured it: that prediction was wrong.**

```
BenchmarkInsertJournalOnly            858 µs/op   39.8 KB/op   214 allocs/op
BenchmarkInsertJournalAndBufferPool   867 µs/op   44.2 KB/op   217 allocs/op
```

No improvement at all — if anything, marginally worse, the same pattern
as Stage 3's own numbers. But notice the absolute scale: ~860µs/op here
versus ~34µs/op back in Stage 2/3, a roughly 25x jump. `fsync` really is
that expensive — the prediction wasn't wrong about that part.

What it got wrong: buffering's fsync-avoidance benefit only shows up when
writes can be *deferred across operations* without needing to be durable
in between. `Insert` and `Delete` each commit their own transaction right
now, so the journal fsyncs once per distinct page per *operation*
regardless of whether a buffer pool sits above it — there's no batching
happening, because there's no multi-operation transaction for the buffer
pool to batch within. That's not a Stage 3 or Stage 4 problem to fix;
it's precisely what Stage 5 (grouping several `Insert`/`Delete` calls
into one atomic unit) needs to exist for. This is the same shape of
finding as Stage 3's original one: each stage's benchmark reveals what
the *next* stage actually has to deliver, rather than confirming
whatever the current stage's README predicted it would.

### A real bug, and what it says about the test suite

`Delete` originally returned success without writing back a leaf's
now-empty state — reasonable-looking, since the parent was about to drop
its only reference to that page anyway. But dead leaf pages are never
spliced out of the `nextLeaf` chain (see limitations below), so a
surviving sibling could still walk into one during a scan — and read its
stale, pre-deletion bytes instead of zero cells.

The randomized differential test (insert/delete against a reference map)
ran clean the entire time this bug existed. It never fully drained the
tree, so it never forced enough leaves to empty out in sequence to expose
it. A separate, targeted test — delete almost every key in reverse order
— caught it immediately (1 key expected, 23 phantom keys found, each one
exactly the last key a dead leaf held before going empty). Fixed by
persisting the emptied state before signaling collapse, then the
randomized test was rebuilt to always drain to empty and check across
multiple seeds, specifically so this class of bug can't hide behind a
bounded random walk again.

### What Stage 1 deliberately does not handle yet

- **Partial writes from a mid-operation crash.** `AllocatePage` does a
  page write followed by a meta-page write; if the process dies between
  those two, the file and the meta page can disagree. That's a real
  problem with a real, well-known solution — it's exactly what Stage 4's
  write-ahead log exists to fix. Stage 1 only promises correctness in
  the no-crash case.
- **Reuse of freed pages.** There's no delete/free path yet, so
  `AllocatePage` only ever grows the file. The free-list head is already
  reserved in the meta page for when that lands.

### What Stage 2 deliberately does not handle yet

- **Dead leaf pages are never spliced out of the `nextLeaf` chain.** When
  a leaf empties out, its page is abandoned by the tree (parent stops
  referencing it) but a sibling may still point to it. That's harmless
  today — the emptied page correctly reports zero cells and the scan
  just passes through it — but it does mean dead pages accumulate rather
  than being reclaimed. Same underlying fix as the free-list item above.
- **A single oversized value can make a node unsplittable.** Splitting
  divides a node's *cells* in half, not its *bytes*. A pathologically
  large single value (approaching half of `PageSize`) could in principle
  leave one half still overflowing after a split. Real databases solve
  this with overflow pages for large values; out of scope for now.

### What Stage 3 deliberately does not handle yet

- **No write-back on a timer or memory-pressure signal.** Dirty pages sit
  in memory until they're evicted (because the cache is full) or someone
  calls `Flush`/`Close`. On its own, a process that crashes with dirty
  pages still cached loses them. With Stage 4's journal underneath,
  though, this mostly resolves itself: any page that reaches the journal
  — whether via an explicit `Commit` or a mid-transaction eviction — is
  protected. What's still genuinely lost on a crash is only whatever
  belonged to a transaction that never reached `Commit`, which is exactly
  the data that *should* disappear.
- **No read-ahead or write coalescing beyond the cache itself.** Genuinely
  hot pages (like the root) benefit from staying cached, but there's no
  extra batching of writes beyond "don't write until evicted or flushed."

### What Stage 4 deliberately does not handle yet

- **Only single-operation atomicity, not multi-operation transactions.**
  Each `Insert`/`Delete` is its own transaction; there's no public API yet
  to group several calls into one atomic unit. That's explicitly Stage
  5's job.
- **No concurrent-access protection.** Everything so far assumes
  single-threaded, one-operation-at-a-time use from BTree's side.
  `BufferPool`'s mutex protects its own internal state from data races,
  but nothing stops two goroutines from interleaving `Begin`/`Commit`
  calls and breaking the atomicity guarantee between them — that's
  concurrency control, which is also Stage 5's job.
- **A torn write to an actual data page isn't detected**, only a torn
  write to the *journal*. Recovery assumes a single `PageSize` `WriteAt`
  either fully lands or doesn't — true in practice on common filesystems
  and block sizes, but not something POSIX strictly guarantees at the
  byte level. Real per-page checksums in the main file format would close
  this; out of scope here.
- **No directory-entry durability.** Creating/removing the journal file
  relies on ordinary filesystem semantics; the containing directory is
  never explicitly `fsync`'d. Some filesystems technically need that for
  a file's own presence/absence to be crash-durable — a genuinely deep
  rabbit hole, not chased down here.

### What Stage 5 deliberately does not handle yet

- **One coarse lock, not real parallelism.** Every writer is fully
  serialized against every other writer and every reader. Throughput
  under contention is exactly what one lock gives you — correct, safe,
  not fast. Per-page locking or MVCC-style snapshot isolation would allow
  genuinely concurrent writers; unbuilt.
- **`WithTxn`'s rollback guarantee only means something if a `Journal` is
  actually in the stack.** This one is worth being blunt about, because
  it's exactly the kind of thing that's easy to get quietly wrong: `Store`
  makes `Rollback` a no-op on the bare `Pager` (there's nothing deferred
  to undo — every write already landed immediately), so building a
  `*BTree` directly over a `Pager` with no `Journal` gives you working
  concurrency control but *no* real atomicity — an aborted `WithTxn`
  batch's earlier writes simply stay. `openTestTree` (used by most of
  this package's tests) does exactly that, on purpose, since most tests
  don't need the guarantee; anything testing rollback specifically uses
  `openJournaledTestTree` instead. Forgetting this distinction is exactly
  the mistake that produced this section — see `TestWithTxnRollsBackEverythingOnError`'s
  history for how it was caught.
- **No deadlock protection for misuse of `Txn`.** Calling `bt.Insert`
  (which locks `bt.mu` itself) from inside a `WithTxn` closure — instead
  of the `*Txn` handle fn is given — deadlocks, since Go's `sync.Mutex`
  isn't reentrant. `Txn`'s methods exist specifically so correct usage
  never needs to do this, but nothing stops incorrect usage from trying.
- **No timeout or deadlock detection for lock contention in general.** A
  slow or stuck `WithTxn` closure blocks every other goroutine
  indefinitely; there's no `context.Context` plumbed through to allow
  cancellation.

## Usage

```go
import "kiln/pager"

p, err := pager.Open("mydata.db")
if err != nil {
    log.Fatal(err)
}
defer p.Close()

pageNum, err := p.AllocatePage()
if err != nil {
    log.Fatal(err)
}

data := make([]byte, pager.PageSize)
copy(data, []byte("hello"))
if err := p.WritePage(pageNum, data); err != nil {
    log.Fatal(err)
}
```

Higher-level key/value access goes through `btree`, which sits on top of
the same `Pager`:

```go
import (
    "kiln/btree"
    "kiln/pager"
)

p, err := pager.Open("mydata.db")
if err != nil {
    log.Fatal(err)
}
defer p.Close()

bt, err := btree.Open(p)
if err != nil {
    log.Fatal(err)
}

if err := bt.Insert([]byte("hello"), []byte("world")); err != nil {
    log.Fatal(err)
}

val, err := bt.Get([]byte("hello")) // -> "world"

if err := bt.Delete([]byte("hello")); err != nil {
    log.Fatal(err)
}

all, err := bt.All() // every key-value pair, ascending key order
```

Dropping a `BufferPool` underneath is a one-line change — `btree.Open`
takes anything satisfying `pager.Store`, and `*pager.Pager` and
`*bufferpool.BufferPool` both qualify:

```go
import (
    "kiln/btree"
    "kiln/bufferpool"
    "kiln/pager"
)

p, err := pager.Open("mydata.db")
if err != nil {
    log.Fatal(err)
}

bp := bufferpool.New(p, 256) // cache up to 256 pages
defer bp.Close()             // flushes dirty pages, then closes p

bt, err := btree.Open(bp) // <- bp instead of p; nothing else changes
if err != nil {
    log.Fatal(err)
}
```

Adding crash safety is the same pattern again — a `Journal` also
satisfies `pager.Store`, and it's what should sit directly on the raw
`Pager`, with `BufferPool` on top of *that* (see "layering is load-bearing"
above for why that order matters):

```go
import (
    "kiln/btree"
    "kiln/bufferpool"
    "kiln/journal"
    "kiln/pager"
)

p, err := pager.Open("mydata.db")
if err != nil {
    log.Fatal(err)
}

j, err := journal.Open(p, "mydata.db.journal", pager.PageSize)
if err != nil {
    log.Fatal(err) // recovers any journal left over from a previous crash
}

bp := bufferpool.New(j, 256)
defer bp.Close() // flushes dirty pages through the journal, then closes p

bt, err := btree.Open(bp) // full stack: BTree -> BufferPool -> Journal -> Pager
if err != nil {
    log.Fatal(err)
}

// Insert/Delete are now real transactions: if the process is killed
// mid-call, the next Open (or the next Begin) rolls it back entirely.
if err := bt.Insert([]byte("hello"), []byte("world")); err != nil {
    log.Fatal(err)
}
```

Grouping several calls into one atomic unit -- and sharing one `*BTree`
safely across goroutines -- goes through `WithTxn`:

```go
err = bt.WithTxn(func(txn *btree.Txn) error {
    if err := txn.Insert([]byte("a"), []byte("1")); err != nil {
        return err
    }
    if err := txn.Delete([]byte("old-key")); err != nil {
        return err
    }
    return txn.Insert([]byte("b"), []byte("2"))
    // If any call above returns an error, NONE of this batch lands.
})
```

## Testing

```
go test ./... -race -cover
```

Currently: 59 tests total (9 in `pager`, 24 in `btree` including 6
randomized-seed subtests and a concurrency suite, 11 in `bufferpool`, 15
in `journal` including three that spawn a real subprocess and send it a
real `SIGKILL`), clean under `-race`.

- `pager`: 80.3% statement coverage. Uncovered lines are OS-level failure
  branches (`Stat`/`WriteAt`/`ReadAt` erroring mid-call) that need real
  fault injection to exercise honestly.
- `btree`: 86.7% statement coverage. Beyond the splitting/collapse edge
  cases from earlier stages, this now includes a genuine fault-injection
  test (`TestFailedCommitDoesNotLeakPartialState`) built around a small
  `pager.Store` wrapper that fails on a chosen `WritePage` call — the
  test that caught the `txnTouched` bug described above — plus a
  concurrent-access suite verified under `-race`.
- `bufferpool`: 90.1% statement coverage. `TestBTreeOverBufferPool` proves
  transparency; `TestRollbackEvictsDirtyPagesAndDelegates` proves the
  eviction fix directly.
- `journal`: 85.2% statement coverage. Three real-crash tests now:
  single-page (`TestJournalRecoversFromRealCrash`), full single-operation
  stack (`TestBTreeSurvivesRealCrash`), and full multi-operation-batch
  stack (`TestWithTxnSurvivesRealCrash`) — see "testing a crash, for
  real" above.

The randomized differential test (`TestRandomizedAgainstReferenceMap`)
runs 6 fixed seeds, each doing 3,000 random insert/delete ops against a
plain Go map and cross-checking both point lookups and a full ordered
scan every 50 steps, then fully draining the tree and confirming true
emptiness. That drain phase is there on purpose — see "a real bug" above.

Benchmarks, across all three storage-layer configurations:

```
BenchmarkInsert                       33.6 µs/op   40.8 KB/op   337 allocs/op
BenchmarkInsertBuffered                35.7 µs/op   45.2 KB/op   339 allocs/op
BenchmarkGet                          24.5 µs/op   24.2 KB/op   252 allocs/op
BenchmarkGetBuffered                   20.5 µs/op   24.2 KB/op   251 allocs/op
BenchmarkInsertHotKeysUnbuffered       13.4 µs/op   12.2 KB/op   115 allocs/op
BenchmarkInsertHotKeysBuffered         11.6 µs/op   16.3 KB/op   116 allocs/op
BenchmarkInsertJournalOnly            858.0 µs/op   39.8 KB/op   214 allocs/op
BenchmarkInsertJournalAndBufferPool   867.0 µs/op   44.2 KB/op   217 allocs/op
```

Read all of these as findings, not wins — see "the buffer pool's
benchmark story" above for what each one actually revealed, including
where an earlier prediction in this same file turned out to be wrong.

## Project name

"Kiln" is a placeholder — rename freely (just update the `module` line
in `go.mod` and the import path in usage above).
