package btree

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"kiln/bufferpool"
	"kiln/journal"
	"kiln/pager"
)

// failAfterStore wraps a pager.Store and returns a fixed error on the Nth
// WritePage call (1-indexed), letting tests simulate a write failing
// partway through an operation that touches multiple pages. Embedding
// pager.Store gives every other method for free.
type failAfterStore struct {
	pager.Store
	failAfter int
	calls     int
	err       error
}

func (f *failAfterStore) WritePage(pageNum uint32, data []byte) error {
	f.calls++
	if f.calls == f.failAfter {
		return f.err
	}
	return f.Store.WritePage(pageNum, data)
}

// openJournaledTestTree builds the full BTree -> BufferPool -> Journal ->
// Pager stack. Plain openTestTree (used everywhere else in this package)
// wraps a bare Pager, on which Rollback is a no-op -- there's nothing
// deferred to undo, since every write lands immediately. Tests that
// actually need the rollback-on-error guarantee to mean something must
// use this instead.
func openJournaledTestTree(t *testing.T) *BTree {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	journalPath := filepath.Join(dir, "test.db.journal")

	p, err := pager.Open(dbPath)
	if err != nil {
		t.Fatalf("pager.Open failed: %v", err)
	}
	t.Cleanup(func() { p.Close() })

	j, err := journal.Open(p, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("journal.Open failed: %v", err)
	}
	bp := bufferpool.New(j, 64)
	t.Cleanup(func() { bp.Close() })

	bt, err := Open(bp)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	return bt
}

func TestWithTxnCommitsAllOperationsAtomically(t *testing.T) {
	bt := openJournaledTestTree(t)

	if err := bt.Insert([]byte("existing"), []byte("v0")); err != nil {
		t.Fatalf("seed Insert failed: %v", err)
	}

	err := bt.WithTxn(func(txn *Txn) error {
		if err := txn.Insert([]byte("a"), []byte("1")); err != nil {
			return err
		}
		if err := txn.Insert([]byte("b"), []byte("2")); err != nil {
			return err
		}
		return txn.Delete([]byte("existing"))
	})
	if err != nil {
		t.Fatalf("WithTxn failed: %v", err)
	}

	for k, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := bt.Get([]byte(k))
		if err != nil || string(got) != want {
			t.Fatalf("Get(%q) = (%q, %v), want (%q, nil)", k, got, err, want)
		}
	}
	if _, err := bt.Get([]byte("existing")); err != ErrKeyNotFound {
		t.Fatalf("Get(existing) after txn delete: expected ErrKeyNotFound, got %v", err)
	}
}

func TestWithTxnRollsBackEverythingOnError(t *testing.T) {
	bt := openJournaledTestTree(t)

	if err := bt.Insert([]byte("pre-existing"), []byte("v0")); err != nil {
		t.Fatalf("seed Insert failed: %v", err)
	}

	sentinel := errors.New("abort this batch")
	err := bt.WithTxn(func(txn *Txn) error {
		if err := txn.Insert([]byte("first"), []byte("1")); err != nil {
			return err
		}
		if err := txn.Insert([]byte("second"), []byte("2")); err != nil {
			return err
		}
		return sentinel // abort after two successful-looking inserts
	})
	if err != sentinel {
		t.Fatalf("WithTxn error = %v, want the sentinel", err)
	}

	// NEITHER earlier call in the batch should have landed -- that's the
	// entire point of grouping them into one transaction.
	for _, k := range []string{"first", "second"} {
		if _, err := bt.Get([]byte(k)); err != ErrKeyNotFound {
			t.Fatalf("Get(%q) after aborted txn: expected ErrKeyNotFound, got %v", k, err)
		}
	}
	got, err := bt.Get([]byte("pre-existing"))
	if err != nil || string(got) != "v0" {
		t.Fatalf("Get(pre-existing) after aborted txn = (%q, %v), want (\"v0\", nil)", got, err)
	}

	// The tree must still work normally afterward.
	if err := bt.Insert([]byte("post-abort"), []byte("ok")); err != nil {
		t.Fatalf("Insert after an aborted txn failed: %v", err)
	}
}

func TestWithTxnReadYourOwnWrites(t *testing.T) {
	bt := openTestTree(t)

	err := bt.WithTxn(func(txn *Txn) error {
		if err := txn.Insert([]byte("k"), []byte("v1")); err != nil {
			return err
		}
		got, err := txn.Get([]byte("k"))
		if err != nil {
			return err
		}
		if string(got) != "v1" {
			return fmt.Errorf("mid-transaction Get = %q, want %q", got, "v1")
		}
		return txn.Insert([]byte("k"), []byte("v2")) // overwrite within the same txn
	})
	if err != nil {
		t.Fatalf("WithTxn failed: %v", err)
	}

	got, err := bt.Get([]byte("k"))
	if err != nil || string(got) != "v2" {
		t.Fatalf("Get(k) after txn = (%q, %v), want (\"v2\", nil)", got, err)
	}
}

func TestWithTxnPanicStillReleasesLock(t *testing.T) {
	bt := openJournaledTestTree(t)

	func() {
		defer func() { recover() }()
		bt.WithTxn(func(txn *Txn) error {
			txn.Insert([]byte("during-panic"), []byte("v"))
			panic("simulated panic mid-transaction")
		})
	}()

	// If the lock were leaked, this would deadlock the test.
	if err := bt.Insert([]byte("after-panic"), []byte("ok")); err != nil {
		t.Fatalf("Insert after a panicking WithTxn failed: %v", err)
	}
	// The panicking transaction's write must not have landed either --
	// the deferred Rollback runs during panic unwinding same as on a
	// normal error return.
	if _, err := bt.Get([]byte("during-panic")); err != ErrKeyNotFound {
		t.Fatalf("Get(during-panic): expected ErrKeyNotFound (rolled back), got %v", err)
	}
}

// TestFailedCommitDoesNotLeakPartialState is the test that motivated
// BufferPool's txnTouched tracking: a multi-page flush that fails
// partway through (page 1 succeeds and is marked clean, page 2 then
// fails) must not leave page 1's now-committed-looking-but-actually-
// rolled-back content visible in the cache. Checking only "is it dirty"
// would miss exactly this case. A WithTxn batch of several distinct
// single-key inserts gives precise control over how many pages get
// touched -- relying on a natural B-tree split to land the fault exactly
// mid-flush turned out to be unreliable (leaf capacity depends on
// key/value sizes and prior split history, not a number worth
// hard-coding).
func TestFailedCommitDoesNotLeakPartialState(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	journalPath := filepath.Join(dir, "test.db.journal")

	p, err := pager.Open(dbPath)
	if err != nil {
		t.Fatalf("pager.Open failed: %v", err)
	}
	defer p.Close()
	j, err := journal.Open(p, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("journal.Open failed: %v", err)
	}

	injected := errors.New("simulated write failure")
	faulty := &failAfterStore{Store: j, err: injected}
	bp := bufferpool.New(faulty, 256)
	bt, err := Open(bp)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Seed enough sequential keys that the tree has definitely split into
	// several leaves -- with only a handful of keys, everything still
	// lives in one leaf, and 5 inserts into the same page would only ever
	// produce one dirty page (one flush call), never triggering the fault.
	const seedCount = 800
	for i := 0; i < seedCount; i++ {
		key := fmt.Sprintf("key-%05d", i)
		if err := bt.Insert([]byte(key), []byte("v")); err != nil {
			t.Fatalf("seed Insert(%q) failed: %v", key, err)
		}
	}
	if depth := treeDepth(t, bt); depth < 1 {
		t.Fatalf("seeding didn't produce multiple leaves (depth=%d)", depth)
	}

	// A 5-key batch in one WithTxn, spread across the seeded range so
	// each key lands near a different point in the (now-split) keyspace
	// -- likely a different leaf each -- so the commit-time flush makes
	// several separate WritePage calls. Failing on the 3rd guarantees at
	// least 2 succeeded and were marked clean before the failure, which
	// is exactly the scenario under test.
	faulty.calls = 0
	faulty.failAfter = 3
	batchKeys := []string{
		"key-00100-x", "key-00250-x", "key-00400-x", "key-00550-x", "key-00700-x",
	}

	err = bt.WithTxn(func(txn *Txn) error {
		for _, k := range batchKeys {
			if err := txn.Insert([]byte(k), []byte("batchval")); err != nil {
				return err
			}
		}
		return nil
	})
	if err != injected {
		t.Fatalf("expected the injected error from WithTxn, got %v (fault injector saw %d WritePage calls)", err, faulty.calls)
	}
	if faulty.calls < 3 {
		t.Fatalf("fault injector only saw %d WritePage calls; need at least 3 for this test to mean anything", faulty.calls)
	}

	// Immediately -- no time has passed, no separate recovery has run --
	// every seeded key must still be readable and correct, and NONE of
	// the batch's keys must be present, including the ones whose flush
	// individually "succeeded" before the 3rd call failed. This is
	// exactly the window a concurrent reader could have observed if
	// Rollback didn't correctly evict already-flushed-but-still-part-of-
	// this-transaction pages.
	for i := 0; i < seedCount; i += 11 { // sample rather than all 800, for speed
		key := fmt.Sprintf("key-%05d", i)
		got, err := bt.Get([]byte(key))
		if err != nil || string(got) != "v" {
			t.Fatalf("Get(%q) after failed txn = (%q, %v), want (\"v\", nil)", key, got, err)
		}
	}
	for _, k := range batchKeys {
		if _, err := bt.Get([]byte(k)); err != ErrKeyNotFound {
			t.Fatalf("Get(%q) after failed txn: expected ErrKeyNotFound, got %v", k, err)
		}
	}

	all, err := bt.All()
	if err != nil {
		t.Fatalf("All() after failed txn failed: %v", err)
	}
	if len(all) != seedCount {
		t.Fatalf("All() returned %d entries after a failed+rolled-back txn, want exactly the %d seeded ones", len(all), seedCount)
	}

	// The tree must still be fully usable afterward.
	if err := bt.Insert([]byte("post-failure"), []byte("ok")); err != nil {
		t.Fatalf("Insert after a failed-then-rolled-back txn failed: %v", err)
	}
	got, err := bt.Get([]byte("post-failure"))
	if err != nil || string(got) != "ok" {
		t.Fatalf("Get(post-failure) = (%q, %v), want (\"ok\", nil)", got, err)
	}
}

// TestConcurrentAccessIsRaceFree runs many goroutines doing a mix of
// Insert, Get, and WithTxn against one shared *BTree. The point isn't
// performance -- bt.mu fully serializes writers -- it's that sharing one
// BTree across goroutines is actually safe (verified under -race) and
// produces a fully correct final state, which is the concurrency
// guarantee this stage claims.
func TestConcurrentAccessIsRaceFree(t *testing.T) {
	bt := openTestTree(t)

	const goroutines = 12
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*opsPerGoroutine)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("g%02d-k%04d", id, i)
				if err := bt.Insert([]byte(key), []byte("v")); err != nil {
					errCh <- fmt.Errorf("goroutine %d: Insert(%q): %w", id, key, err)
					return
				}
				if _, err := bt.Get([]byte(key)); err != nil {
					errCh <- fmt.Errorf("goroutine %d: Get(%q): %w", id, key, err)
					return
				}
			}
			// One multi-op batch per goroutine too, to exercise WithTxn
			// concurrently with plain Insert/Get from other goroutines.
			err := bt.WithTxn(func(txn *Txn) error {
				for i := 0; i < 5; i++ {
					key := fmt.Sprintf("g%02d-txn%d", id, i)
					if err := txn.Insert([]byte(key), []byte("txv")); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				errCh <- fmt.Errorf("goroutine %d: WithTxn: %w", id, err)
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent access error: %v", err)
	}

	// Every key from every goroutine must be present and correct.
	for g := 0; g < goroutines; g++ {
		for i := 0; i < opsPerGoroutine; i++ {
			key := fmt.Sprintf("g%02d-k%04d", g, i)
			got, err := bt.Get([]byte(key))
			if err != nil || string(got) != "v" {
				t.Fatalf("Get(%q) = (%q, %v), want (\"v\", nil)", key, got, err)
			}
		}
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("g%02d-txn%d", g, i)
			got, err := bt.Get([]byte(key))
			if err != nil || string(got) != "txv" {
				t.Fatalf("Get(%q) = (%q, %v), want (\"txv\", nil)", key, got, err)
			}
		}
	}

	all, err := bt.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	wantCount := goroutines * (opsPerGoroutine + 5)
	if len(all) != wantCount {
		t.Fatalf("All() returned %d entries, want %d", len(all), wantCount)
	}
	var lastKey []byte
	for _, kv := range all {
		if lastKey != nil && bytes.Compare(kv.Key, lastKey) <= 0 {
			t.Fatalf("All() not strictly ascending after concurrent access, at key %q", kv.Key)
		}
		lastKey = kv.Key
	}
}

// TestConcurrentWithTxnIsolation checks that two goroutines each running
// a multi-op WithTxn batch never see each other's partial writes -- one
// goroutine's batch fully completes (or fully rolls back) before the
// other's is allowed to start, because bt.mu is held for the whole
// closure.
func TestConcurrentWithTxnIsolation(t *testing.T) {
	bt := openTestTree(t)

	const batches = 8
	const opsPerBatch = 20

	var wg sync.WaitGroup
	for b := 0; b < batches; b++ {
		wg.Add(1)
		go func(batchID int) {
			defer wg.Done()
			bt.WithTxn(func(txn *Txn) error {
				for i := 0; i < opsPerBatch; i++ {
					key := fmt.Sprintf("batch%02d-%03d", batchID, i)
					if err := txn.Insert([]byte(key), []byte("v")); err != nil {
						return err
					}
					// If another goroutine's batch were interleaved here,
					// a partial count would show up in a concurrent All()
					// scan taken by some other reader; instead of racing
					// that directly (flaky by nature), the real assertion
					// below just checks every batch landed completely.
				}
				return nil
			})
		}(b)
	}
	wg.Wait()

	all, err := bt.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	if len(all) != batches*opsPerBatch {
		t.Fatalf("All() returned %d entries, want %d -- a batch landed partially", len(all), batches*opsPerBatch)
	}
	for b := 0; b < batches; b++ {
		for i := 0; i < opsPerBatch; i++ {
			key := fmt.Sprintf("batch%02d-%03d", b, i)
			if _, err := bt.Get([]byte(key)); err != nil {
				t.Fatalf("Get(%q) failed: %v", key, err)
			}
		}
	}
}
