package bufferpool

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"

	"kiln/btree"
	"kiln/pager"
)

func newTestPager(t *testing.T) *pager.Pager {
	t.Helper()
	f, err := os.CreateTemp("", "kiln-bufferpool-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	t.Cleanup(func() { os.Remove(path) })

	p, err := pager.Open(path)
	if err != nil {
		t.Fatalf("pager.Open failed: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func TestReadWriteBasic(t *testing.T) {
	bp := New(newTestPager(t), 16)
	defer bp.Close()

	pn, err := bp.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	data := make([]byte, pager.PageSize)
	copy(data, []byte("hello, buffer pool"))
	if err := bp.WritePage(pn, data); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	got, err := bp.ReadPage(pn)
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("ReadPage returned mismatched data")
	}
}

func TestReadReturnsIndependentCopy(t *testing.T) {
	bp := New(newTestPager(t), 16)
	defer bp.Close()

	pn, err := bp.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	data := make([]byte, pager.PageSize)
	copy(data, []byte("original"))
	if err := bp.WritePage(pn, data); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	got, err := bp.ReadPage(pn)
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}
	// Mutate the caller's copy; this must not corrupt the cache.
	copy(got, make([]byte, pager.PageSize))

	got2, err := bp.ReadPage(pn)
	if err != nil {
		t.Fatalf("second ReadPage failed: %v", err)
	}
	if !bytes.Contains(got2, []byte("original")) {
		t.Fatalf("mutating a previous ReadPage result corrupted the cache")
	}
}

func TestWriteIsCachedBeforeFlush(t *testing.T) {
	underlying := newTestPager(t)
	bp := New(underlying, 16)
	defer bp.Close()

	pageNum, err := bp.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	data := make([]byte, pager.PageSize)
	copy(data, []byte("cached-not-yet-flushed"))
	if err := bp.WritePage(pageNum, data); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	// Bypass the pool and read directly from the underlying store: it
	// must NOT see this write yet.
	raw, err := underlying.ReadPage(pageNum)
	if err != nil {
		t.Fatalf("direct ReadPage on underlying store failed: %v", err)
	}
	if bytes.Contains(raw, []byte("cached-not-yet-flushed")) {
		t.Fatalf("underlying store already has the write before Flush -- pool isn't actually caching")
	}

	// But reading through the pool must see it.
	got, err := bp.ReadPage(pageNum)
	if err != nil {
		t.Fatalf("ReadPage through pool failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("ReadPage through pool did not return the cached write")
	}

	if err := bp.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	raw2, err := underlying.ReadPage(pageNum)
	if err != nil {
		t.Fatalf("direct ReadPage after Flush failed: %v", err)
	}
	if !bytes.Equal(raw2, data) {
		t.Fatalf("underlying store does not reflect the write after Flush")
	}
}

func TestEvictionFlushesDirtyPages(t *testing.T) {
	underlying := newTestPager(t)
	const capacity = 4
	bp := New(underlying, capacity)
	defer bp.Close()

	var pages []uint32
	for i := 0; i < capacity+2; i++ { // 2 more than capacity forces 2 evictions
		pn, err := bp.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage failed: %v", err)
		}
		data := make([]byte, pager.PageSize)
		copy(data, []byte(fmt.Sprintf("page-%d", pn)))
		if err := bp.WritePage(pn, data); err != nil {
			t.Fatalf("WritePage(%d) failed: %v", pn, err)
		}
		pages = append(pages, pn)
	}

	// The first two pages written must have been evicted -- and, being
	// dirty, flushed -- to make room. Check the underlying store directly.
	for i := 0; i < 2; i++ {
		raw, err := underlying.ReadPage(pages[i])
		if err != nil {
			t.Fatalf("direct ReadPage(%d) failed: %v", pages[i], err)
		}
		want := fmt.Sprintf("page-%d", pages[i])
		if !bytes.Contains(raw, []byte(want)) {
			t.Fatalf("evicted page %d was not flushed to the underlying store", pages[i])
		}
	}

	// Everything -- evicted or not -- must still read back correctly
	// through the pool.
	for _, pn := range pages {
		got, err := bp.ReadPage(pn)
		if err != nil {
			t.Fatalf("ReadPage(%d) through pool failed: %v", pn, err)
		}
		want := fmt.Sprintf("page-%d", pn)
		if !bytes.Contains(got, []byte(want)) {
			t.Fatalf("ReadPage(%d) through pool = %q, want to contain %q", pn, got, want)
		}
	}
}

func TestReadProtectsFromEviction(t *testing.T) {
	underlying := newTestPager(t)
	const capacity = 3
	bp := New(underlying, capacity)
	defer bp.Close()

	var pages []uint32
	for i := 0; i < capacity; i++ {
		pn, err := bp.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage failed: %v", err)
		}
		data := make([]byte, pager.PageSize)
		copy(data, []byte(fmt.Sprintf("page-%d", pn)))
		if err := bp.WritePage(pn, data); err != nil {
			t.Fatalf("WritePage failed: %v", err)
		}
		pages = append(pages, pn)
	}
	// Cache is full, LRU order back-to-front is [pages[0], pages[1], pages[2]].
	// Touch pages[0] via a read: it should jump to the front, leaving
	// pages[1] as the next eviction victim instead.
	if _, err := bp.ReadPage(pages[0]); err != nil {
		t.Fatalf("ReadPage(%d) failed: %v", pages[0], err)
	}

	newPage, err := bp.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	data := make([]byte, pager.PageSize)
	copy(data, []byte("fresh"))
	if err := bp.WritePage(newPage, data); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	raw, err := underlying.ReadPage(pages[1])
	if err != nil {
		t.Fatalf("direct ReadPage(%d) failed: %v", pages[1], err)
	}
	want := fmt.Sprintf("page-%d", pages[1])
	if !bytes.Contains(raw, []byte(want)) {
		t.Fatalf("expected pages[1] (%d) to be the one evicted after a read protected pages[0], but it wasn't flushed", pages[1])
	}
}

func TestCapacityClampedToAtLeastOne(t *testing.T) {
	underlying := newTestPager(t)
	bp := New(underlying, 0) // should be clamped to 1
	defer bp.Close()

	pn1, err := bp.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	data1 := make([]byte, pager.PageSize)
	copy(data1, []byte("first"))
	if err := bp.WritePage(pn1, data1); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	pn2, err := bp.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	data2 := make([]byte, pager.PageSize)
	copy(data2, []byte("second"))
	if err := bp.WritePage(pn2, data2); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	raw, err := underlying.ReadPage(pn1)
	if err != nil {
		t.Fatalf("direct ReadPage failed: %v", err)
	}
	if !bytes.Contains(raw, []byte("first")) {
		t.Fatalf("expected pn1 to already be flushed with capacity clamped to 1, but it wasn't")
	}
}

func TestPageCountDelegatesToStore(t *testing.T) {
	underlying := newTestPager(t)
	bp := New(underlying, 16)
	defer bp.Close()

	before := bp.PageCount()
	if _, err := bp.AllocatePage(); err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	after := bp.PageCount()
	if after != before+1 {
		t.Fatalf("PageCount() = %d after one AllocatePage, want %d", after, before+1)
	}
	if after != underlying.PageCount() {
		t.Fatalf("PageCount() = %d, underlying store reports %d -- AllocatePage always passes through, so these must match", after, underlying.PageCount())
	}
}

func TestRollbackEvictsDirtyPagesAndDelegates(t *testing.T) {
	underlying := newTestPager(t)
	bp := New(underlying, 16)
	defer bp.Close()

	pn, err := bp.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	original := make([]byte, pager.PageSize)
	copy(original, []byte("original"))
	if err := bp.Begin(); err != nil {
		t.Fatalf("Begin (baseline) failed: %v", err)
	}
	if err := bp.WritePage(pn, original); err != nil {
		t.Fatalf("initial WritePage failed: %v", err)
	}
	if err := bp.Commit(); err != nil {
		t.Fatalf("Commit (baseline) failed: %v", err)
	}

	if err := bp.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	uncommitted := make([]byte, pager.PageSize)
	copy(uncommitted, []byte("uncommitted"))
	if err := bp.WritePage(pn, uncommitted); err != nil {
		t.Fatalf("WritePage in txn failed: %v", err)
	}

	if err := bp.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// The dirty page must be evicted (not just left dirty), so this read
	// falls through to the underlying store rather than serving the
	// uncommitted cached value.
	got, err := bp.ReadPage(pn)
	if err != nil {
		t.Fatalf("ReadPage after Rollback failed: %v", err)
	}
	if bytes.Contains(got, []byte("uncommitted")) {
		t.Fatalf("ReadPage after Rollback still shows the uncommitted value -- not evicted from cache")
	}
	if !bytes.Contains(got, []byte("original")) {
		t.Fatalf("ReadPage after Rollback = %q, want the original pre-transaction value", got[:20])
	}

	// A no-op Rollback (nothing in progress) must also be safe.
	if err := bp.Rollback(); err != nil {
		t.Fatalf("Rollback with no transaction open failed: %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	bp := New(newTestPager(t), 16)
	defer bp.Close()

	const numPages = 8
	var pageNums []uint32
	for i := 0; i < numPages; i++ {
		pn, err := bp.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage failed: %v", err)
		}
		pageNums = append(pageNums, pn)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 500)
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				pn := pageNums[(id+i)%numPages]
				data := make([]byte, pager.PageSize)
				copy(data, []byte(fmt.Sprintf("g%d-i%d", id, i)))
				if err := bp.WritePage(pn, data); err != nil {
					errCh <- err
					return
				}
				if _, err := bp.ReadPage(pn); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent access error: %v", err)
	}
}

// TestBTreeOverBufferPool re-runs a realistic randomized insert/delete
// workload with BTree sitting on top of a BufferPool instead of a direct
// Pager. The whole point of Stage 3 is that BTree shouldn't need to know
// or care -- this is the test that actually proves it, deliberately using
// a small capacity to force real eviction traffic during the run.
func TestBTreeOverBufferPool(t *testing.T) {
	f, err := os.CreateTemp("", "kiln-bp-integration-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.Remove(path)

	p, err := pager.Open(path)
	if err != nil {
		t.Fatalf("pager.Open failed: %v", err)
	}
	bp := New(p, 32)

	bt, err := btree.Open(bp)
	if err != nil {
		t.Fatalf("btree.Open over BufferPool failed: %v", err)
	}

	reference := make(map[string]string)
	rng := rand.New(rand.NewSource(7))

	const numOps = 2000
	const keySpace = 300
	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("key-%04d", rng.Intn(keySpace))
		if rng.Intn(3) < 2 {
			value := fmt.Sprintf("v-%d", rng.Intn(1_000_000))
			if err := bt.Insert([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Insert(%q) failed at op %d: %v", key, i, err)
			}
			reference[key] = value
		} else {
			_, existed := reference[key]
			err := bt.Delete([]byte(key))
			if existed {
				if err != nil {
					t.Fatalf("Delete(%q) at op %d: expected success, got %v", key, i, err)
				}
				delete(reference, key)
			} else if err != btree.ErrKeyNotFound {
				t.Fatalf("Delete(%q) at op %d: expected ErrKeyNotFound, got %v", key, i, err)
			}
		}
	}

	for k, want := range reference {
		got, err := bt.Get([]byte(k))
		if err != nil || string(got) != want {
			t.Fatalf("Get(%q) = (%q, %v), want (%q, nil)", k, got, err, want)
		}
	}

	// Close (which flushes) and reopen a fresh, unbuffered Pager on the
	// same file. A brand new BTree handle should see exactly the same
	// data, proving the buffer pool actually persists everything rather
	// than just serving a correct-looking in-memory illusion.
	if err := bp.Close(); err != nil {
		t.Fatalf("bp.Close failed: %v", err)
	}

	p2, err := pager.Open(path)
	if err != nil {
		t.Fatalf("re-Open failed: %v", err)
	}
	defer p2.Close()
	bt2, err := btree.Open(p2)
	if err != nil {
		t.Fatalf("btree.Open on reopened file failed: %v", err)
	}

	for k, want := range reference {
		got, err := bt2.Get([]byte(k))
		if err != nil || string(got) != want {
			t.Fatalf("after close+reopen: Get(%q) = (%q, %v), want (%q, nil)", k, got, err, want)
		}
	}

	all, err := bt2.All()
	if err != nil {
		t.Fatalf("All() on reopened tree failed: %v", err)
	}
	if len(all) != len(reference) {
		t.Fatalf("reopened tree has %d entries, want %d", len(all), len(reference))
	}
}

// BenchmarkInsertHotKeysUnbuffered and BenchmarkInsertHotKeysBuffered
// hammer a small, fixed set of keys that all live in the same one or two
// pages -- the scenario where coalescing many writes into one flush
// should matter most, unlike BenchmarkInsert/BenchmarkInsertBuffered
// (sequential unique keys), which mostly touch new pages and barely
// exercise caching at all.
//
// The honest result on this machine: only a modest improvement, not a
// dramatic one. That's because Stage 1 never calls fsync and this
// container's filesystem already makes a WriteAt to the OS page cache
// cheap -- so avoiding the syscall isn't worth much yet. The dominant
// cost right now is Stage 2's own encode()/decode(), which rebuilds/reparses
// the whole page from its cell list on every access; the buffer pool
// caches raw bytes, not decoded nodes, so it can't touch that cost. This
// setup is expected to matter far more once Stage 4 adds real fsync
// calls for durability -- fsync is genuinely expensive, and batching many
// logical writes into one flush before an fsync is the classic reason
// buffer pools exist in real databases.
func BenchmarkInsertHotKeysUnbuffered(b *testing.B) {
	f, err := os.CreateTemp("", "kiln-hotkey-*.db")
	if err != nil {
		b.Fatal(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.Remove(path)

	p, err := pager.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	bt, err := btree.Open(p)
	if err != nil {
		b.Fatal(err)
	}

	const hotKeys = 50
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("hot-%02d", i%hotKeys)
		if err := bt.Insert([]byte(key), []byte(fmt.Sprintf("v-%d", i))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertHotKeysBuffered(b *testing.B) {
	f, err := os.CreateTemp("", "kiln-hotkey-*.db")
	if err != nil {
		b.Fatal(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.Remove(path)

	p, err := pager.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	bp := New(p, 64)
	defer bp.Close()
	bt, err := btree.Open(bp)
	if err != nil {
		b.Fatal(err)
	}

	const hotKeys = 50
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("hot-%02d", i%hotKeys)
		if err := bt.Insert([]byte(key), []byte(fmt.Sprintf("v-%d", i))); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInsertBuffered and BenchmarkGetBuffered mirror btree's
// BenchmarkInsert/BenchmarkGet exactly (same key/value shapes, same
// preload size for Get), the only difference being a BufferPool between
// BTree and the Pager. Comparing the two numbers directly is the point.
func BenchmarkInsertBuffered(b *testing.B) {
	f, err := os.CreateTemp("", "kiln-bufbench-*.db")
	if err != nil {
		b.Fatal(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.Remove(path)

	p, err := pager.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	bp := New(p, 256)
	defer bp.Close()
	bt, err := btree.Open(bp)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%08d", i)
		if err := bt.Insert([]byte(key), []byte("some-benchmark-value")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetBuffered(b *testing.B) {
	f, err := os.CreateTemp("", "kiln-bufbench-*.db")
	if err != nil {
		b.Fatal(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.Remove(path)

	p, err := pager.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	bp := New(p, 256)
	defer bp.Close()
	bt, err := btree.Open(bp)
	if err != nil {
		b.Fatal(err)
	}

	const preload = 20000
	for i := 0; i < preload; i++ {
		key := fmt.Sprintf("key-%08d", i)
		if err := bt.Insert([]byte(key), []byte("some-benchmark-value")); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%08d", i%preload)
		if _, err := bt.Get([]byte(key)); err != nil {
			b.Fatal(err)
		}
	}
}
