package journal

// BenchmarkInsertJournalOnly and BenchmarkInsertJournalAndBufferPool exist
// to test a specific prediction made in Stage 3's README: that buffering
// would show a much bigger win once real fsync calls existed. It doesn't.
// See the README's benchmark story for why -- short version: Insert and
// Delete each commit their own transaction, so the journal fsyncs once
// per distinct page per *operation* regardless of whether a buffer pool
// sits above it. Buffering only pays off across operations that share one
// transaction, which doesn't exist until Stage 5 groups multiple calls
// into one atomic unit.

import (
	"fmt"
	"path/filepath"
	"testing"

	"kiln/btree"
	"kiln/bufferpool"
	"kiln/pager"
)

func BenchmarkInsertJournalOnly(b *testing.B) {
	dir := b.TempDir()
	dbPath := filepath.Join(dir, "bench.db")
	journalPath := filepath.Join(dir, "bench.db.journal")

	store, err := pager.Open(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		b.Fatal(err)
	}
	bt, err := btree.Open(j)
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

func BenchmarkInsertJournalAndBufferPool(b *testing.B) {
	dir := b.TempDir()
	dbPath := filepath.Join(dir, "bench.db")
	journalPath := filepath.Join(dir, "bench.db.journal")

	store, err := pager.Open(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		b.Fatal(err)
	}
	bp := bufferpool.New(j, 256)
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
