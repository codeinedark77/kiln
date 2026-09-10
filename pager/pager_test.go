package pager

import (
	"bytes"
	"os"
	"testing"
)

func tempPagerPath(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "kiln-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path) // Open() must create the file itself
	t.Cleanup(func() { os.Remove(path) })
	return path
}

func TestOpenFreshFile(t *testing.T) {
	path := tempPagerPath(t)

	p, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer p.Close()

	if p.PageCount() != 1 {
		t.Errorf("expected fresh file to report 1 page (meta only), got %d", p.PageCount())
	}
}

func TestAllocateAndReadWritePage(t *testing.T) {
	path := tempPagerPath(t)
	p, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer p.Close()

	pageNum, err := p.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	if pageNum != 1 {
		t.Errorf("expected first allocated page to be page 1, got %d", pageNum)
	}

	data := make([]byte, PageSize)
	copy(data, []byte("hello, kiln"))

	if err := p.WritePage(pageNum, data); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	readBack, err := p.ReadPage(pageNum)
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !bytes.Equal(data, readBack) {
		t.Errorf("read back data does not match written data")
	}
}

func TestWriteRejectsWrongSize(t *testing.T) {
	path := tempPagerPath(t)
	p, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer p.Close()

	pageNum, _ := p.AllocatePage()

	err = p.WritePage(pageNum, []byte("too short"))
	if err != ErrBadPageWrite {
		t.Errorf("expected ErrBadPageWrite, got %v", err)
	}
}

func TestReadInvalidPage(t *testing.T) {
	path := tempPagerPath(t)
	p, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer p.Close()

	_, err = p.ReadPage(99)
	if err != ErrInvalidPage {
		t.Errorf("expected ErrInvalidPage, got %v", err)
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	path := tempPagerPath(t)

	p, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	pageNum, err := p.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	data := make([]byte, PageSize)
	copy(data, []byte("persisted data"))
	if err := p.WritePage(pageNum, data); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	p2, err := Open(path)
	if err != nil {
		t.Fatalf("re-Open failed: %v", err)
	}
	defer p2.Close()

	if p2.PageCount() != 2 {
		t.Errorf("expected reopened file to report 2 pages, got %d", p2.PageCount())
	}

	readBack, err := p2.ReadPage(pageNum)
	if err != nil {
		t.Fatalf("ReadPage after reopen failed: %v", err)
	}

	if !bytes.Equal(data, readBack) {
		t.Errorf("data did not persist correctly across reopen")
	}
}

func TestRejectsCorruptOrForeignFile(t *testing.T) {
	path := tempPagerPath(t)

	// Write PageSize bytes of garbage (no valid magic number) before Open ever sees it.
	garbage := bytes.Repeat([]byte{0xFF}, PageSize)
	if err := os.WriteFile(path, garbage, 0644); err != nil {
		t.Fatalf("failed to write garbage file: %v", err)
	}

	_, err := Open(path)
	if err != ErrCorruptMeta {
		t.Errorf("expected ErrCorruptMeta for a foreign file, got %v", err)
	}
}

func TestOpenFailsOnNonexistentDirectory(t *testing.T) {
	_, err := Open("/this/path/does/not/exist/foo.db")
	if err == nil {
		t.Error("expected an error when opening a file inside a nonexistent directory")
	}
}

func TestBeginCommitAreNoOpsOnRawPager(t *testing.T) {
	path := tempPagerPath(t)
	p, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer p.Close()

	if err := p.Begin(); err != nil {
		t.Errorf("Begin() = %v, want nil (no-op on the raw Pager)", err)
	}
	if err := p.Commit(); err != nil {
		t.Errorf("Commit() = %v, want nil (no-op on the raw Pager)", err)
	}
}

func TestAllocateMultiplePages(t *testing.T) {
	path := tempPagerPath(t)
	p, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer p.Close()

	var pageNums []uint32
	for i := 0; i < 10; i++ {
		pn, err := p.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage failed on iteration %d: %v", i, err)
		}
		pageNums = append(pageNums, pn)
	}

	for i, pn := range pageNums {
		if pn != uint32(i+1) {
			t.Errorf("expected page %d to be numbered %d, got %d", i, i+1, pn)
		}
	}

	if p.PageCount() != 11 {
		t.Errorf("expected 11 total pages (1 meta + 10 allocated), got %d", p.PageCount())
	}
}
