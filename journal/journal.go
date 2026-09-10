// Package journal implements Stage 4 of Kiln: crash recovery via a
// rollback journal. Before a page is overwritten for the first time in
// the current transaction, its OLD content is appended to a journal file
// and fsynced. If the process dies before Commit, the journal is replayed
// on the next Open or Begin, restoring every touched page to its
// pre-transaction content -- so a transaction is atomic even across a
// hard crash, not just an in-process error.
//
// This is a *rollback* journal (SQLite's original default mode), not the
// alternative WAL-with-checkpointing design (full pages appended to a
// separate log, readers check the log before the main file, a checkpoint
// periodically folds the log back in). That alternative needs a WAL index
// so readers can find a page's latest version, plus read-path changes
// throughout the stack. A rollback journal gives the same all-or-nothing
// guarantee with a much smaller surface: it only intercepts writes, never
// reads, so BufferPool and BTree above it are completely unaware it
// exists -- see how little btree.go had to change for this stage.
package journal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"

	"kiln/pager"
)

// ErrNoTransaction is returned by Commit if no transaction is open.
var ErrNoTransaction = errors.New("journal: no transaction is open")

const (
	pageNumFieldSize  = 4 // uint32
	checksumFieldSize = 4 // uint32 (crc32)
)

// recordSize is the fixed size of one journal record: a page number, the
// full old page image, and a checksum covering both. Fixed-size records
// make scanning trivial, and let a torn trailing record (from a crash
// mid-append) be detected by a simple length check rather than needing a
// separate commit-marker scheme.
func recordSize(pageSize int) int {
	return pageNumFieldSize + pageSize + checksumFieldSize
}

// Journal wraps a pager.Store, adding Begin/Commit-bracketed crash safety.
// Locking is intentionally out of scope here -- Journal expects to be used
// single-threaded from BTree's perspective (one operation, fully
// Begin/Commit-bracketed, at a time), same as the rest of this stack so far.
type Journal struct {
	store     pager.Store
	path      string
	pageSize  int
	file      *os.File        // non-nil only while a transaction is open
	journaled map[uint32]bool // pages already journaled THIS transaction
}

// Open wraps store with a journal at journalPath, immediately recovering
// (and removing) any journal left over from a previous unclean shutdown
// before returning.
func Open(store pager.Store, journalPath string, pageSize int) (*Journal, error) {
	j := &Journal{store: store, path: journalPath, pageSize: pageSize}
	if err := j.recover(); err != nil {
		return nil, err
	}
	return j, nil
}

// recover reads any existing journal file record by record, restoring
// each page to the OLD content recorded for it, then removes the journal.
// A torn or checksum-mismatched trailing record (from a crash mid-append
// to the journal itself) stops processing at that point rather than
// trusting it -- everything valid before it is still correctly applied.
func (j *Journal) recover() error {
	f, err := os.Open(j.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	recSize := recordSize(j.pageSize)
	buf := make([]byte, recSize)
	for {
		_, err := io.ReadFull(f, buf)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			break // torn trailing record; stop, don't trust it
		}
		if err != nil {
			return err
		}

		pageNum := binary.LittleEndian.Uint32(buf[0:pageNumFieldSize])
		oldData := buf[pageNumFieldSize : pageNumFieldSize+j.pageSize]
		wantSum := binary.LittleEndian.Uint32(buf[pageNumFieldSize+j.pageSize:])
		gotSum := crc32.ChecksumIEEE(buf[0 : pageNumFieldSize+j.pageSize])
		if gotSum != wantSum {
			break // corrupt trailing record; same handling as torn
		}

		restored := make([]byte, j.pageSize)
		copy(restored, oldData)
		if err := j.store.WritePage(pageNum, restored); err != nil {
			return err
		}
	}

	return os.Remove(j.path)
}

// Begin starts a new transaction. If a previous transaction in this same
// process never reached Commit (the caller hit an error and didn't call
// it), Begin recovers it first -- the same way a fresh process restart
// would -- so a single dangling transaction can never block all future
// operations.
func (j *Journal) Begin() error {
	if j.file != nil {
		if err := j.file.Close(); err != nil {
			return err
		}
		j.file = nil
		j.journaled = nil
		if err := j.recover(); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(j.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	j.file = f
	j.journaled = make(map[uint32]bool)
	return nil
}

// WritePage journals pageNum's OLD content the first time it's touched in
// the current transaction (fsynced before anything else happens), then
// applies the new content to the underlying store. Outside a Begin/Commit
// bracket (j.file == nil), it passes straight through unprotected.
func (j *Journal) WritePage(pageNum uint32, data []byte) error {
	if j.file != nil && !j.journaled[pageNum] {
		old, err := j.store.ReadPage(pageNum)
		if err != nil {
			return err
		}
		if err := j.appendRecord(pageNum, old); err != nil {
			return err
		}
		j.journaled[pageNum] = true
	}
	return j.store.WritePage(pageNum, data)
}

func (j *Journal) appendRecord(pageNum uint32, oldData []byte) error {
	buf := make([]byte, recordSize(j.pageSize))
	binary.LittleEndian.PutUint32(buf[0:pageNumFieldSize], pageNum)
	copy(buf[pageNumFieldSize:pageNumFieldSize+j.pageSize], oldData)
	sum := crc32.ChecksumIEEE(buf[0 : pageNumFieldSize+j.pageSize])
	binary.LittleEndian.PutUint32(buf[pageNumFieldSize+j.pageSize:], sum)

	if _, err := j.file.Write(buf); err != nil {
		return err
	}
	// The entire point of a journal is durability before the real write
	// is trusted -- an unsynced journal record protects nothing.
	return j.file.Sync()
}

// Rollback immediately discards the current transaction (if any) by
// rolling back whatever it already wrote to the underlying store. This is
// the same recovery Begin performs when it finds a dangling transaction
// left over from a previous call -- Rollback just triggers it right now
// instead of waiting for the next Begin.
func (j *Journal) Rollback() error {
	if j.file == nil {
		return nil
	}
	if err := j.file.Close(); err != nil {
		return err
	}
	j.file = nil
	j.journaled = nil
	return j.recover()
}

// Commit finalizes the current transaction: closes and deletes the
// journal file, so a future crash has nothing left to roll back.
func (j *Journal) Commit() error {
	if j.file == nil {
		return ErrNoTransaction
	}
	if err := j.file.Close(); err != nil {
		return err
	}
	j.file = nil
	j.journaled = nil
	return os.Remove(j.path)
}

// AllocatePage passes straight through, uncached and unjournaled. A crash
// between an allocation and the Commit of the transaction that requested
// it leaves at worst one leaked page (never referenced by anything, since
// any node that would reference it is itself rolled back) -- not
// corruption. Reclaiming leaked pages is what the free-list (reserved in
// the meta page since Stage 1, still unused) is for.
func (j *Journal) AllocatePage() (uint32, error) {
	return j.store.AllocatePage()
}

// ReadPage, ReadMeta, WriteMeta, PageCount, and Close all pass straight
// through -- this journal only ever intercepts WritePage.
func (j *Journal) ReadPage(pageNum uint32) ([]byte, error) { return j.store.ReadPage(pageNum) }
func (j *Journal) ReadMeta() (*pager.Meta, error)          { return j.store.ReadMeta() }
func (j *Journal) WriteMeta(m *pager.Meta) error           { return j.store.WriteMeta(m) }
func (j *Journal) PageCount() uint32                       { return j.store.PageCount() }
func (j *Journal) Close() error                            { return j.store.Close() }
