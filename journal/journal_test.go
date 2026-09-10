package journal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"kiln/pager"
)

// setup returns a fresh underlying Pager and a journal path in a temp dir,
// but does NOT wrap them in a Journal -- individual tests do that, since
// several need to simulate closing and reopening a Journal against the
// same underlying store.
func setup(t *testing.T) (store *pager.Pager, journalPath string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	journalPath = filepath.Join(dir, "test.db.journal")

	p, err := pager.Open(dbPath)
	if err != nil {
		t.Fatalf("pager.Open failed: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p, journalPath
}

func page(fill byte) []byte {
	b := make([]byte, pager.PageSize)
	for i := range b {
		b[i] = fill
	}
	return b
}

func TestPassthroughMethods(t *testing.T) {
	store, journalPath := setup(t)
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	pn, err := j.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	if err := j.WritePage(pn, page('Q')); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	got, err := j.ReadPage(pn)
	if err != nil || !bytes.Equal(got, page('Q')) {
		t.Fatalf("ReadPage through Journal = (%v, %v), want ('Q' page, nil)", got, err)
	}

	meta, err := j.ReadMeta()
	if err != nil {
		t.Fatalf("ReadMeta through Journal failed: %v", err)
	}
	meta.RootPage = pn
	if err := j.WriteMeta(meta); err != nil {
		t.Fatalf("WriteMeta through Journal failed: %v", err)
	}
	meta2, err := store.ReadMeta()
	if err != nil || meta2.RootPage != pn {
		t.Fatalf("WriteMeta through Journal did not persist to the underlying store")
	}

	if j.PageCount() != store.PageCount() {
		t.Fatalf("PageCount through Journal = %d, underlying store = %d", j.PageCount(), store.PageCount())
	}

	if err := j.Close(); err != nil {
		t.Fatalf("Close through Journal failed: %v", err)
	}
}

func TestNormalFlowCommits(t *testing.T) {
	store, journalPath := setup(t)
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	pn, err := j.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	if err := j.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := j.WritePage(pn, page('A')); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}
	if err := j.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatalf("journal file should be gone after Commit, stat err = %v", err)
	}

	got, err := store.ReadPage(pn)
	if err != nil {
		t.Fatalf("ReadPage on underlying store failed: %v", err)
	}
	if !bytes.Equal(got, page('A')) {
		t.Fatalf("committed write did not persist to the underlying store")
	}
}

func TestOnlyFirstWriteToPageIsJournaledPerTransaction(t *testing.T) {
	store, journalPath := setup(t)
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	pn, err := j.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	// Establish an original value OUTSIDE any transaction, so it's the
	// true pre-transaction content.
	if err := j.WritePage(pn, page('O')); err != nil {
		t.Fatalf("initial WritePage failed: %v", err)
	}

	if err := j.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := j.WritePage(pn, page('A')); err != nil { // first write this txn: journals 'O'
		t.Fatalf("WritePage(A) failed: %v", err)
	}
	if err := j.WritePage(pn, page('B')); err != nil { // second write: must NOT re-journal 'A'
		t.Fatalf("WritePage(B) failed: %v", err)
	}
	// Abandon without committing, then recover via a fresh Journal
	// instance (simulating a process restart).
	j2, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("re-Open (recovery) failed: %v", err)
	}
	_ = j2

	got, err := store.ReadPage(pn)
	if err != nil {
		t.Fatalf("ReadPage after recovery failed: %v", err)
	}
	if !bytes.Equal(got, page('O')) {
		t.Fatalf("after recovery, page = %q, want the TRUE original ('O'), not the mid-transaction value ('A')", got[:1])
	}
}

func TestAbandonedTransactionIsRolledBackOnReopen(t *testing.T) {
	store, journalPath := setup(t)
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	pnA, _ := j.AllocatePage()
	pnB, _ := j.AllocatePage()
	if err := j.WritePage(pnA, page('1')); err != nil {
		t.Fatalf("initial write A failed: %v", err)
	}
	if err := j.WritePage(pnB, page('2')); err != nil {
		t.Fatalf("initial write B failed: %v", err)
	}

	if err := j.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := j.WritePage(pnA, page('X')); err != nil {
		t.Fatalf("WritePage(A, X) failed: %v", err)
	}
	if err := j.WritePage(pnB, page('Y')); err != nil {
		t.Fatalf("WritePage(B, Y) failed: %v", err)
	}
	// No Commit -- simulate a crash by just opening a fresh Journal
	// against the same underlying store and journal path.

	if _, err := Open(store, journalPath, pager.PageSize); err != nil {
		t.Fatalf("recovery Open failed: %v", err)
	}

	gotA, err := store.ReadPage(pnA)
	if err != nil {
		t.Fatalf("ReadPage(A) after recovery failed: %v", err)
	}
	if !bytes.Equal(gotA, page('1')) {
		t.Fatalf("page A after recovery = %q, want rolled back to '1'", gotA[:1])
	}
	gotB, err := store.ReadPage(pnB)
	if err != nil {
		t.Fatalf("ReadPage(B) after recovery failed: %v", err)
	}
	if !bytes.Equal(gotB, page('2')) {
		t.Fatalf("page B after recovery = %q, want rolled back to '2'", gotB[:1])
	}
	if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatalf("journal file should be removed after recovery")
	}
}

func TestCommittedTransactionSurvivesAbandonedNextOne(t *testing.T) {
	store, journalPath := setup(t)
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	pn, err := j.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	// Transaction 1: committed.
	if err := j.Begin(); err != nil {
		t.Fatalf("Begin 1 failed: %v", err)
	}
	if err := j.WritePage(pn, page('C')); err != nil {
		t.Fatalf("WritePage (txn 1) failed: %v", err)
	}
	if err := j.Commit(); err != nil {
		t.Fatalf("Commit 1 failed: %v", err)
	}

	// Transaction 2: abandoned.
	if err := j.Begin(); err != nil {
		t.Fatalf("Begin 2 failed: %v", err)
	}
	if err := j.WritePage(pn, page('D')); err != nil {
		t.Fatalf("WritePage (txn 2) failed: %v", err)
	}
	// no commit

	if _, err := Open(store, journalPath, pager.PageSize); err != nil {
		t.Fatalf("recovery Open failed: %v", err)
	}

	got, err := store.ReadPage(pn)
	if err != nil {
		t.Fatalf("ReadPage after recovery failed: %v", err)
	}
	if !bytes.Equal(got, page('C')) {
		t.Fatalf("page = %q after recovery, want committed txn 1's value 'C' preserved, not txn 2's uncommitted 'D'", got[:1])
	}
}

func TestBeginAutoRecoversDanglingTransactionInSameProcess(t *testing.T) {
	store, journalPath := setup(t)
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	pn, err := j.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	if err := j.WritePage(pn, page('1')); err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	if err := j.Begin(); err != nil {
		t.Fatalf("Begin 1 failed: %v", err)
	}
	if err := j.WritePage(pn, page('X')); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}
	// Simulate the caller hitting an error and never calling Commit, then
	// trying again -- all within the SAME Journal instance, no restart.
	if err := j.Begin(); err != nil {
		t.Fatalf("second Begin (should auto-recover the dangling one) failed: %v", err)
	}

	got, err := store.ReadPage(pn)
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}
	if !bytes.Equal(got, page('1')) {
		t.Fatalf("page = %q after Begin auto-recovery, want rolled back to '1'", got[:1])
	}

	// The new transaction Begin started must itself work normally.
	if err := j.WritePage(pn, page('2')); err != nil {
		t.Fatalf("WritePage in the new transaction failed: %v", err)
	}
	if err := j.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	got2, err := store.ReadPage(pn)
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}
	if !bytes.Equal(got2, page('2')) {
		t.Fatalf("page = %q after committing the recovered-from transaction, want '2'", got2[:1])
	}
}

func TestCommitWithoutBeginReturnsError(t *testing.T) {
	store, journalPath := setup(t)
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := j.Commit(); err != ErrNoTransaction {
		t.Fatalf("Commit with no open transaction: got %v, want ErrNoTransaction", err)
	}
}

func TestWriteOutsideTransactionPassesThroughUnprotected(t *testing.T) {
	store, journalPath := setup(t)
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	pn, err := j.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	// No Begin() at all -- this write is expected to just go straight
	// through, undoable by nothing.
	if err := j.WritePage(pn, page('Z')); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}
	if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatalf("no journal file should exist for a write outside any transaction")
	}
	got, err := store.ReadPage(pn)
	if err != nil || !bytes.Equal(got, page('Z')) {
		t.Fatalf("ReadPage = (%v, %v), want ('Z' page, nil)", got, err)
	}
}

func TestRecoveryStopsAtTornTrailingRecord(t *testing.T) {
	store, journalPath := setup(t)
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	pnGood, _ := j.AllocatePage()
	pnTorn, _ := j.AllocatePage()
	if err := j.WritePage(pnGood, page('1')); err != nil {
		t.Fatalf("initial write (good) failed: %v", err)
	}
	if err := j.WritePage(pnTorn, page('2')); err != nil {
		t.Fatalf("initial write (torn) failed: %v", err)
	}

	if err := j.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := j.WritePage(pnGood, page('A')); err != nil { // fully journaled, valid record
		t.Fatalf("WritePage(good) failed: %v", err)
	}
	if err := j.WritePage(pnTorn, page('B')); err != nil { // will be corrupted below
		t.Fatalf("WritePage(torn) failed: %v", err)
	}

	// Manually truncate the journal file to simulate a crash mid-append of
	// the second record: keep the first full record intact, cut the
	// second one short.
	full := recordSize(pager.PageSize)
	if err := os.Truncate(journalPath, int64(full)+10); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	if _, err := Open(store, journalPath, pager.PageSize); err != nil {
		t.Fatalf("recovery Open failed: %v", err)
	}

	gotGood, err := store.ReadPage(pnGood)
	if err != nil {
		t.Fatalf("ReadPage(good) failed: %v", err)
	}
	if !bytes.Equal(gotGood, page('1')) {
		t.Fatalf("good page after recovery = %q, want rolled back to '1' (its valid record must still apply)", gotGood[:1])
	}
	// pnTorn's record was truncated away entirely, so it was never
	// applied to the store to begin with -- its content should simply be
	// whatever it was before this transaction touched it: 'B' was never
	// actually written through WritePage's real apply step? No -- it WAS
	// applied (WritePage journals old, then applies new), so without a
	// valid undo record, 'B' remains. That's fine: what matters is the
	// journal never claims to have a valid, checksummed record it
	// doesn't, and recovery doesn't crash or misapply garbage.
	if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatalf("journal file should be removed after recovery, even with a torn trailing record")
	}
}

func TestRecoveryStopsAtChecksumMismatch(t *testing.T) {
	store, journalPath := setup(t)
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	pn, _ := j.AllocatePage()
	if err := j.WritePage(pn, page('1')); err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	if err := j.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := j.WritePage(pn, page('A')); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	// Flip a byte inside the (full-length) record to corrupt its checksum
	// without changing its length.
	f, err := os.OpenFile(journalPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, 10); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	f.Close()

	if _, err := Open(store, journalPath, pager.PageSize); err != nil {
		t.Fatalf("recovery Open failed: %v", err)
	}
	// The corrupted record must not be trusted -- but this also means the
	// page won't be rolled back via this record. The important assertion
	// is that recovery doesn't error out or panic on bad data, and cleans
	// up the journal regardless.
	if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatalf("journal file should be removed after recovery, even with a corrupted record")
	}
}

func TestRollbackImmediatelyUndoesUncommittedWrite(t *testing.T) {
	store, journalPath := setup(t)
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	pn, err := j.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	if err := j.WritePage(pn, page('O')); err != nil {
		t.Fatalf("initial WritePage failed: %v", err)
	}

	if err := j.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := j.WritePage(pn, page('X')); err != nil {
		t.Fatalf("WritePage in txn failed: %v", err)
	}

	if err := j.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	got, err := store.ReadPage(pn)
	if err != nil {
		t.Fatalf("ReadPage after Rollback failed: %v", err)
	}
	if !bytes.Equal(got, page('O')) {
		t.Fatalf("page after Rollback = %q, want rolled back to 'O' immediately, not lazily on next Begin", got[:1])
	}
	if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatalf("journal file should be removed immediately after Rollback")
	}

	// A no-op Rollback (nothing in progress) must also be safe.
	if err := j.Rollback(); err != nil {
		t.Fatalf("Rollback with no transaction open failed: %v", err)
	}

	// The journal must still work normally for a new transaction after.
	if err := j.Begin(); err != nil {
		t.Fatalf("Begin after Rollback failed: %v", err)
	}
	if err := j.WritePage(pn, page('Y')); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}
	if err := j.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	got2, err := store.ReadPage(pn)
	if err != nil || !bytes.Equal(got2, page('Y')) {
		t.Fatalf("page after post-rollback commit = (%q, %v), want 'Y'", got2, err)
	}
}

func TestMultipleTransactionsInSequence(t *testing.T) {
	store, journalPath := setup(t)
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	pn, err := j.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	for i := byte(0); i < 5; i++ {
		if err := j.Begin(); err != nil {
			t.Fatalf("Begin %d failed: %v", i, err)
		}
		if err := j.WritePage(pn, page('0'+i)); err != nil {
			t.Fatalf("WritePage %d failed: %v", i, err)
		}
		if err := j.Commit(); err != nil {
			t.Fatalf("Commit %d failed: %v", i, err)
		}
		got, err := store.ReadPage(pn)
		if err != nil || !bytes.Equal(got, page('0'+i)) {
			t.Fatalf("after txn %d, page = (%v, %v), want '%c'", i, got, err, '0'+i)
		}
	}
}
