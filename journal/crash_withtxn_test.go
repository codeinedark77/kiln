package journal

// Same real-subprocess-plus-SIGKILL methodology as crash_test.go and
// crash_btree_test.go, extended to Stage 5's multi-operation
// transactions specifically: a WithTxn batch is the one place several
// logical writes are meant to land or vanish as a single unit, so it's
// the one place that most needs proof against a real kill, not just the
// in-process fault-injection tests in btree/txn_test.go.

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"kiln/btree"
	"kiln/bufferpool"
	"kiln/pager"
)

// TestCrashHelperWithTxn is not a real test. It's invoked as a subprocess
// by TestWithTxnSurvivesRealCrash. It commits a few small WithTxn batches
// normally, then starts one large batch, confirms progress partway
// through it, and blocks forever without ever returning from the
// closure -- so WithTxn's own Commit is never reached.
func TestCrashHelperWithTxn(t *testing.T) {
	dbPath := os.Getenv("KILN_CRASH_DB_PATH")
	if dbPath == "" {
		t.Skip("not running as a crash-test child")
	}
	journalPath := os.Getenv("KILN_CRASH_JOURNAL_PATH")

	store, err := pager.Open(dbPath)
	if err != nil {
		fmt.Println("CHILD_ERROR: pager.Open:", err)
		os.Exit(1)
	}
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		fmt.Println("CHILD_ERROR: journal.Open:", err)
		os.Exit(1)
	}
	bp := bufferpool.New(j, 16)
	bt, err := btree.Open(bp)
	if err != nil {
		fmt.Println("CHILD_ERROR: btree.Open:", err)
		os.Exit(1)
	}

	// A few small batches that each fully commit -- these must survive.
	for b := 0; b < 3; b++ {
		err := bt.WithTxn(func(txn *btree.Txn) error {
			for i := 0; i < 10; i++ {
				key := fmt.Sprintf("committed-batch%d-%03d", b, i)
				if err := txn.Insert([]byte(key), []byte("v")); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			fmt.Println("CHILD_ERROR: committed batch:", err)
			os.Exit(1)
		}
	}
	fmt.Println("COMMITTED-BATCHES-DONE")

	// One large batch that never reaches Commit: report progress
	// partway through, then block forever so the parent's kill lands
	// mid-transaction, not after some graceful exit.
	bt.WithTxn(func(txn *btree.Txn) error {
		for i := 0; i < 500; i++ {
			key := fmt.Sprintf("uncommitted-%04d", i)
			if err := txn.Insert([]byte(key), []byte("v")); err != nil {
				fmt.Println("CHILD_ERROR: uncommitted batch insert:", err)
				os.Exit(1)
			}
			if i == 250 {
				fmt.Println("HALFWAY-THROUGH-BIG-BATCH")
			}
		}
		select {} // never return; never reach WithTxn's own Commit
	})
}

// TestWithTxnSurvivesRealCrash spawns TestCrashHelperWithTxn, lets it
// commit several small batches and get partway through one large,
// never-committed batch, sends SIGKILL, then reopens through the same
// stack and verifies: every committed batch is fully present, and NOT A
// SINGLE key from the killed, uncommitted batch survived -- not even the
// ones inserted well before the kill signal was sent.
func TestWithTxnSurvivesRealCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess and sends SIGKILL; skipped with -short")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crash.db")
	journalPath := filepath.Join(dir, "crash.db.journal")

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashHelperWithTxn")
	cmd.Env = append(os.Environ(),
		"KILN_CRASH_DB_PATH="+dbPath,
		"KILN_CRASH_JOURNAL_PATH="+journalPath,
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe failed: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start child process: %v", err)
	}

	sawCommitted := false
	sawHalfway := false
	killed := false
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "CHILD_ERROR") {
			cmd.Process.Kill()
			cmd.Wait()
			t.Fatalf("child reported an error: %s", line)
		}
		if line == "COMMITTED-BATCHES-DONE" {
			sawCommitted = true
		}
		if line == "HALFWAY-THROUGH-BIG-BATCH" {
			sawHalfway = true
			if err := cmd.Process.Kill(); err != nil {
				t.Fatalf("failed to kill child: %v", err)
			}
			killed = true
			break
		}
	}
	cmd.Wait()

	if !sawCommitted || !sawHalfway || !killed {
		t.Fatalf("child didn't reach the expected checkpoints (committed=%v halfway=%v killed=%v)", sawCommitted, sawHalfway, killed)
	}

	store, err := pager.Open(dbPath)
	if err != nil {
		t.Fatalf("pager.Open after crash failed: %v", err)
	}
	defer store.Close()
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("journal.Open (recovery) after crash failed: %v", err)
	}
	bp := bufferpool.New(j, 64)
	defer bp.Close()
	bt, err := btree.Open(bp)
	if err != nil {
		t.Fatalf("btree.Open after crash recovery failed: %v", err)
	}

	// Every committed batch's keys must be present.
	for b := 0; b < 3; b++ {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("committed-batch%d-%03d", b, i)
			got, err := bt.Get([]byte(key))
			if err != nil || string(got) != "v" {
				t.Fatalf("Get(%q) (from a committed batch) after crash = (%q, %v), want (\"v\", nil)", key, got, err)
			}
		}
	}

	// NOT ONE key from the killed batch may survive -- including the
	// ones inserted well before the halfway checkpoint, since the whole
	// batch was one uncommitted transaction.
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("uncommitted-%04d", i)
		if _, err := bt.Get([]byte(key)); err != btree.ErrKeyNotFound {
			t.Fatalf("Get(%q) (from the killed, uncommitted batch) after crash: expected ErrKeyNotFound, got %v -- partial batch survived", key, err)
		}
	}

	all, err := bt.All()
	if err != nil {
		t.Fatalf("All() after crash+recovery failed: %v", err)
	}
	if len(all) != 30 {
		t.Fatalf("All() returned %d entries after recovery, want exactly 30 (3 committed batches of 10)", len(all))
	}
	var lastKey []byte
	for _, kv := range all {
		if lastKey != nil && bytes.Compare(kv.Key, lastKey) <= 0 {
			t.Fatalf("All() not strictly ascending after recovery, at key %q", kv.Key)
		}
		lastKey = kv.Key
	}

	// The recovered database must still be fully functional.
	if err := bt.Insert([]byte("post-crash"), []byte("alive")); err != nil {
		t.Fatalf("Insert after crash recovery failed: %v", err)
	}
}
