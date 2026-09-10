package journal

// The test in this file is the one that actually matters most for Stage 4:
// it exercises the full stack a real caller would use (BTree on top of a
// BufferPool on top of a Journal on top of a Pager), does real inserts
// through it, and kills the process for real partway through. Everything
// else in this package builds up to being confident this test means what
// it looks like it means.

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

// TestCrashHelperBTree is not a real test. It's invoked as a subprocess by
// TestBTreeSurvivesRealCrash. It inserts keys one at a time, each its own
// committed transaction, printing a confirmation line after every
// successful commit so the parent knows exactly how many are durable
// before it kills this process.
func TestCrashHelperBTree(t *testing.T) {
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
	bp := bufferpool.New(j, 16) // small on purpose: forces real eviction traffic through the journal during the run
	bt, err := btree.Open(bp)
	if err != nil {
		fmt.Println("CHILD_ERROR: btree.Open:", err)
		os.Exit(1)
	}

	const n = 3000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		val := fmt.Sprintf("val-%05d", i)
		if err := bt.Insert([]byte(key), []byte(val)); err != nil {
			fmt.Println("CHILD_ERROR: Insert:", err)
			os.Exit(1)
		}
		fmt.Printf("OK %d\n", i)
	}
	// If the parent's kill signal never arrives, exit cleanly -- the
	// parent treats "the child finished on its own" as its own (still
	// checkable, just less interesting) outcome.
}

// TestBTreeSurvivesRealCrash spawns TestCrashHelperBTree, lets it commit a
// known number of inserts, sends SIGKILL immediately after the confirmed
// one, then reopens the same files through the full stack and verifies:
// every confirmed-committed key is present and correct, and the tree as a
// whole is still structurally sound (sorted, no corruption) -- proving
// the journal's guarantee holds for real BTree operations, not just raw
// page writes.
func TestBTreeSurvivesRealCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess and sends SIGKILL; skipped with -short")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crash.db")
	journalPath := filepath.Join(dir, "crash.db.journal")

	const killAfter = 400 // kill right after this many confirmed commits

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashHelperBTree")
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

	killed := false
	scanner := bufio.NewScanner(stdout)
	target := fmt.Sprintf("OK %d", killAfter-1)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "CHILD_ERROR") {
			cmd.Process.Kill()
			cmd.Wait()
			t.Fatalf("child reported an error before any kill: %s", line)
		}
		if line == target {
			if err := cmd.Process.Kill(); err != nil { // SIGKILL
				t.Fatalf("failed to kill child: %v", err)
			}
			killed = true
			break
		}
	}
	cmd.Wait()

	if !killed {
		t.Fatalf("child exited before reaching the intended kill point (%s) -- test didn't exercise a real crash", target)
	}

	// Reopen through the exact same stack a real caller would use. This
	// is where journal recovery happens (inside journal.Open).
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

	// Every key the child explicitly confirmed as committed must be
	// present and correct.
	for i := 0; i < killAfter; i++ {
		key := fmt.Sprintf("key-%05d", i)
		want := fmt.Sprintf("val-%05d", i)
		got, err := bt.Get([]byte(key))
		if err != nil {
			t.Fatalf("key %q (confirmed committed by child) missing after crash+recovery: %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("key %q = %q after recovery, want %q", key, got, want)
		}
	}

	// The tree as a whole must still be structurally sound: sorted, no
	// duplicate/garbage entries, every entry independently gettable.
	all, err := bt.All()
	if err != nil {
		t.Fatalf("All() after crash+recovery failed: %v", err)
	}
	if len(all) < killAfter {
		t.Fatalf("All() returned %d entries after recovery, expected at least the %d confirmed-committed keys", len(all), killAfter)
	}
	var lastKey []byte
	seen := make(map[string]bool, len(all))
	for _, kv := range all {
		if lastKey != nil && bytes.Compare(kv.Key, lastKey) <= 0 {
			t.Fatalf("All() not strictly ascending after recovery, at key %q", kv.Key)
		}
		lastKey = kv.Key
		if seen[string(kv.Key)] {
			t.Fatalf("All() returned duplicate key %q after recovery", kv.Key)
		}
		seen[string(kv.Key)] = true

		got, err := bt.Get(kv.Key)
		if err != nil || !bytes.Equal(got, kv.Value) {
			t.Fatalf("All() entry %q=%q doesn't match Get(): (%q, %v)", kv.Key, kv.Value, got, err)
		}
	}

	// One more real operation must work normally -- the recovered tree
	// isn't just readable, it's still a fully functional database.
	if err := bt.Insert([]byte("post-crash-sentinel"), []byte("alive")); err != nil {
		t.Fatalf("Insert after crash recovery failed: %v", err)
	}
	got, err := bt.Get([]byte("post-crash-sentinel"))
	if err != nil || string(got) != "alive" {
		t.Fatalf("post-crash Insert/Get = (%q, %v), want (\"alive\", nil)", got, err)
	}
}
