package journal

// These tests spawn a real subprocess and send it a real SIGKILL -- not a
// simulated crash. That's deliberately more expensive and more work than
// the in-process "abandon a transaction, open a fresh Journal" tests in
// journal_test.go, but it's the only way to be honestly confident that
// recovery works against an actual hard kill: no deferred cleanup runs,
// no goroutine gets a chance to finish a write, nothing in the Go runtime
// gets to react at all.

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"kiln/pager"
)

// TestCrashHelperLowLevel is not a real test. It's invoked as a
// subprocess by TestJournalRecoversFromRealCrash, gated behind an
// environment variable so a normal `go test` run does nothing here.
func TestCrashHelperLowLevel(t *testing.T) {
	dbPath := os.Getenv("KILN_CRASH_DB_PATH")
	journalPath := os.Getenv("KILN_CRASH_JOURNAL_PATH")
	if dbPath == "" {
		t.Skip("not running as a crash-test child")
	}

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

	pn, err := j.AllocatePage()
	if err != nil {
		fmt.Println("CHILD_ERROR: AllocatePage:", err)
		os.Exit(1)
	}
	original := make([]byte, pager.PageSize)
	copy(original, []byte("ORIGINAL-VALUE"))
	if err := j.WritePage(pn, original); err != nil {
		fmt.Println("CHILD_ERROR: initial WritePage:", err)
		os.Exit(1)
	}
	fmt.Printf("PAGE %d\n", pn)

	if err := j.Begin(); err != nil {
		fmt.Println("CHILD_ERROR: Begin:", err)
		os.Exit(1)
	}
	newVal := make([]byte, pager.PageSize)
	copy(newVal, []byte("UNCOMMITTED-VALUE"))
	if err := j.WritePage(pn, newVal); err != nil {
		fmt.Println("CHILD_ERROR: WritePage in txn:", err)
		os.Exit(1)
	}
	fmt.Println("WROTE-UNCOMMITTED")
	// Deliberately never call Commit. Block here; the parent kills us
	// with SIGKILL once it has seen the line above.
	select {}
}

// TestJournalRecoversFromRealCrash spawns TestCrashHelperLowLevel as a
// real subprocess, waits for confirmation that it wrote an uncommitted
// value, sends SIGKILL, then opens a fresh Journal on the same files and
// verifies the page rolled back to its pre-transaction content.
func TestJournalRecoversFromRealCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess and sends SIGKILL; skipped with -short")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crash.db")
	journalPath := filepath.Join(dir, "crash.db.journal")

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashHelperLowLevel")
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

	var pageNum uint32
	sawPage := false
	sawUncommitted := false
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "CHILD_ERROR") {
			cmd.Process.Kill()
			cmd.Wait()
			t.Fatalf("child reported an error: %s", line)
		}
		if strings.HasPrefix(line, "PAGE ") {
			n, err := strconv.ParseUint(strings.TrimPrefix(line, "PAGE "), 10, 32)
			if err != nil {
				t.Fatalf("couldn't parse page number from child output %q: %v", line, err)
			}
			pageNum = uint32(n)
			sawPage = true
		}
		if line == "WROTE-UNCOMMITTED" {
			sawUncommitted = true
			break
		}
	}
	if !sawPage || !sawUncommitted {
		cmd.Wait()
		t.Fatalf("child didn't confirm both the page number and the uncommitted write before ending")
	}

	if err := cmd.Process.Kill(); err != nil { // SIGKILL
		t.Fatalf("failed to kill child: %v", err)
	}
	cmd.Wait() // reap; a kill-induced exit status is expected here

	store, err := pager.Open(dbPath)
	if err != nil {
		t.Fatalf("pager.Open after crash failed: %v", err)
	}
	defer store.Close()

	// This Open call is where recovery happens.
	j, err := Open(store, journalPath, pager.PageSize)
	if err != nil {
		t.Fatalf("journal.Open (recovery) after crash failed: %v", err)
	}

	if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatalf("journal file should be gone after recovery")
	}

	got, err := j.ReadPage(pageNum)
	if err != nil {
		t.Fatalf("ReadPage after recovery failed: %v", err)
	}
	if strings.HasPrefix(string(got), "UNCOMMITTED-VALUE") {
		t.Fatalf("page still shows the UNCOMMITTED value after a real crash -- recovery did not roll it back")
	}
	if !strings.HasPrefix(string(got), "ORIGINAL-VALUE") {
		t.Fatalf("page = %q after recovery, want it to start with ORIGINAL-VALUE", got[:20])
	}
}
