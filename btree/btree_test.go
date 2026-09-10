package btree

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"kiln/pager"
)

func openTestTree(t *testing.T) *BTree {
	t.Helper()
	f, err := os.CreateTemp("", "kiln-btree-test-*.db")
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

	bt, err := Open(p)
	if err != nil {
		t.Fatalf("btree.Open failed: %v", err)
	}
	return bt
}

func TestEmptyTree(t *testing.T) {
	bt := openTestTree(t)

	if _, err := bt.Get([]byte("anything")); err != ErrKeyNotFound {
		t.Errorf("Get on empty tree: expected ErrKeyNotFound, got %v", err)
	}

	all, err := bt.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("expected empty tree to report 0 entries, got %d", len(all))
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	bt := openTestTree(t)

	if err := bt.Insert(nil, []byte("v")); err != ErrEmptyKey {
		t.Errorf("Insert with empty key: expected ErrEmptyKey, got %v", err)
	}
	if _, err := bt.Get(nil); err != ErrEmptyKey {
		t.Errorf("Get with empty key: expected ErrEmptyKey, got %v", err)
	}
	if err := bt.Delete(nil); err != ErrEmptyKey {
		t.Errorf("Delete with empty key: expected ErrEmptyKey, got %v", err)
	}
}

func TestInsertAndGet(t *testing.T) {
	bt := openTestTree(t)

	if err := bt.Insert([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := bt.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, []byte("world")) {
		t.Errorf("Get(hello) = %q, want %q", got, "world")
	}
}

func TestInsertOverwrite(t *testing.T) {
	bt := openTestTree(t)

	if err := bt.Insert([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := bt.Insert([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("Insert (overwrite) failed: %v", err)
	}

	got, err := bt.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Errorf("Get(k) after overwrite = %q, want %q", got, "v2")
	}

	all, err := bt.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("overwrite should not create a duplicate entry; All() returned %d entries", len(all))
	}
}

func TestManySequentialInserts(t *testing.T) {
	bt := openTestTree(t)

	const n = 5000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		val := fmt.Sprintf("val-%05d", i)
		if err := bt.Insert([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Insert(%q) failed at i=%d: %v", key, i, err)
		}
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		want := fmt.Sprintf("val-%05d", i)
		got, err := bt.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q) failed at i=%d: %v", key, i, err)
		}
		if !bytes.Equal(got, []byte(want)) {
			t.Fatalf("Get(%q) = %q, want %q", key, got, want)
		}
	}

	all, err := bt.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	if len(all) != n {
		t.Fatalf("All() returned %d entries, want %d", len(all), n)
	}
	for i, kv := range all {
		wantKey := fmt.Sprintf("key-%05d", i)
		if string(kv.Key) != wantKey {
			t.Fatalf("All()[%d].Key = %q, want %q (ordering broken)", i, kv.Key, wantKey)
		}
	}
}

// treeDepth walks the leftmost spine from the root and returns how many
// internal-node hops it takes to reach a leaf (0 means the root is itself
// a leaf).
func treeDepth(t *testing.T, bt *BTree) int {
	t.Helper()
	pageNum := bt.rootPage
	depth := 0
	for {
		n, err := bt.readNode(pageNum)
		if err != nil {
			t.Fatalf("treeDepth: readNode failed: %v", err)
		}
		if n.nodeType == LeafNode {
			return depth
		}
		if len(n.internalCells) > 0 {
			pageNum = n.internalCells[0].childPage
		} else {
			pageNum = n.rightChild
		}
		depth++
	}
}

// TestDeepTreeForcesInternalSplit inserts enough keys that an internal node
// -- not just a leaf -- has to split, growing the tree to a third level.
// This exists specifically to cover splitInternal: every other test in this
// file stays within a 2-level tree (root + leaves) and never touches that
// code path. (~45k small sequential keys was empirically checked to reach
// depth 2; ~170 cells fit per leaf and ~240 children fit per internal node
// at this key/value size, so a 3-level tree needs roughly 170*240 keys.)
func TestDeepTreeForcesInternalSplit(t *testing.T) {
	bt := openTestTree(t)

	const n = 45000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		if err := bt.Insert([]byte(key), []byte("v")); err != nil {
			t.Fatalf("Insert failed at i=%d: %v", i, err)
		}
	}

	if depth := treeDepth(t, bt); depth < 2 {
		t.Fatalf("expected at least a 3-level tree (depth >= 2), got depth %d -- splitInternal was never exercised", depth)
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		got, err := bt.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q) failed at i=%d: %v", key, i, err)
		}
		if string(got) != "v" {
			t.Fatalf("Get(%q) = %q, want %q", key, got, "v")
		}
	}

	all, err := bt.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	if len(all) != n {
		t.Fatalf("All() returned %d entries, want %d", len(all), n)
	}
	for i, kv := range all {
		want := fmt.Sprintf("key-%06d", i)
		if string(kv.Key) != want {
			t.Fatalf("All()[%d].Key = %q, want %q", i, kv.Key, want)
		}
	}
}

// TestDeleteCollapsesInternalNode specifically targets the branch where a
// recursive delete finds that a CHILD (itself an internal node, not a
// leaf) has collapsed down to a single grandchild, and must be spliced
// out by repointing rather than removed outright. That only happens in a
// tree with 3+ levels, so it needs the same ~45k-key setup as
// TestDeepTreeForcesInternalSplit, followed by deleting enough to exhaust
// an internal node from its normal ~114-240 keys down to one. Deleting
// from both ends (keeping only a middle slice) exercises both the
// "collapsing child was a specific keyed cell" case and the symmetric
// "collapsing child was the rightChild" case in the same run.
func TestDeleteCollapsesInternalNode(t *testing.T) {
	bt := openTestTree(t)

	const n = 45000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		if err := bt.Insert([]byte(key), []byte("v")); err != nil {
			t.Fatalf("Insert failed at i=%d: %v", i, err)
		}
	}
	if depth := treeDepth(t, bt); depth < 2 {
		t.Fatalf("setup didn't reach a 3-level tree (depth=%d); can't exercise internal-node collapse", depth)
	}

	// Delete large contiguous ranges from BOTH ends, keeping only a middle
	// slice. A prefix-only deletion exhausts left-hand subtrees and only
	// ever exercises the "collapsing child was a specific keyed cell"
	// case; deleting a suffix too is what's needed to exhaust the
	// rightmost subtree and exercise the symmetric "collapsing child was
	// the rightChild" case in the same function.
	const keepFrom, keepTo = 20000, 25000
	for i := 0; i < n; i++ {
		if i >= keepFrom && i < keepTo {
			continue
		}
		key := fmt.Sprintf("key-%06d", i)
		if err := bt.Delete([]byte(key)); err != nil {
			t.Fatalf("Delete(%q) failed at i=%d: %v", key, i, err)
		}
	}

	// Everything inside the kept range must still be present and correct;
	// everything outside it (deleted from both ends) must be gone.
	for i := 0; i < n; i += 7 { // sample rather than check all 45k, for speed
		key := fmt.Sprintf("key-%06d", i)
		got, err := bt.Get([]byte(key))
		if i >= keepFrom && i < keepTo {
			if err != nil || string(got) != "v" {
				t.Fatalf("Get(%q) = (%q, %v), want (\"v\", nil)", key, got, err)
			}
			continue
		}
		if err != ErrKeyNotFound {
			t.Fatalf("Get(%q) after deletion: expected ErrKeyNotFound, got (%q, %v)", key, got, err)
		}
	}

	all, err := bt.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	if len(all) != keepTo-keepFrom {
		t.Fatalf("All() returned %d entries, want %d", len(all), keepTo-keepFrom)
	}
	var lastKey []byte
	for _, kv := range all {
		if lastKey != nil && bytes.Compare(kv.Key, lastKey) <= 0 {
			t.Fatalf("All() not strictly ascending after range delete, at key %q", kv.Key)
		}
		lastKey = kv.Key
	}
}

func TestDeleteBasic(t *testing.T) {
	bt := openTestTree(t)

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		if err := bt.Insert([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("Insert(%q) failed: %v", k, err)
		}
	}

	if err := bt.Delete([]byte("c")); err != nil {
		t.Fatalf("Delete(c) failed: %v", err)
	}

	if _, err := bt.Get([]byte("c")); err != ErrKeyNotFound {
		t.Errorf("Get(c) after delete: expected ErrKeyNotFound, got %v", err)
	}

	for _, k := range []string{"a", "b", "d", "e"} {
		got, err := bt.Get([]byte(k))
		if err != nil {
			t.Errorf("Get(%q) after unrelated delete failed: %v", k, err)
			continue
		}
		if !bytes.Equal(got, []byte("v-"+k)) {
			t.Errorf("Get(%q) = %q, want %q", k, got, "v-"+k)
		}
	}
}

func TestDeleteNotFound(t *testing.T) {
	bt := openTestTree(t)
	if err := bt.Insert([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := bt.Delete([]byte("nonexistent")); err != ErrKeyNotFound {
		t.Errorf("Delete of missing key: expected ErrKeyNotFound, got %v", err)
	}
}

func TestDeleteToEmptyAndReuse(t *testing.T) {
	bt := openTestTree(t)

	if err := bt.Insert([]byte("only"), []byte("key")); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := bt.Delete([]byte("only")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if _, err := bt.Get([]byte("only")); err != ErrKeyNotFound {
		t.Errorf("Get after emptying tree: expected ErrKeyNotFound, got %v", err)
	}
	all, err := bt.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("expected empty tree after deleting the only key, got %d entries", len(all))
	}

	// The tree must still work after being emptied out.
	if err := bt.Insert([]byte("fresh"), []byte("start")); err != nil {
		t.Fatalf("Insert after emptying tree failed: %v", err)
	}
	got, err := bt.Get([]byte("fresh"))
	if err != nil {
		t.Fatalf("Get after re-insert failed: %v", err)
	}
	if !bytes.Equal(got, []byte("start")) {
		t.Errorf("Get(fresh) = %q, want %q", got, "start")
	}
}

func TestDeleteCausesMultiLevelCollapse(t *testing.T) {
	bt := openTestTree(t)

	const n = 3000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		if err := bt.Insert([]byte(key), []byte("v")); err != nil {
			t.Fatalf("Insert(%q) failed: %v", key, err)
		}
	}

	// Delete almost everything, in reverse order, forcing repeated merges
	// and root collapses back toward a single leaf.
	for i := n - 1; i >= 1; i-- {
		key := fmt.Sprintf("key-%05d", i)
		if err := bt.Delete([]byte(key)); err != nil {
			t.Fatalf("Delete(%q) failed at i=%d: %v", key, i, err)
		}
	}

	all, err := bt.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected exactly 1 key remaining, got %d", len(all))
	}
	if string(all[0].Key) != "key-00000" {
		t.Fatalf("remaining key = %q, want %q", all[0].Key, "key-00000")
	}

	got, err := bt.Get([]byte("key-00000"))
	if err != nil || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get(key-00000) = (%q, %v), want (\"v\", nil)", got, err)
	}
}

// TestRandomizedAgainstReferenceMap hammers the tree with a long sequence
// of random inserts/deletes and checks it against a plain Go map at every
// checkpoint -- both by point lookups and by a full ordered scan.
//
// It finishes by draining every remaining key and confirming the tree is
// truly empty. That drain phase is not decorative: an earlier version of
// this test ran a bounded number of random ops and never fully emptied the
// tree, and it completely missed a real bug -- a leaf that empties out left
// its stale pre-deletion bytes on disk, which a surviving sibling's
// nextLeaf pointer would still wander into during a scan. A targeted "delete
// almost everything" test caught it; this version now drains to empty every
// time specifically so a bounded random walk can't get lucky and hide the
// same class of bug again. Multiple seeds guard against one op sequence
// getting lucky in a different way.
func TestRandomizedAgainstReferenceMap(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 42, 1337, 99999} {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			bt := openTestTree(t)
			reference := make(map[string]string)
			rng := rand.New(rand.NewSource(seed))

			const numOps = 3000
			const keySpace = 350

			checkpoint := func(step int) {
				t.Helper()
				for k, want := range reference {
					got, err := bt.Get([]byte(k))
					if err != nil {
						t.Fatalf("[step %d] Get(%q) failed but key should exist: %v", step, k, err)
					}
					if string(got) != want {
						t.Fatalf("[step %d] Get(%q) = %q, want %q", step, k, got, want)
					}
				}

				all, err := bt.All()
				if err != nil {
					t.Fatalf("[step %d] All() failed: %v", step, err)
				}
				if len(all) != len(reference) {
					t.Fatalf("[step %d] All() returned %d entries, reference has %d", step, len(all), len(reference))
				}
				var lastKey []byte
				for _, kv := range all {
					if lastKey != nil && bytes.Compare(kv.Key, lastKey) <= 0 {
						t.Fatalf("[step %d] All() not strictly ascending at key %q", step, kv.Key)
					}
					lastKey = kv.Key
					want, ok := reference[string(kv.Key)]
					if !ok {
						t.Fatalf("[step %d] All() returned unexpected key %q", step, kv.Key)
					}
					if string(kv.Value) != want {
						t.Fatalf("[step %d] All() value for %q = %q, want %q", step, kv.Key, kv.Value, want)
					}
				}
			}

			for i := 0; i < numOps; i++ {
				key := fmt.Sprintf("key-%04d", rng.Intn(keySpace))
				if rng.Intn(3) < 2 {
					value := fmt.Sprintf("v-%d-%d", i, rng.Intn(1_000_000))
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
					} else if err != ErrKeyNotFound {
						t.Fatalf("Delete(%q) at op %d: expected ErrKeyNotFound, got %v", key, i, err)
					}
				}

				if i%50 == 0 {
					checkpoint(i)
				}
			}
			checkpoint(numOps)

			for k := range reference {
				if err := bt.Delete([]byte(k)); err != nil {
					t.Fatalf("final drain: Delete(%q) failed: %v", k, err)
				}
			}
			all, err := bt.All()
			if err != nil {
				t.Fatalf("post-drain All() failed: %v", err)
			}
			if len(all) != 0 {
				t.Fatalf("expected 0 keys after full drain, got %d (e.g. %q)", len(all), all[0].Key)
			}
			if _, err := bt.Get([]byte("key-0000")); err != ErrKeyNotFound {
				t.Fatalf("post-drain Get: expected ErrKeyNotFound, got %v", err)
			}

			// The tree must still work after being fully drained.
			if err := bt.Insert([]byte("post-drain-sentinel"), []byte("ok")); err != nil {
				t.Fatalf("Insert after full drain failed: %v", err)
			}
			got, err := bt.Get([]byte("post-drain-sentinel"))
			if err != nil || !bytes.Equal(got, []byte("ok")) {
				t.Fatalf("Get after post-drain insert = (%q, %v), want (\"ok\", nil)", got, err)
			}
		})
	}
}

func BenchmarkInsert(b *testing.B) {
	f, err := os.CreateTemp("", "kiln-bench-*.db")
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
	bt, err := Open(p)
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

func BenchmarkGet(b *testing.B) {
	f, err := os.CreateTemp("", "kiln-bench-*.db")
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
	bt, err := Open(p)
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
