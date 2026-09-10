package btree

import (
	"errors"
	"sync"

	"kiln/pager"
)

var (
	ErrKeyNotFound = errors.New("btree: key not found")
	ErrEmptyKey    = errors.New("btree: key must not be empty")
)

// KV is one key-value pair, as returned by All.
type KV struct {
	Key   []byte
	Value []byte
}

// BTree is a B+tree index stored across the pages of a pager.Store. Only
// leaves hold values. It depends on the Store interface rather than the
// concrete Pager specifically so a buffering layer can sit underneath it
// unnoticed -- see pager.Store.
//
// Concurrency: mu is a single coarse read-write lock. Get/All take a read
// lock, so concurrent reads never block each other; Insert, Delete, and
// WithTxn take the full write lock for their entire duration (WithTxn
// holds it across every call its closure makes), so writers are fully
// serialized against both other writers and all readers. That's a real,
// tested concurrency guarantee -- multiple goroutines can safely share
// one *BTree -- just not a maximally parallel one. Per-page locking or
// MVCC would allow more concurrent writers; that's future work, not
// attempted here.
type BTree struct {
	pager    pager.Store
	rootPage uint32
	mu       sync.RWMutex
}

// Open attaches a BTree to an already-open Store: it creates a fresh empty
// root leaf on first use (recorded in the meta page) or reuses the
// existing root pointer otherwise.
func Open(p pager.Store) (*BTree, error) {
	meta, err := p.ReadMeta()
	if err != nil {
		return nil, err
	}

	if meta.RootPage == 0 {
		if err := p.Begin(); err != nil {
			return nil, err
		}
		rootPageNum, err := p.AllocatePage()
		if err != nil {
			return nil, err
		}
		buf, err := (&node{nodeType: LeafNode}).encode()
		if err != nil {
			return nil, err
		}
		if err := p.WritePage(rootPageNum, buf); err != nil {
			return nil, err
		}
		meta.RootPage = rootPageNum
		if err := p.WriteMeta(meta); err != nil {
			return nil, err
		}
		if err := p.Commit(); err != nil {
			return nil, err
		}
	}

	return &BTree{pager: p, rootPage: meta.RootPage}, nil
}

func (bt *BTree) readNode(pageNum uint32) (*node, error) {
	buf, err := bt.pager.ReadPage(pageNum)
	if err != nil {
		return nil, err
	}
	return decodeNode(buf)
}

func (bt *BTree) writeNode(pageNum uint32, n *node) error {
	buf, err := n.encode()
	if err != nil {
		return err
	}
	return bt.pager.WritePage(pageNum, buf)
}

// Get returns the value stored for key, or ErrKeyNotFound.
func (bt *BTree) Get(key []byte) ([]byte, error) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.getLocked(key)
}

func (bt *BTree) getLocked(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	pageNum := bt.rootPage
	for {
		n, err := bt.readNode(pageNum)
		if err != nil {
			return nil, err
		}
		if n.nodeType == LeafNode {
			idx, found := n.findLeafCell(key)
			if !found {
				return nil, ErrKeyNotFound
			}
			return n.leafCells[idx].value, nil
		}
		pageNum = n.childPageAt(n.findChildIndex(key))
	}
}

// All returns every key-value pair in ascending key order, by descending to
// the leftmost leaf and then walking the nextLeaf chain.
func (bt *BTree) All() ([]KV, error) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.allLocked()
}

func (bt *BTree) allLocked() ([]KV, error) {
	pageNum := bt.rootPage
	for {
		n, err := bt.readNode(pageNum)
		if err != nil {
			return nil, err
		}
		if n.nodeType == LeafNode {
			break
		}
		if len(n.internalCells) > 0 {
			pageNum = n.internalCells[0].childPage
		} else {
			pageNum = n.rightChild
		}
	}

	var result []KV
	for pageNum != 0 {
		leaf, err := bt.readNode(pageNum)
		if err != nil {
			return nil, err
		}
		for _, c := range leaf.leafCells {
			result = append(result, KV{Key: c.key, Value: c.value})
		}
		pageNum = leaf.nextLeaf
	}
	return result, nil
}

type insertResult struct {
	split        bool
	separatorKey []byte
	newRightPage uint32
}

// Insert upserts key -> value as its own single-operation transaction:
// either all of it lands -- including any cascading node splits -- or
// (if the process dies before Commit, or this call returns an error) none
// of it does.
func (bt *BTree) Insert(key, value []byte) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if err := bt.pager.Begin(); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			// Best-effort: the original error above is more useful to the
			// caller than a secondary failure here, so it isn't surfaced.
			bt.pager.Rollback()
		}
	}()

	if err := bt.insertLocked(key, value); err != nil {
		return err
	}
	if err := bt.pager.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// insertLocked is Insert's logic without the locking or transaction
// bracketing -- both Insert itself and Txn.Insert (for multi-operation
// transactions) call it after already arranging those separately.
func (bt *BTree) insertLocked(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	res, err := bt.insert(bt.rootPage, key, value)
	if err != nil {
		return err
	}
	if !res.split {
		return nil
	}

	// The root itself overflowed. Its PAGE NUMBER must stay fixed (the
	// pager's meta page points at it), so the old root's content moves to a
	// freshly allocated page, and the original root page becomes a new,
	// one-key internal node pointing at the relocated old root and the new
	// sibling produced by the split.
	oldRootBuf, err := bt.pager.ReadPage(bt.rootPage)
	if err != nil {
		return err
	}
	leftPage, err := bt.pager.AllocatePage()
	if err != nil {
		return err
	}
	if err := bt.pager.WritePage(leftPage, oldRootBuf); err != nil {
		return err
	}

	newRoot := &node{
		nodeType:      InternalNode,
		internalCells: []internalCell{{key: res.separatorKey, childPage: leftPage}},
		rightChild:    res.newRightPage,
	}
	return bt.writeNode(bt.rootPage, newRoot)
}

func (bt *BTree) insert(pageNum uint32, key, value []byte) (*insertResult, error) {
	n, err := bt.readNode(pageNum)
	if err != nil {
		return nil, err
	}

	if n.nodeType == LeafNode {
		idx, found := n.findLeafCell(key)
		if found {
			n.leafCells[idx].value = value
		} else {
			n.leafCells = append(n.leafCells, leafCell{})
			copy(n.leafCells[idx+1:], n.leafCells[idx:])
			n.leafCells[idx] = leafCell{key: key, value: value}
		}

		if err := bt.writeNode(pageNum, n); err == ErrNodeOverflow {
			return bt.splitLeaf(pageNum, n)
		} else if err != nil {
			return nil, err
		}
		return &insertResult{}, nil
	}

	childIdx := n.findChildIndex(key)
	childPage := n.childPageAt(childIdx)
	res, err := bt.insert(childPage, key, value)
	if err != nil {
		return nil, err
	}
	if !res.split {
		return &insertResult{}, nil
	}

	newCell := internalCell{key: res.separatorKey, childPage: childPage}
	if childIdx == len(n.internalCells) {
		n.internalCells = append(n.internalCells, newCell)
		n.rightChild = res.newRightPage
	} else {
		n.internalCells = append(n.internalCells, internalCell{})
		copy(n.internalCells[childIdx+1:], n.internalCells[childIdx:])
		n.internalCells[childIdx] = newCell
		n.internalCells[childIdx+1].childPage = res.newRightPage
	}

	if err := bt.writeNode(pageNum, n); err == ErrNodeOverflow {
		return bt.splitInternal(pageNum, n)
	} else if err != nil {
		return nil, err
	}
	return &insertResult{}, nil
}

func (bt *BTree) splitLeaf(pageNum uint32, n *node) (*insertResult, error) {
	mid := len(n.leafCells) / 2
	leftCells := append([]leafCell(nil), n.leafCells[:mid]...)
	rightCells := append([]leafCell(nil), n.leafCells[mid:]...)

	newPageNum, err := bt.pager.AllocatePage()
	if err != nil {
		return nil, err
	}

	right := &node{nodeType: LeafNode, leafCells: rightCells, nextLeaf: n.nextLeaf}
	left := &node{nodeType: LeafNode, leafCells: leftCells, nextLeaf: newPageNum}

	if err := bt.writeNode(pageNum, left); err != nil {
		return nil, err
	}
	if err := bt.writeNode(newPageNum, right); err != nil {
		return nil, err
	}

	return &insertResult{split: true, separatorKey: rightCells[0].key, newRightPage: newPageNum}, nil
}

func (bt *BTree) splitInternal(pageNum uint32, n *node) (*insertResult, error) {
	mid := len(n.internalCells) / 2
	separatorKey := n.internalCells[mid].key

	leftCells := append([]internalCell(nil), n.internalCells[:mid]...)
	rightCells := append([]internalCell(nil), n.internalCells[mid+1:]...)

	newPageNum, err := bt.pager.AllocatePage()
	if err != nil {
		return nil, err
	}

	left := &node{nodeType: InternalNode, internalCells: leftCells, rightChild: n.internalCells[mid].childPage}
	right := &node{nodeType: InternalNode, internalCells: rightCells, rightChild: n.rightChild}

	if err := bt.writeNode(pageNum, left); err != nil {
		return nil, err
	}
	if err := bt.writeNode(newPageNum, right); err != nil {
		return nil, err
	}

	return &insertResult{split: true, separatorKey: separatorKey, newRightPage: newPageNum}, nil
}

// deleteResult communicates rebalancing information back up the recursion.
type deleteResult struct {
	deleted     bool
	collapsed   bool
	replacement uint32 // 0 = no replacement, just remove the entry entirely
}

// Delete removes key. Returns ErrKeyNotFound if it wasn't present. Like
// Insert, the whole operation -- including any cascading merges/collapses
// -- is its own single-operation transaction.
func (bt *BTree) Delete(key []byte) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if err := bt.pager.Begin(); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			bt.pager.Rollback()
		}
	}()

	err := bt.deleteLocked(key)
	if err == ErrKeyNotFound {
		// Nothing was written for this single, now-auto-committed
		// operation -- close out the harmless empty transaction rather
		// than leaving a no-op journal dangling until the next Begin.
		if cerr := bt.pager.Commit(); cerr != nil {
			return cerr
		}
		committed = true
		return ErrKeyNotFound
	}
	if err != nil {
		return err
	}
	if err := bt.pager.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// deleteLocked is Delete's logic without the locking, transaction
// bracketing, or not-found bookkeeping -- both Delete itself and
// Txn.Delete (for multi-operation transactions) call it after already
// arranging those separately. Unlike Delete, it returns ErrKeyNotFound to
// the caller uninterpreted, since a Txn may want to handle that itself
// without aborting the whole batch.
func (bt *BTree) deleteLocked(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	res, err := bt.delete(bt.rootPage, key)
	if err != nil {
		return err
	}
	if !res.deleted {
		return ErrKeyNotFound
	}
	if !res.collapsed {
		return nil
	}

	if res.replacement != 0 {
		// Root was a pointless single-child pass-through; splice it out by
		// copying its one remaining child's content into the fixed root page.
		buf, err := bt.pager.ReadPage(res.replacement)
		if err != nil {
			return err
		}
		return bt.pager.WritePage(bt.rootPage, buf)
	}

	// Root's last key was just removed: the tree is empty. Root's page
	// number stays fixed; make it a fresh empty leaf.
	buf, err := (&node{nodeType: LeafNode}).encode()
	if err != nil {
		return err
	}
	return bt.pager.WritePage(bt.rootPage, buf)
}

func (bt *BTree) delete(pageNum uint32, key []byte) (*deleteResult, error) {
	n, err := bt.readNode(pageNum)
	if err != nil {
		return nil, err
	}

	if n.nodeType == LeafNode {
		idx, found := n.findLeafCell(key)
		if !found {
			return &deleteResult{}, nil
		}
		n.leafCells = append(n.leafCells[:idx], n.leafCells[idx+1:]...)

		if len(n.leafCells) == 0 {
			// Nothing left here as far as the TREE is concerned -- the
			// parent drops its reference below. But we don't splice dead
			// pages out of the leaf chain (see README), so a sibling's
			// nextLeaf can still wander in here. Persist the now-empty
			// state (nextLeaf preserved) so that traversal finds 0 cells
			// and moves on, instead of the stale pre-deletion bytes.
			if err := bt.writeNode(pageNum, n); err != nil {
				return nil, err
			}
			return &deleteResult{deleted: true, collapsed: true}, nil
		}
		if err := bt.writeNode(pageNum, n); err != nil {
			return nil, err
		}
		return &deleteResult{deleted: true}, nil
	}

	childIdx := n.findChildIndex(key)
	childPage := n.childPageAt(childIdx)
	res, err := bt.delete(childPage, key)
	if err != nil {
		return nil, err
	}
	if !res.deleted {
		return &deleteResult{}, nil
	}
	if !res.collapsed {
		return &deleteResult{deleted: true}, nil
	}

	if res.replacement != 0 {
		// Child spliced down to one grandchild -- repoint to it. This
		// node's own key structure doesn't change.
		if childIdx == len(n.internalCells) {
			n.rightChild = res.replacement
		} else {
			n.internalCells[childIdx].childPage = res.replacement
		}
		if err := bt.writeNode(pageNum, n); err != nil {
			return nil, err
		}
		return &deleteResult{deleted: true}, nil
	}

	// Child became completely empty: remove the reference to it entirely.
	if childIdx == len(n.internalCells) {
		if len(n.internalCells) == 0 {
			// Defensive only: a persisted internal node should never have
			// zero keys in practice (we splice those out below before they
			// ever hit disk). If it somehow happens, propagate the
			// collapse rather than index out of range.
			return &deleteResult{deleted: true, collapsed: true}, nil
		}
		last := n.internalCells[len(n.internalCells)-1]
		n.rightChild = last.childPage
		n.internalCells = n.internalCells[:len(n.internalCells)-1]
	} else {
		n.internalCells = append(n.internalCells[:childIdx], n.internalCells[childIdx+1:]...)
	}

	if len(n.internalCells) == 0 {
		// Down to a single child: this node is now a pointless
		// pass-through. Splice it out by reporting the remaining child as
		// our replacement -- so a zero-key internal node is never written.
		return &deleteResult{deleted: true, collapsed: true, replacement: n.rightChild}, nil
	}

	if err := bt.writeNode(pageNum, n); err != nil {
		return nil, err
	}
	return &deleteResult{deleted: true}, nil
}

// Txn is a handle for making multiple Insert/Delete/Get calls as one
// atomic unit. It's only valid for the duration of the WithTxn call that
// creates it -- don't retain one past its closure returning.
type Txn struct {
	bt *BTree
}

// Insert behaves like BTree.Insert, but as part of the enclosing
// transaction rather than committing on its own.
func (t *Txn) Insert(key, value []byte) error {
	return t.bt.insertLocked(key, value)
}

// Delete behaves like BTree.Delete, but as part of the enclosing
// transaction rather than committing on its own.
func (t *Txn) Delete(key []byte) error {
	return t.bt.deleteLocked(key)
}

// Get reads within the enclosing transaction. Since WithTxn holds the
// full write lock for its whole duration, this is just a normal read
// against whatever this transaction has written so far.
func (t *Txn) Get(key []byte) ([]byte, error) {
	return t.bt.getLocked(key)
}

// All reads within the enclosing transaction, same caveat as Get.
func (t *Txn) All() ([]KV, error) {
	return t.bt.allLocked()
}

// WithTxn runs fn as a single transaction spanning every Insert/Delete
// call fn makes through the Txn it's given. If fn returns an error (or
// panics -- Go re-panics after the deferred rollback runs), the whole
// transaction is rolled back immediately and none of it lands; otherwise
// every call fn made commits together, atomically, as one unit.
//
// The write lock is held for fn's entire execution, so other Insert,
// Delete, and WithTxn calls -- and even Get/All -- block until fn
// returns. That's what makes the batch atomic and isolated from other
// goroutines, at the cost of blocking them for as long as fn takes to run.
func (bt *BTree) WithTxn(fn func(*Txn) error) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if err := bt.pager.Begin(); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			bt.pager.Rollback()
		}
	}()

	if err := fn(&Txn{bt: bt}); err != nil {
		return err
	}
	if err := bt.pager.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}
