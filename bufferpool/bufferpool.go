// Package bufferpool implements Stage 3 of Kiln: an in-memory LRU cache
// sitting between BTree and the page store, so hot pages don't hit disk
// on every single operation. It satisfies pager.Store itself, so it's a
// drop-in replacement anywhere a *pager.Pager was used directly --
// btree.Open takes a pager.Store and never needs to know which one it has.
package bufferpool

import (
	"container/list"
	"sync"

	"kiln/pager"
)

// frame is one cached page: its bytes, and whether it's been modified
// since it was last written through to the underlying store.
type frame struct {
	pageNum uint32
	data    []byte
	dirty   bool
}

// BufferPool wraps a pager.Store with a fixed-capacity LRU cache. Reads
// and writes are served from memory when possible; a write only marks a
// page dirty; nothing reaches the underlying store until that page is
// evicted or Flush/Close is called.
//
// Locking is a single coarse mutex around the whole pool -- correct and
// simple, at the cost of serializing access to unrelated pages.
// Finer-grained (per-page) locking is a reasonable future refinement, not
// attempted here.
type BufferPool struct {
	mu       sync.Mutex
	store    pager.Store
	capacity int

	lru    *list.List               // front = most recently used, back = next eviction victim
	frames map[uint32]*list.Element // pageNum -> its element in lru

	// txnTouched tracks every page written to since the last Begin,
	// regardless of whether it's still dirty right now. It exists
	// specifically for Rollback: if a multi-page flush partially succeeds
	// before failing (page 1 flushed and marked clean, page 2 then
	// fails), checking "is it dirty" alone would miss page 1 -- it's
	// clean, but its cached content is still part of the failed
	// transaction and must not be served to a later reader. nil outside
	// an open transaction.
	txnTouched map[uint32]bool
}

// New wraps store with an LRU cache holding up to capacity pages.
// capacity is clamped to at least 1.
func New(store pager.Store, capacity int) *BufferPool {
	if capacity < 1 {
		capacity = 1
	}
	return &BufferPool{
		store:    store,
		capacity: capacity,
		lru:      list.New(),
		frames:   make(map[uint32]*list.Element),
	}
}

// ReadPage returns a caller-owned copy of the page (mutating the result
// can never affect the cache), reading through to the underlying store on
// a miss.
func (bp *BufferPool) ReadPage(pageNum uint32) ([]byte, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if elem, ok := bp.frames[pageNum]; ok {
		bp.lru.MoveToFront(elem)
		return cloneBytes(elem.Value.(*frame).data), nil
	}

	data, err := bp.store.ReadPage(pageNum)
	if err != nil {
		return nil, err
	}
	if err := bp.insertLocked(pageNum, cloneBytes(data), false); err != nil {
		return nil, err
	}
	return data, nil
}

// WritePage stores data for pageNum in the cache, marked dirty. Nothing
// reaches the underlying store until this page is evicted or
// Flush/Close is called.
func (bp *BufferPool) WritePage(pageNum uint32, data []byte) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.txnTouched != nil {
		bp.txnTouched[pageNum] = true
	}

	cp := cloneBytes(data)

	if elem, ok := bp.frames[pageNum]; ok {
		bp.lru.MoveToFront(elem)
		f := elem.Value.(*frame)
		f.data = cp
		f.dirty = true
		return nil
	}

	return bp.insertLocked(pageNum, cp, true)
}

// AllocatePage passes straight through to the underlying store. Page
// allocation is rare relative to reads/writes (it only happens on a node
// split), so there's little to gain from buffering it -- and doing so
// would mean also caching meta-page updates, more moving parts for a path
// that was never the bottleneck.
func (bp *BufferPool) AllocatePage() (uint32, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.store.AllocatePage()
}

// ReadMeta and WriteMeta also pass straight through, for the same reason:
// BTree only ever touches the meta page once, at root creation, so
// caching it buys nothing.
func (bp *BufferPool) ReadMeta() (*pager.Meta, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.store.ReadMeta()
}

func (bp *BufferPool) WriteMeta(m *pager.Meta) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.store.WriteMeta(m)
}

// PageCount delegates directly: AllocatePage always passes through, so
// the underlying store's count is authoritative regardless of caching.
func (bp *BufferPool) PageCount() uint32 {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.store.PageCount()
}

// Begin delegates straight through: the buffer pool itself holds no
// transactional state, it just needs the underlying store (e.g. a
// journal.Journal) to know a new transaction has started.
func (bp *BufferPool) Begin() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.txnTouched = make(map[uint32]bool)
	return bp.store.Begin()
}

// Commit flushes every dirty page down to the underlying store -- so a
// wrapped Journal actually sees and protects them -- and only then commits
// the underlying store. Getting this ordering right is the entire reason
// BufferPool can sit above a Journal transparently: if Commit skipped the
// flush, writes still sitting in this cache would never have been
// journaled at all, and a crash would silently lose them.
func (bp *BufferPool) Commit() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if err := bp.flushLocked(); err != nil {
		return err
	}
	bp.txnTouched = nil
	return bp.store.Commit()
}

// Rollback evicts every page touched since the last Begin -- not just
// pages still marked dirty, since a partially-successful flush can leave
// an earlier page clean-but-still-wrong right before a later page fails
// (see the txnTouched doc comment) -- so the next read for any of them
// falls through to the underlying store, which the store's own Rollback
// (e.g. a Journal) is responsible for having put back to its
// pre-transaction content.
func (bp *BufferPool) Rollback() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for pageNum := range bp.txnTouched {
		if elem, ok := bp.frames[pageNum]; ok {
			bp.lru.Remove(elem)
			delete(bp.frames, pageNum)
		}
	}
	bp.txnTouched = nil
	return bp.store.Rollback()
}

// Flush writes every dirty cached page back to the underlying store,
// without evicting anything.
func (bp *BufferPool) Flush() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.flushLocked()
}

// Close flushes every dirty page and then closes the underlying store.
func (bp *BufferPool) Close() error {
	bp.mu.Lock()
	if err := bp.flushLocked(); err != nil {
		bp.mu.Unlock()
		return err
	}
	bp.mu.Unlock()
	return bp.store.Close()
}

func (bp *BufferPool) flushLocked() error {
	for elem := bp.lru.Front(); elem != nil; elem = elem.Next() {
		f := elem.Value.(*frame)
		if f.dirty {
			if err := bp.store.WritePage(f.pageNum, f.data); err != nil {
				return err
			}
			f.dirty = false
		}
	}
	return nil
}

func (bp *BufferPool) insertLocked(pageNum uint32, data []byte, dirty bool) error {
	if bp.lru.Len() >= bp.capacity {
		if err := bp.evictOneLocked(); err != nil {
			return err
		}
	}
	elem := bp.lru.PushFront(&frame{pageNum: pageNum, data: data, dirty: dirty})
	bp.frames[pageNum] = elem
	return nil
}

func (bp *BufferPool) evictOneLocked() error {
	elem := bp.lru.Back()
	if elem == nil {
		return nil
	}
	f := elem.Value.(*frame)
	if f.dirty {
		if err := bp.store.WritePage(f.pageNum, f.data); err != nil {
			return err
		}
	}
	bp.lru.Remove(elem)
	delete(bp.frames, f.pageNum)
	return nil
}

func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
