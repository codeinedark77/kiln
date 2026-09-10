package pager

// Store is the page-level contract that BTree (and any future layer)
// depends on, rather than the concrete Pager. This is what lets a layer
// like a buffer pool sit invisibly between BTree and disk: anything that
// implements Store is a valid backing store, and BTree never needs to
// know which one it has.
//
// *Pager already satisfies this interface as-is -- nothing about Stage 1
// needed to change for Stage 3 to build on top of it.
type Store interface {
	ReadPage(pageNum uint32) ([]byte, error)
	WritePage(pageNum uint32, data []byte) error
	AllocatePage() (uint32, error)
	ReadMeta() (*Meta, error)
	WriteMeta(m *Meta) error
	PageCount() uint32
	Close() error

	// Begin and Commit mark a transaction boundary. A Store with no
	// transactional guarantees of its own (like the raw Pager) can treat
	// both as no-ops; a journal.Journal wrapping one is what makes them
	// meaningful -- see that package.
	Begin() error
	Commit() error

	// Rollback immediately discards the current transaction, if any,
	// rather than leaving it to be cleaned up lazily by the next Begin.
	// This matters for concurrent access: a writer that fails partway
	// through and just released its lock without rolling back could let
	// a concurrent reader observe a partially-applied, uncommitted state.
	Rollback() error
}
