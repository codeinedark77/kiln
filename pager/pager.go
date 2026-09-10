// Package pager implements Stage 1 of Kiln: fixed-size page storage
// on top of a single flat file. Everything above this layer (B-tree,
// buffer pool, WAL) will be built in terms of ReadPage/WritePage/AllocatePage.
//
// File layout:
//
//	Page 0            -> reserved meta page (magic, page size, page count,
//	                      B-tree root pointer, free list head)
//	Page 1, 2, 3, ...  -> data pages, PageSize bytes each, no interpretation
//	                      at this layer
//
// What this stage deliberately does NOT handle yet: partial writes from a
// crash mid-AllocatePage, and reuse of freed pages. Both are real problems
// with real solutions (that's what Stage 4's WAL is for) -- Stage 1 only
// promises correctness in the no-crash case.
package pager

import (
	"encoding/binary"
	"errors"
	"os"
)

const (
	// PageSize matches the common OS page size and is the same default
	// modern SQLite uses.
	PageSize = 4096

	metaPageNum = 0
	magicNumber = 0x4B494C4E // arbitrary constant used to detect a foreign/corrupt file
)

var (
	ErrInvalidPage  = errors.New("pager: invalid page number")
	ErrCorruptMeta  = errors.New("pager: corrupt or foreign meta page")
	ErrBadPageWrite = errors.New("pager: page data must be exactly PageSize bytes")
)

// Meta holds the contents of page 0. RootPage and FreeListHead are 0
// (unset) until the B-tree and free-list stages exist above this one.
type Meta struct {
	Magic        uint32
	PageSize     uint32
	PageCount    uint32
	RootPage     uint32
	FreeListHead uint32
}

// Pager manages fixed-size page read/write against a single backing file.
type Pager struct {
	file      *os.File
	pageCount uint32
}

// Open opens (creating if necessary) the database file at path.
// A brand new file is initialized with a fresh meta page; an existing
// file has its meta page validated via the magic number.
func Open(path string) (*Pager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	p := &Pager{file: f}

	if info.Size() == 0 {
		p.pageCount = 1 // page 0 (meta) always exists
		meta := &Meta{
			Magic:     magicNumber,
			PageSize:  PageSize,
			PageCount: 1,
		}
		if err := p.writeMeta(meta); err != nil {
			f.Close()
			return nil, err
		}
		return p, nil
	}

	meta, err := p.readMeta()
	if err != nil {
		f.Close()
		return nil, err
	}
	if meta.Magic != magicNumber {
		f.Close()
		return nil, ErrCorruptMeta
	}
	p.pageCount = meta.PageCount

	return p, nil
}

// Close closes the underlying file.
func (p *Pager) Close() error {
	return p.file.Close()
}

// PageCount reports how many pages (including the meta page) currently exist.
func (p *Pager) PageCount() uint32 {
	return p.pageCount
}

// ReadPage returns the raw PageSize bytes stored at pageNum.
func (p *Pager) ReadPage(pageNum uint32) ([]byte, error) {
	if pageNum >= p.pageCount {
		return nil, ErrInvalidPage
	}
	return p.readRaw(pageNum)
}

// WritePage overwrites the PageSize bytes at pageNum. data must be
// exactly PageSize bytes.
func (p *Pager) WritePage(pageNum uint32, data []byte) error {
	if len(data) != PageSize {
		return ErrBadPageWrite
	}
	if pageNum >= p.pageCount {
		return ErrInvalidPage
	}
	offset := int64(pageNum) * PageSize
	_, err := p.file.WriteAt(data, offset)
	return err
}

// AllocatePage grows the file by one zeroed page and returns its page
// number. Free-list reuse of previously-freed pages arrives in a later
// stage; for now allocation only ever grows the file.
func (p *Pager) AllocatePage() (uint32, error) {
	newPageNum := p.pageCount
	blank := make([]byte, PageSize)
	offset := int64(newPageNum) * PageSize
	if _, err := p.file.WriteAt(blank, offset); err != nil {
		return 0, err
	}
	p.pageCount++

	meta, err := p.readMeta()
	if err != nil {
		return 0, err
	}
	meta.PageCount = p.pageCount
	if err := p.writeMeta(meta); err != nil {
		return 0, err
	}

	return newPageNum, nil
}

// Begin and Commit are no-ops on the raw Pager -- it provides no
// transactional guarantees by itself. A journal.Journal wrapping a Pager
// is what makes these meaningful; see the journal package.
func (p *Pager) Begin() error    { return nil }
func (p *Pager) Commit() error   { return nil }
func (p *Pager) Rollback() error { return nil }

// ReadMeta returns the current contents of the reserved meta page (page 0).
// Higher layers use this to find persistent state that must survive a
// reopen -- e.g. the B-tree's root page number.
func (p *Pager) ReadMeta() (*Meta, error) {
	return p.readMeta()
}

// WriteMeta persists updated metadata, such as a newly-created B-tree root
// pointer, back to page 0.
func (p *Pager) WriteMeta(m *Meta) error {
	return p.writeMeta(m)
}

func (p *Pager) readMeta() (*Meta, error) {
	buf, err := p.readRaw(metaPageNum)
	if err != nil {
		return nil, err
	}
	return &Meta{
		Magic:        binary.LittleEndian.Uint32(buf[0:4]),
		PageSize:     binary.LittleEndian.Uint32(buf[4:8]),
		PageCount:    binary.LittleEndian.Uint32(buf[8:12]),
		RootPage:     binary.LittleEndian.Uint32(buf[12:16]),
		FreeListHead: binary.LittleEndian.Uint32(buf[16:20]),
	}, nil
}

func (p *Pager) writeMeta(m *Meta) error {
	buf := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(buf[0:4], m.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], m.PageSize)
	binary.LittleEndian.PutUint32(buf[8:12], m.PageCount)
	binary.LittleEndian.PutUint32(buf[12:16], m.RootPage)
	binary.LittleEndian.PutUint32(buf[16:20], m.FreeListHead)
	offset := int64(metaPageNum) * PageSize
	_, err := p.file.WriteAt(buf, offset)
	return err
}

// readRaw reads a page without the pageCount bounds check. Needed
// internally because the meta page itself must be read during Open,
// before pageCount has been established.
func (p *Pager) readRaw(pageNum uint32) ([]byte, error) {
	buf := make([]byte, PageSize)
	offset := int64(pageNum) * PageSize
	_, err := p.file.ReadAt(buf, offset)
	return buf, err
}
