// Package btree implements Stage 2 of Kiln: a B+tree index built on top of
// the Stage 1 page storage layer. Only leaves hold values; internal nodes
// hold separator keys and child pointers. Leaves are linked left-to-right
// via nextLeaf, which is what All() walks for an ordered full scan.
package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"kiln/pager"
)

type NodeType uint8

const (
	LeafNode NodeType = iota
	InternalNode
)

const (
	headerSize  = 9 // 1 (type) + 2 (numCells) + 2 (cellContentStart) + 4 (extra)
	pointerSize = 2 // one uint16 offset per cell, in the pointer array
)

// ErrNodeOverflow is returned by encode when a node's cells don't fit in a
// single page. Insert's split logic is the only caller expected to see it.
var ErrNodeOverflow = errors.New("btree: node does not fit in a single page")

type leafCell struct {
	key   []byte
	value []byte
}

// internalCell pairs a separator key with the child holding all keys
// strictly less than it. The one child with no preceding key (holding keys
// >= the last separator) lives in the node's rightChild field instead, so
// an internal node with n keys always has exactly n+1 children.
type internalCell struct {
	key       []byte
	childPage uint32
}

type node struct {
	nodeType NodeType

	leafCells []leafCell
	nextLeaf  uint32 // page number of the next leaf in key order; 0 = none

	internalCells []internalCell // sorted ascending by key
	rightChild    uint32         // child for keys >= internalCells[last].key
}

// decodeNode parses a raw page into a node. Layout is a classic slotted
// page: fixed header, a pointer array (grows forward from the header),
// free space, then packed cell data (grows backward from the end of the
// page). See encode for why fragmentation is a non-issue here.
func decodeNode(buf []byte) (*node, error) {
	if len(buf) != pager.PageSize {
		return nil, fmt.Errorf("btree: expected a %d-byte page, got %d", pager.PageSize, len(buf))
	}

	nt := NodeType(buf[0])
	numCells := int(binary.LittleEndian.Uint16(buf[1:3]))
	extra := binary.LittleEndian.Uint32(buf[5:9])

	pointers := make([]uint16, numCells)
	for i := 0; i < numCells; i++ {
		off := headerSize + i*pointerSize
		pointers[i] = binary.LittleEndian.Uint16(buf[off : off+2])
	}

	n := &node{nodeType: nt}

	switch nt {
	case LeafNode:
		n.nextLeaf = extra
		n.leafCells = make([]leafCell, numCells)
		for i, ptr := range pointers {
			pos := int(ptr)
			keyLen := int(binary.LittleEndian.Uint16(buf[pos : pos+2]))
			valLen := int(binary.LittleEndian.Uint16(buf[pos+2 : pos+4]))
			keyStart := pos + 4
			key := append([]byte(nil), buf[keyStart:keyStart+keyLen]...)
			valStart := keyStart + keyLen
			val := append([]byte(nil), buf[valStart:valStart+valLen]...)
			n.leafCells[i] = leafCell{key: key, value: val}
		}
	case InternalNode:
		n.rightChild = extra
		n.internalCells = make([]internalCell, numCells)
		for i, ptr := range pointers {
			pos := int(ptr)
			keyLen := int(binary.LittleEndian.Uint16(buf[pos : pos+2]))
			childPage := binary.LittleEndian.Uint32(buf[pos+2 : pos+6])
			keyStart := pos + 6
			key := append([]byte(nil), buf[keyStart:keyStart+keyLen]...)
			n.internalCells[i] = internalCell{key: key, childPage: childPage}
		}
	default:
		return nil, fmt.Errorf("btree: unknown node type byte %d", nt)
	}

	return n, nil
}

// encode packs the node back into exactly PageSize bytes, rebuilding the
// whole page from the in-memory cell list every time. That sidesteps
// slotted-page fragmentation entirely (no in-place compaction to get
// wrong), at the cost of O(page size) work per write -- a fine trade before
// Stage 3's buffer pool exists to make writes cheap. Returns
// ErrNodeOverflow if the cells don't fit, which Insert uses to trigger a
// split.
func (n *node) encode() ([]byte, error) {
	buf := make([]byte, pager.PageSize)
	buf[0] = byte(n.nodeType)

	var numCells int
	var extra uint32
	cellContentStart := pager.PageSize
	var pointerOffsets []uint16

	fits := func() bool {
		return cellContentStart >= headerSize+numCells*pointerSize
	}

	switch n.nodeType {
	case LeafNode:
		numCells = len(n.leafCells)
		extra = n.nextLeaf
		if !fits() {
			return nil, ErrNodeOverflow
		}
		for _, cell := range n.leafCells {
			cellContentStart -= 4 + len(cell.key) + len(cell.value)
			if !fits() {
				return nil, ErrNodeOverflow
			}
			pos := cellContentStart
			binary.LittleEndian.PutUint16(buf[pos:pos+2], uint16(len(cell.key)))
			binary.LittleEndian.PutUint16(buf[pos+2:pos+4], uint16(len(cell.value)))
			copy(buf[pos+4:pos+4+len(cell.key)], cell.key)
			copy(buf[pos+4+len(cell.key):pos+4+len(cell.key)+len(cell.value)], cell.value)
			pointerOffsets = append(pointerOffsets, uint16(pos))
		}
	case InternalNode:
		numCells = len(n.internalCells)
		extra = n.rightChild
		if !fits() {
			return nil, ErrNodeOverflow
		}
		for _, cell := range n.internalCells {
			cellContentStart -= 6 + len(cell.key)
			if !fits() {
				return nil, ErrNodeOverflow
			}
			pos := cellContentStart
			binary.LittleEndian.PutUint16(buf[pos:pos+2], uint16(len(cell.key)))
			binary.LittleEndian.PutUint32(buf[pos+2:pos+6], cell.childPage)
			copy(buf[pos+6:pos+6+len(cell.key)], cell.key)
			pointerOffsets = append(pointerOffsets, uint16(pos))
		}
	}

	binary.LittleEndian.PutUint16(buf[1:3], uint16(numCells))
	binary.LittleEndian.PutUint16(buf[3:5], uint16(cellContentStart))
	binary.LittleEndian.PutUint32(buf[5:9], extra)

	for i, off := range pointerOffsets {
		p := headerSize + i*pointerSize
		binary.LittleEndian.PutUint16(buf[p:p+2], off)
	}

	return buf, nil
}

// findLeafCell binary-searches a leaf's sorted cells for key. If not found,
// idx is the position at which it would need to be inserted to keep the
// slice sorted.
func (n *node) findLeafCell(key []byte) (idx int, found bool) {
	lo, hi := 0, len(n.leafCells)
	for lo < hi {
		mid := (lo + hi) / 2
		cmp := bytes.Compare(n.leafCells[mid].key, key)
		if cmp == 0 {
			return mid, true
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, false
}

// findChildIndex returns the position of the first separator key strictly
// greater than key; childPageAt of that position is the child to descend
// into for key.
func (n *node) findChildIndex(key []byte) int {
	lo, hi := 0, len(n.internalCells)
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(n.internalCells[mid].key, key) > 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (n *node) childPageAt(i int) uint32 {
	if i < len(n.internalCells) {
		return n.internalCells[i].childPage
	}
	return n.rightChild
}
