package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const HEADER_SIZE uint16 = 4
const PAGE_SIZE = 4096
const MAX_KEY_SIZE = 1000
const MAX_VAL_SIZE = 3000

const (
	INTERNAL = 1
	LEAF     = 2
)

type BNode []byte

type BTree struct {
	root uint64
	get  func(uint64) []byte
	new  func([]byte) uint64
	del  func(uint64)
}

func (node BNode) nodeType() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) keyCount() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(nodeType uint16, keyCount uint16) {
	binary.LittleEndian.PutUint16(node[0:2], nodeType)
	binary.LittleEndian.PutUint16(node[2:4], keyCount)
}

func (node BNode) getPointer(index uint16) (uint64, error) {
	if node.nodeType() != INTERNAL {
		return 0, errors.New("Trying to retrieve a pointer from a non-internal node.")
	}
	if index >= node.keyCount() {
		return 0, errors.New("Trying to retrieve pointer for out-of-bounds index.")
	}
	position := HEADER_SIZE + index*8
	return binary.LittleEndian.Uint64(node[position : position+8]), nil
}

func (node BNode) setPointer(index uint16, pointer uint64) error {
	if node.nodeType() != INTERNAL {
		return errors.New("Trying to set a pointer from a non-internal node.")
	}
	if index >= node.keyCount() {
		return errors.New("Trying to set pointer for out-of-bounds index.")
	}
	position := HEADER_SIZE + index*8
	binary.LittleEndian.PutUint64(node[position:position+8], pointer)
	return nil
}

func (node BNode) pointerBytes() uint16 {
	t := node.nodeType()
	switch t {
	case INTERNAL:
		return 8 * node.keyCount()
	case LEAF:
		return 0
	default:
		panic(fmt.Errorf("Unknown node type: %d", t))
	}
}

func (node BNode) getOffset(index uint16) (uint16, error) {
	if index > node.keyCount() {
		return 0, errors.New("Trying to retrieve offset for out-of-bounds index.")
	}
	if index == 0 {
		return 0, nil
	}
	position := HEADER_SIZE + node.pointerBytes() + 2*(index-1)
	return binary.LittleEndian.Uint16(node[position : position+2]), nil
}

func (node BNode) setOffset(index uint16, offset uint16) error {
	if index == 0 {
		return errors.New("Trying to set offset for index zero, which is always zero.")
	}
	if index > node.keyCount() {
		return errors.New("Trying to set offset for out-of-bounds index.")
	}
	position := HEADER_SIZE + node.pointerBytes() + 2*(index-1)
	binary.LittleEndian.PutUint16(node[position:position+2], offset)
	return nil
}

func (node BNode) getKeyValuePosition(index uint16) (uint16, error) {
	offset, err := node.getOffset(index)
	if err != nil {
		return 0, err
	}
	position := HEADER_SIZE + node.pointerBytes() + 2*node.keyCount() + offset
	return position, nil
}

func (node BNode) headerKeyValue() uint16 {
	t := node.nodeType()
	switch t {
	case INTERNAL:
		return 2
	case LEAF:
		return 4
	default:
		panic(fmt.Errorf("Unknown node type: %d", t))
	}
}

func (node BNode) getKey(index uint16) ([]byte, error) {
	if index >= node.keyCount() {
		return nil, errors.New("Trying to retrieve key for out-of-bounds index.")
	}
	position, err := node.getKeyValuePosition(index)
	if err != nil {
		return nil, err
	}
	keyLength := binary.LittleEndian.Uint16(node[position : position+2])

	keyPosition := position + node.headerKeyValue()
	return node[keyPosition : keyPosition+keyLength], nil
}

func (node BNode) getValue(index uint16) ([]byte, error) {
	if node.nodeType() != LEAF {
		return nil, errors.New("Trying to retrieve value from non-leaf node.")
	}
	if index >= node.keyCount() {
		return nil, errors.New("Trying to retrieve value for out-of-bounds index.")
	}
	position, err := node.getKeyValuePosition(index)
	if err != nil {
		return nil, err
	}
	keyLength := binary.LittleEndian.Uint16(node[position : position+2])
	valueLength := binary.LittleEndian.Uint16(node[position+2 : position+4])

	valuePosition := position + node.headerKeyValue() + keyLength
	return node[valuePosition : valuePosition+valueLength], nil
}

func (node BNode) totalSize() uint16 {
	count := node.keyCount()
	afterLastPosition, err := node.getKeyValuePosition(count)
	if err != nil {
		panic("Unreachable, because last position should always be in range")
	}
	return HEADER_SIZE + node.pointerBytes() + 2*count + afterLastPosition
}

func (node BNode) find(key []byte) (uint16, bool) {
	count := node.keyCount()
	if count <= 0 {
		panic("Node should never be empty.")
	}
	end := count
	var start uint16 = 0

	firstKey, err := node.getKey(0)
	if err != nil {
		panic("Key at index zero should always exist in non-empty node.")
	}
	if bytes.Compare(firstKey, key) > 0 {
		panic("First key is a copy from the parent node, and should therefore always be less or equal to the key.")
	}

	for end-start > 1 {
		middle := (start + end) / 2
		middleKey, err := node.getKey(middle)
		if err != nil {
			panic(fmt.Errorf("Unexpected error, because in search '%d < %d' should always hold.", middle, count))
		}
		cmp := bytes.Compare(middleKey, key)
		if cmp == 0 {
			return middle, true
		} else if cmp < 0 {
			start = middle
		} else {
			end = middle
		}
	}
	return start, false
}

func (node BNode) sections() ([]byte, []byte, []byte) {
	pointersStart := HEADER_SIZE
	offsetsStart := pointersStart + node.pointerBytes()
	keyValueStart := offsetsStart + 2*node.keyCount()
	end := node.totalSize()

	return node[pointersStart:offsetsStart], node[offsetsStart:keyValueStart], node[keyValueStart:end]
}
