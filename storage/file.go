package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type BlockIDHash [32]byte

type BlockID struct {
	fileName string
	BlkNum   int
	Hash     BlockIDHash
}

func NewBlockID(fileName string, blkNum int) *BlockID {
	var h BlockIDHash
	{
		b := make([]byte, len(fileName)+binary.MaxVarintLen64)
		sb := []byte(fileName)
		copy(b, sb)
		ib := make([]byte, binary.MaxVarintLen64)
		binary.PutVarint(ib, int64(blkNum))
		copy(b[len(sb):], ib)

		h = sha256.Sum256(b)
	}

	return &BlockID{
		fileName: fileName,
		BlkNum:   blkNum,
		Hash:     h,
	}
}

func (id *BlockID) equal(a *BlockID) bool {
	if id.fileName == a.fileName && id.BlkNum == a.BlkNum {
		return true
	}
	return false
}

type page struct {
	buf []byte
}

var (
	errPageBlockSizeOutOfRange = fmt.Errorf("block size is out of range")
	errPageOffsetOutOfRange    = fmt.Errorf("offset is out of range")
	errPageInvalidOffset       = fmt.Errorf("invalid offset")
	errPageTooBigData          = fmt.Errorf("data is too big")
	errPageNegativeDataSize    = fmt.Errorf("data size must be >0")
	errPageDataOutOfRange      = fmt.Errorf("data is out of range")
	errPageNoData              = fmt.Errorf("data does not exist yet")
)

func newPage(blkSize int) (*page, error) {
	if blkSize <= 0 {
		return nil, fmt.Errorf("%w: block size: %v byte", errPageBlockSizeOutOfRange, blkSize)
	}

	return &page{
		buf: make([]byte, blkSize),
	}, nil
}

func (p *page) load(src io.Reader) error {
	_, err := io.ReadFull(src, p.buf)
	if err != nil {
		return fmt.Errorf("failed to load a block onto a page: %w", err)
	}
	return nil
}

func (p *page) readInt64(offset int) (int64, int, error) {
	b, n, err := p.read(offset)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read an int64: %w", err)
	}
	if len(b) == 0 {
		return 0, 0, fmt.Errorf("failed to read an int64: %w", errPageNoData)
	}
	v, err := decodeToInt64(b)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decode data to an int64: %w", err)
	}
	return v, n, nil
}

func (p *page) writeInt64(offset int, v int64) (int, error) {
	b := make([]byte, binary.MaxVarintLen64)
	l := binary.PutVarint(b, v)
	n, err := p.write(offset, b[:l])
	if err != nil {
		return 0, fmt.Errorf("failed to write a uint64: %w", err)
	}
	return n, nil
}

func (p *page) readUint64(offset int) (uint64, int, error) {
	b, n, err := p.read(offset)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read a uint64: %w", err)
	}
	if len(b) == 0 {
		return 0, 0, fmt.Errorf("failed to read a uint64: %w", errPageNoData)
	}
	v, err := decodeToUint64(b)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decode data to a uint64: %w", err)
	}
	return v, n, nil
}

func (p *page) writeUint64(offset int, v uint64) (int, error) {
	b := make([]byte, binary.MaxVarintLen64)
	l := binary.PutUvarint(b, v)
	n, err := p.write(offset, b[:l])
	if err != nil {
		return 0, fmt.Errorf("failed to write a uint64: %w", err)
	}
	return n, nil
}

func (p *page) readString(offset int) (string, int, error) {
	b, n, err := p.read(offset)
	if err != nil {
		return "", 0, fmt.Errorf("failed to read a string: %w", err)
	}
	// NOTE: If `b` is empty, we want to return an `errPageNoData`, but we cannot do that because we cannot
	// distinguish it from the case where `b` represents the empty string.
	return string(b), n, nil
}

func (p *page) writeString(offset int, v string) (int, error) {
	n, err := p.write(offset, []byte(v))
	if err != nil {
		return 0, fmt.Errorf("failed to write a string: %w", err)
	}
	return n, nil
}

func (p *page) read(offset int) ([]byte, int, error) {
	if offset < 0 || offset >= len(p.buf) {
		return nil, 0, fmt.Errorf("%w: block size: %v byte, offset: %v", errPageOffsetOutOfRange, len(p.buf), offset)
	}

	dataOffset := offset + binary.MaxVarintLen64
	var size int64
	{
		if dataOffset > len(p.buf) {
			return nil, 0, fmt.Errorf("%w: block size: %v byte, offset: %v", errPageInvalidOffset, len(p.buf), offset)
		}
		var err error
		size, err = decodeToInt64(p.buf[offset:dataOffset])
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode the size of data to be read: %w", err)
		}
		if size < 0 {
			return nil, 0, fmt.Errorf("failed to get the size of data to be read: %w: size: %v", errPageNegativeDataSize, size)
		}
	}

	if dataOffset+int(size) > len(p.buf) {
		return nil, 0, fmt.Errorf("data that is out of range is requested: %w: block size: %v byte, requested range: %v-%v", errPageDataOutOfRange, len(p.buf), dataOffset, dataOffset+int(size))
	}
	return p.buf[dataOffset : dataOffset+int(size)], binary.MaxVarintLen64 + int(size), nil
}

func (p *page) write(offset int, data []byte) (int, error) {
	if offset < 0 || offset >= len(p.buf) {
		return 0, fmt.Errorf("%w: block size: %v byte, offset: %v", errPageOffsetOutOfRange, len(p.buf), offset)
	}
	if offset+CalcBytesNeeded(len(data)) > len(p.buf) {
		return 0, fmt.Errorf("%w: block size: %v byte, offset: %v, data size: %v byte", errPageTooBigData, len(p.buf), offset, len(data))
	}

	b := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(b, int64(len(data)))
	copy(p.buf[offset:], b)
	copy(p.buf[offset+len(b):], data)

	return len(b) + len(data), nil
}

func CalcBytesNeeded(dataSize int) int {
	return binary.MaxVarintLen64 + dataSize
}

var (
	errDecodeVarintEmptySource = fmt.Errorf("empty source")
	errDecodeVarintOverflow    = fmt.Errorf("overflow")
)

func decodeToInt64(b []byte) (int64, error) {
	v, n := binary.Varint(b)
	if n == 0 {
		return 0, errDecodeVarintEmptySource
	}
	if n < 0 {
		return 0, errDecodeVarintOverflow
	}
	return v, nil
}

func decodeToUint64(b []byte) (uint64, error) {
	v, n := binary.Uvarint(b)
	if n == 0 {
		return 0, errDecodeVarintEmptySource
	}
	if n < 0 {
		return 0, errDecodeVarintOverflow
	}
	return v, nil
}

type fileManager struct {
	dirPath   string
	blkSize   int
	openFiles map[string]*os.File
	mu        sync.Mutex
}

func newFileManager(dirPath string, blkSize int) (*fileManager, error) {
	s, err := os.Stat(dirPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		err := os.Mkdir(dirPath, 0700)
		if err != nil {
			return nil, err
		}
	} else {
		if !s.IsDir() {
			return nil, fmt.Errorf("not a directory: %v", dirPath)
		}

		entries, err := os.ReadDir(dirPath)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), "tmp_") {
				continue
			}
			err := os.Remove(filepath.Join(dirPath, e.Name()))
			if err != nil {
				return nil, err
			}
		}
	}

	return &fileManager{
		dirPath:   dirPath,
		blkSize:   blkSize,
		openFiles: map[string]*os.File{},
	}, nil
}

// read reads the contents of a block into a page.
func (m *fileManager) read(blk *BlockID, p *page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.openNoLock(blk.fileName)
	if err != nil {
		return err
	}
	_, err = f.Seek(int64(blk.BlkNum*m.blkSize), 0)
	if err != nil {
		return err
	}
	return p.load(f)
}

// write writes the contents of a page to a block on a disk.
func (m *fileManager) write(blk *BlockID, p *page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.openNoLock(blk.fileName)
	if err != nil {
		return err
	}
	_, err = f.Seek(int64(blk.BlkNum*m.blkSize), 0)
	if err != nil {
		return err
	}
	_, err = f.Write(p.buf)
	if err != nil {
		return err
	}

	return nil
}

func (m *fileManager) alloc(fileName string) (*BlockID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.openNoLock(fileName)
	if err != nil {
		return nil, err
	}
	blkNum, err := m.blockCount(fileName)
	if err != nil {
		return nil, err
	}
	_, err = f.Seek(int64(blkNum*m.blkSize), 0)
	if err != nil {
		return nil, err
	}
	_, err = f.WriteAt(make([]byte, m.blkSize), int64(blkNum*m.blkSize))
	if err != nil {
		return nil, err
	}

	return NewBlockID(fileName, blkNum), nil
}

func (m *fileManager) blockCount(fileName string) (int, error) {
	s, err := os.Stat(filepath.Join(m.dirPath, fileName))
	if err != nil {
		return 0, err
	}
	return int(s.Size()) / m.blkSize, nil
}

func (m *fileManager) open(fileName string) (*os.File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.openNoLock(fileName)
}

func (m *fileManager) openNoLock(fileName string) (*os.File, error) {
	f, ok := m.openFiles[fileName]
	if ok {
		return f, nil
	}

	f, err := os.OpenFile(filepath.Join(m.dirPath, fileName), os.O_CREATE|os.O_RDWR|os.O_SYNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open a new file: %w", err)
	}
	m.openFiles[fileName] = f

	return f, nil
}
