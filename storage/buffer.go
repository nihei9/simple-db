package storage

import (
	"fmt"
	"time"
)

type transactionNum int

const transactionNumNil transactionNum = 0

var (
	errBufferUnassigned         = fmt.Errorf("buffer is unassigned")
	errBufferNegativePinCounter = fmt.Errorf("the current pin count is less than or equal to 0")
)

type buffer struct {
	fm       *fileManager
	lm       *logManager
	contents *page
	blk      *blockID
	modified bool
	txNum    transactionNum
	lsn      logSeqNum
	pins     int
}

func newBuffer(fm *fileManager, lm *logManager) (*buffer, error) {
	c, err := newPage(fm.blkSize)
	if err != nil {
		return nil, err
	}

	return &buffer{
		fm:       fm,
		lm:       lm,
		contents: c,
		blk:      nil,
		modified: false,
		txNum:    transactionNumNil,
		lsn:      lsnNil,
		pins:     0,
	}, nil
}

func (b *buffer) modify(txNum transactionNum, lsn logSeqNum) error {
	if txNum <= transactionNumNil {
		return fmt.Errorf("a transaction number must be a non-nil value")
	}

	b.modified = true
	b.txNum = txNum
	// When `lsn` is nil, it indicates this modification doesn't need to generate a log record.
	if lsn > lsnNil {
		b.lsn = lsn
	}
	return nil
}

func (b *buffer) pinned() bool {
	return b.pins > 0
}

func (b *buffer) assign(blk *blockID) error {
	err := b.flush()
	if err != nil {
		return err
	}
	b.blk = blk
	err = b.fm.read(b.blk, b.contents)
	if err != nil {
		return err
	}
	b.pins = 0
	return nil
}

func (b *buffer) flush() error {
	if !b.modified {
		return nil
	}
	err := b.lm.flush(b.lsn)
	if err != nil {
		return err
	}
	err = b.fm.write(b.blk, b.contents)
	if err != nil {
		return err
	}
	b.modified = false
	b.txNum = transactionNumNil
	return nil
}

func (b *buffer) pin() error {
	if b.blk == nil {
		return fmt.Errorf("failed to pin: %w", errBufferUnassigned)
	}
	b.pins++
	return nil
}

func (b *buffer) unpin() error {
	if b.blk == nil {
		return fmt.Errorf("failed to unpin: %w", errBufferUnassigned)
	}
	if b.pins <= 0 {
		return fmt.Errorf("failed to unpin: %w: current pin count: %v", errBufferNegativePinCounter, b.pins)
	}
	b.pins--
	return nil
}

type bufferManager struct {
	pool         []*buffer
	freeBufCount int
}

func newBufferManager(fm *fileManager, lm *logManager, bufSize int) (*bufferManager, error) {
	pool := make([]*buffer, bufSize)
	for i := 0; i < bufSize; i++ {
		var err error
		pool[i], err = newBuffer(fm, lm)
		if err != nil {
			return nil, err
		}
	}
	return &bufferManager{
		pool:         pool,
		freeBufCount: bufSize,
	}, nil
}

func (m *bufferManager) flushAll(txNum transactionNum) error {
	for _, buf := range m.pool {
		if buf.txNum != txNum {
			continue
		}
		err := buf.flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *bufferManager) pin(blk *blockID) (*buffer, error) {
	w := time.NewTimer(10 * time.Second)
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-w.C:
			return nil, fmt.Errorf("pinning timed out")
		case <-t.C:
			buf, err := m.tryToPin(blk)
			if err != nil {
				return nil, err
			}
			if buf == nil {
				continue
			}
			return buf, nil
		}
	}
}

func (m *bufferManager) tryToPin(blk *blockID) (*buffer, error) {
	buf := m.findAssignedBuffer(blk)
	if buf == nil {
		buf = m.chooseUnpinnedBuffer()
		if buf == nil {
			return nil, nil
		}
		err := buf.assign(blk)
		if err != nil {
			return nil, err
		}
	}
	if !buf.pinned() {
		m.freeBufCount--
	}
	err := buf.pin()
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (m *bufferManager) findAssignedBuffer(blk *blockID) *buffer {
	for _, buf := range m.pool {
		// A block may not be assigned yet.
		if buf.blk == nil {
			continue
		}

		if buf.blk.equal(blk) {
			return buf
		}
	}
	return nil
}

func (m *bufferManager) chooseUnpinnedBuffer() *buffer {
	for _, buf := range m.pool {
		if !buf.pinned() {
			return buf
		}
	}
	return nil
}

func (m *bufferManager) unpin(buf *buffer) error {
	err := buf.unpin()
	if err != nil {
		return err
	}
	if buf.pinned() {
		return nil
	}
	m.freeBufCount++
	return nil
}