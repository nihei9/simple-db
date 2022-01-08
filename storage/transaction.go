package storage

import (
	"context"
	"fmt"
	"time"
)

func runTransactionNumIssuer(ctx context.Context) <-chan transactionNum {
	c := make(chan transactionNum, 1000)

	go func() {
		txNum := transactionNumNil
		for {
			select {
			case <-ctx.Done():
				close(c)
				return
			default:
				txNum++
				c <- txNum
			}
		}
	}()

	return c
}

type Transaction struct {
	ctx   context.Context
	txNum transactionNum
	cm    *concurrencyManager
	rm    *recoveryManager
	bl    *bufferList
	fm    *fileManager
	bm    *bufferManager
}

func newTransaction(ctx context.Context, txNum transactionNum, fm *fileManager, lm *logManager, bm *bufferManager, lockTab *lockTable) (*Transaction, error) {
	rm, err := newRecoveryManager(lm, bm, txNum)
	if err != nil {
		return nil, err
	}

	fmt.Printf("transaction #%v started\n", txNum)

	return &Transaction{
		ctx:   ctx,
		txNum: txNum,
		cm:    newConcurrencyManager(lockTab),
		rm:    rm,
		bl:    newBufferList(bm),
		fm:    fm,
		bm:    bm,
	}, nil
}

func (t *Transaction) Commit() error {
	err := t.rm.commit()
	if err != nil {
		return err
	}
	t.cm.release()
	err = t.bl.unpinAll()
	if err != nil {
		return err
	}

	fmt.Printf("transaction #%v committed\n", t.txNum)

	return nil
}

func (t *Transaction) Rollback() error {
	err := t.rm.rollback(t)
	if err != nil {
		return err
	}
	t.cm.release()
	err = t.bl.unpinAll()
	if err != nil {
		return err
	}

	fmt.Printf("transaction #%v rolled back\n", t.txNum)

	return nil
}

func (t *Transaction) Recover() error {
	err := t.bm.flushAll(t.txNum)
	if err != nil {
		return err
	}
	err = t.rm.recover(t)
	if err != nil {
		return err
	}
	return nil
}

func (t *Transaction) Pin(blk *BlockID) error {
	return t.bl.pin(blk)
}

func (t *Transaction) Unpin(blk *BlockID) error {
	return t.bl.unpin(blk)
}

func (t *Transaction) ReadInt64(blk BlockIDHash, offset int) (int64, error) {
	ctx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
	defer cancel()
	err := t.cm.sLock(ctx, blk)
	if err != nil {
		return 0, err
	}
	buf, err := t.bl.blockToBuffer(blk)
	if err != nil {
		return 0, err
	}
	v, _, err := buf.contents.readInt64(offset)
	return v, err
}

func (t *Transaction) ReadUint64(blk BlockIDHash, offset int) (uint64, error) {
	ctx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
	defer cancel()
	err := t.cm.sLock(ctx, blk)
	if err != nil {
		return 0, err
	}
	buf, err := t.bl.blockToBuffer(blk)
	if err != nil {
		return 0, err
	}
	v, _, err := buf.contents.readUint64(offset)
	return v, err
}

func (t *Transaction) ReadString(blk BlockIDHash, offset int) (string, error) {
	ctx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
	defer cancel()
	err := t.cm.sLock(ctx, blk)
	if err != nil {
		return "", err
	}
	buf, err := t.bl.blockToBuffer(blk)
	if err != nil {
		return "", err
	}
	v, _, err := buf.contents.readString(offset)
	return v, err
}

func (t *Transaction) WriteInt64(blk BlockIDHash, offset int, val int64, log bool) error {
	ctx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
	defer cancel()
	err := t.cm.xLock(ctx, blk)
	if err != nil {
		return err
	}
	buf, err := t.bl.blockToBuffer(blk)
	if err != nil {
		return err
	}
	lsn := lsnNil
	if log {
		var err error
		lsn, err = t.rm.writeInt64(buf, offset, val)
		if err != nil {
			return fmt.Errorf("failed to write a log: %w", err)
		}
	}
	_, err = buf.contents.writeInt64(offset, val)
	if err != nil {
		return fmt.Errorf("failed to write contents: %w", err)
	}
	return buf.modify(t.txNum, lsn)
}

func (t *Transaction) WriteUint64(blk BlockIDHash, offset int, val uint64, log bool) error {
	ctx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
	defer cancel()
	err := t.cm.xLock(ctx, blk)
	if err != nil {
		return err
	}
	buf, err := t.bl.blockToBuffer(blk)
	if err != nil {
		return err
	}
	lsn := lsnNil
	if log {
		var err error
		lsn, err = t.rm.writeUint64(buf, offset, val)
		if err != nil {
			return fmt.Errorf("failed to write a log: %w", err)
		}
	}
	_, err = buf.contents.writeUint64(offset, val)
	if err != nil {
		return fmt.Errorf("failed to write contents: %w", err)
	}
	return buf.modify(t.txNum, lsn)
}

func (t *Transaction) WriteString(blk BlockIDHash, offset int, val string, log bool) error {
	ctx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
	defer cancel()
	err := t.cm.xLock(ctx, blk)
	if err != nil {
		return err
	}
	buf, err := t.bl.blockToBuffer(blk)
	if err != nil {
		return err
	}
	lsn := lsnNil
	if log {
		var err error
		lsn, err = t.rm.writeString(buf, offset, val)
		if err != nil {
			return fmt.Errorf("failed to write a log: %w", err)
		}
	}
	_, err = buf.contents.writeString(offset, val)
	if err != nil {
		return fmt.Errorf("failed to write contents: %w", err)
	}
	return buf.modify(t.txNum, lsn)
}

//nolint:unused
func (t *Transaction) BlockCount(fileName string) (int, error) {
	ctx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
	defer cancel()
	dummyBlk := NewBlockID(fileName, -1)
	err := t.cm.sLock(ctx, dummyBlk.Hash)
	if err != nil {
		return 0, err
	}
	return t.fm.blockCount(fileName)
}

func (t *Transaction) BlockSize() int {
	return t.fm.blkSize
}

func (t *Transaction) AllocBlock(fileName string) (*BlockID, error) {
	ctx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
	defer cancel()
	dummyBlk := NewBlockID(fileName, -1)
	err := t.cm.xLock(ctx, dummyBlk.Hash)
	if err != nil {
		return nil, err
	}
	return t.fm.alloc(fileName)
}

//nolint:unused
func (t *Transaction) AvailableBufferCount() int {
	return t.bm.availableBufferCount()
}

type bufferList struct {
	bm      *bufferManager
	buffers map[BlockIDHash]*buffer
	pins    map[BlockIDHash]int
}

func newBufferList(bm *bufferManager) *bufferList {
	return &bufferList{
		bm:      bm,
		buffers: map[BlockIDHash]*buffer{},
		pins:    map[BlockIDHash]int{},
	}
}

func (l *bufferList) blockToBuffer(blk BlockIDHash) (*buffer, error) {
	buf, ok := l.buffers[blk]
	if !ok {
		return nil, fmt.Errorf("buffer was not found: block: %x", blk)
	}
	return buf, nil
}

func (l *bufferList) pin(blk *BlockID) error {
	buf, err := l.bm.pin(blk)
	if err != nil {
		return err
	}
	l.buffers[blk.Hash] = buf
	if pins, ok := l.pins[blk.Hash]; ok {
		l.pins[blk.Hash] = pins + 1
	} else {
		l.pins[blk.Hash] = 1
	}
	return nil
}

func (l *bufferList) unpin(blk *BlockID) error {
	buf := l.buffers[blk.Hash]
	err := l.bm.unpin(buf)
	if err != nil {
		return err
	}
	l.pins[blk.Hash]--
	if l.pins[blk.Hash] == 0 {
		delete(l.buffers, blk.Hash)
	}
	return nil
}

func (l *bufferList) unpinAll() error {
	for id := range l.pins {
		buf := l.buffers[id]
		err := l.bm.unpin(buf)
		if err != nil {
			return err
		}
	}
	l.buffers = map[BlockIDHash]*buffer{}
	l.pins = map[BlockIDHash]int{}
	return nil
}
