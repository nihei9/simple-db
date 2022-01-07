package storage

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
)

type operator int

const (
	opCheckPoint operator = iota
	opStart
	opCommit
	opRollBack
	opSetInt64
	opSetUint64
	opSetString
)

type logRecord struct {
	Op       operator
	TxNum    transactionNum
	FileName string
	BlkNum   int
	Offset   int
	Val      interface{}
}

func newStartLogRecord(txNum transactionNum) *logRecord {
	return &logRecord{
		Op:    opStart,
		TxNum: txNum,
	}
}

func newCommitLogRecord(txNum transactionNum) *logRecord {
	return &logRecord{
		Op:    opCommit,
		TxNum: txNum,
	}
}

func newRollbackLogRecord(txNum transactionNum) *logRecord {
	return &logRecord{
		Op:    opRollBack,
		TxNum: txNum,
	}
}

func newCheckPointLogRecord() *logRecord {
	return &logRecord{
		Op: opCheckPoint,
	}
}

func newSetInt64LogRecord(txNum transactionNum, blk *blockID, offset int, val int64) *logRecord {
	return &logRecord{
		Op:       opSetInt64,
		TxNum:    txNum,
		FileName: blk.fileName,
		BlkNum:   blk.blkNum,
		Offset:   offset,
		Val:      val,
	}
}

func newSetUint64LogRecord(txNum transactionNum, blk *blockID, offset int, val uint64) *logRecord {
	return &logRecord{
		Op:       opSetUint64,
		TxNum:    txNum,
		FileName: blk.fileName,
		BlkNum:   blk.blkNum,
		Offset:   offset,
		Val:      val,
	}
}

func newSetStringLogRecord(txNum transactionNum, blk *blockID, offset int, val string) *logRecord {
	return &logRecord{
		Op:       opSetString,
		TxNum:    txNum,
		FileName: blk.fileName,
		BlkNum:   blk.blkNum,
		Offset:   offset,
		Val:      val,
	}
}

func (r *logRecord) marshalBytes() ([]byte, error) {
	b := bytes.NewBuffer([]byte{})
	err := gob.NewEncoder(b).Encode(r)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (r *logRecord) unmarshalBytes(b []byte) error {
	return gob.NewDecoder(bytes.NewReader(b)).Decode(r)
}

type recoveryManager struct {
	lm    *logManager
	bm    *bufferManager
	txNum transactionNum
}

func newRecoveryManager(lm *logManager, bm *bufferManager, txNum transactionNum) (*recoveryManager, error) {
	rm := &recoveryManager{
		lm:    lm,
		bm:    bm,
		txNum: txNum,
	}

	rec, err := newStartLogRecord(txNum).marshalBytes()
	if err != nil {
		return nil, err
	}
	lsn, err := rm.lm.appendLog(rec)
	if err != nil {
		return nil, err
	}
	err = rm.lm.flush(lsn)
	if err != nil {
		return nil, err
	}

	return rm, nil
}

func (m *recoveryManager) commit() error {
	err := m.bm.flushAll(m.txNum)
	if err != nil {
		return err
	}

	rec, err := newCommitLogRecord(m.txNum).marshalBytes()
	if err != nil {
		return err
	}
	lsn, err := m.lm.appendLog(rec)
	if err != nil {
		return err
	}
	return m.lm.flush(lsn)
}

func (m *recoveryManager) rollback(tx *Transaction) error {
	err := m.lm.apply(func(rec []byte) (bool, error) {
		r := &logRecord{}
		err := r.unmarshalBytes(rec)
		if err != nil {
			return false, err
		}
		if r.TxNum != m.txNum || r.Op == opStart {
			return true, nil
		}
		return false, m.undo(tx, r)
	})
	if err != nil {
		return err
	}

	err = m.bm.flushAll(m.txNum)
	if err != nil {
		return err
	}

	rec, err := newRollbackLogRecord(m.txNum).marshalBytes()
	if err != nil {
		return err
	}
	lsn, err := m.lm.appendLog(rec)
	if err != nil {
		return err
	}
	return m.lm.flush(lsn)
}

func (m *recoveryManager) recover(tx *Transaction) error {
	finishedTxs := map[transactionNum]struct{}{}
	err := m.lm.apply(func(rec []byte) (bool, error) {
		r := &logRecord{}
		err := r.unmarshalBytes(rec)
		if err != nil {
			return false, err
		}
		switch r.Op {
		case opCheckPoint:
			return true, nil
		case opCommit:
			finishedTxs[r.TxNum] = struct{}{}
		case opRollBack:
			finishedTxs[r.TxNum] = struct{}{}
		default:
			if _, ok := finishedTxs[r.TxNum]; ok {
				return false, nil
			}
			return false, m.undo(tx, r)
		}
		return false, nil
	})
	if err != nil {
		return err
	}

	err = m.bm.flushAll(m.txNum)
	if err != nil {
		return err
	}

	rec, err := newCheckPointLogRecord().marshalBytes()
	if err != nil {
		return err
	}
	lsn, err := m.lm.appendLog(rec)
	if err != nil {
		return err
	}
	return m.lm.flush(lsn)
}

func (m *recoveryManager) undo(tx *Transaction, rec *logRecord) error {
	if rec.Op != opSetInt64 && rec.Op != opSetUint64 && rec.Op != opSetString {
		return nil
	}

	blk := newBlockID(rec.FileName, rec.BlkNum)

	err := tx.Pin(blk)
	if err != nil {
		return err
	}

	switch rec.Op {
	case opSetInt64:
		err = tx.WriteInt64(blk.hash, rec.Offset, rec.Val.(int64), false)
	case opSetUint64:
		err = tx.WriteUint64(blk.hash, rec.Offset, rec.Val.(uint64), false)
	case opSetString:
		err = tx.WriteString(blk.hash, rec.Offset, rec.Val.(string), false)
	}
	if err != nil {
		return err
	}

	err = tx.Unpin(blk)
	if err != nil {
		return err
	}

	return nil
}

func (m *recoveryManager) writeInt64(buf *buffer, offset int, val int64) (logSeqNum, error) {
	oldVal, _, err := buf.contents.readInt64(offset)
	if err != nil {
		if errors.Is(err, errPageNoData) {
			return lsnNil, nil
		} else {
			return lsnNil, fmt.Errorf("failed to read the current contents: %w", err)
		}
	}
	rec, err := newSetInt64LogRecord(m.txNum, buf.blk, offset, oldVal).marshalBytes()
	if err != nil {
		return lsnNil, err
	}
	return m.lm.appendLog(rec)
}

func (m *recoveryManager) writeUint64(buf *buffer, offset int, val uint64) (logSeqNum, error) {
	oldVal, _, err := buf.contents.readUint64(offset)
	if err != nil {
		if errors.Is(err, errPageNoData) {
			return lsnNil, nil
		} else {
			return lsnNil, fmt.Errorf("failed to read the current contents: %w", err)
		}
	}
	rec, err := newSetUint64LogRecord(m.txNum, buf.blk, offset, oldVal).marshalBytes()
	if err != nil {
		return lsnNil, err
	}
	return m.lm.appendLog(rec)
}

func (m *recoveryManager) writeString(buf *buffer, offset int, val string) (logSeqNum, error) {
	oldVal, _, err := buf.contents.readString(offset)
	if err != nil {
		if errors.Is(err, errPageNoData) {
			return lsnNil, nil
		} else {
			return lsnNil, fmt.Errorf("failed to read the current contents: %w", err)
		}
	}
	rec, err := newSetStringLogRecord(m.txNum, buf.blk, offset, oldVal).marshalBytes()
	if err != nil {
		return lsnNil, err
	}
	return m.lm.appendLog(rec)
}
