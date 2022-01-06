package storage

import "sync"

type logSeqNum int

const lsnNil logSeqNum = 0

type logManager struct {
	fm           *fileManager
	logFileName  string
	currentBlk   *blockID
	logPage      *page
	freeBytes    int
	latestLSN    logSeqNum
	lastSavedLSN logSeqNum
	mu           sync.Mutex
}

func newLogManager(fm *fileManager, logFileName string) (*logManager, error) {
	var m *logManager
	{
		p, err := newPage(fm.blkSize)
		if err != nil {
			return nil, err
		}
		m = &logManager{
			fm:           fm,
			logFileName:  logFileName,
			logPage:      p,
			latestLSN:    lsnNil,
			lastSavedLSN: lsnNil,
		}
	}

	_, err := fm.open(logFileName)
	if err != nil {
		return nil, err
	}
	c, err := fm.blockCount(logFileName)
	if err != nil {
		return nil, err
	}
	if c == 0 {
		var err error
		m.currentBlk, err = m.allocBlock()
		if err != nil {
			return nil, err
		}
	} else {
		m.currentBlk = newBlockID(logFileName, c-1)
		err := fm.read(m.currentBlk, m.logPage)
		if err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *logManager) appendLog(logRec []byte) (logSeqNum, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	boundary, _, err := m.logPage.readInt64(0)
	if err != nil {
		return lsnNil, err
	}
	bytesNeeded := calcBytesNeeded(logRec)
	if bytesNeeded > m.freeBytes {
		err := m.flushAllNoLock()
		if err != nil {
			return lsnNil, err
		}
		m.currentBlk, err = m.allocBlock()
		if err != nil {
			return lsnNil, err
		}
		boundary, _, err = m.logPage.readInt64(0)
		if err != nil {
			return lsnNil, err
		}
	}
	offset := int(boundary) - bytesNeeded
	n, err := m.logPage.write(offset, logRec)
	if err != nil {
		return lsnNil, err
	}
	m.freeBytes -= n
	_, err = m.logPage.writeInt64(0, int64(offset))
	if err != nil {
		return lsnNil, err
	}
	m.latestLSN++
	return m.latestLSN, nil
}

func (m *logManager) allocBlock() (*blockID, error) {
	blk, err := m.fm.alloc(m.logFileName)
	if err != nil {
		return nil, err
	}
	n, err := m.logPage.writeInt64(0, int64(m.fm.blkSize))
	if err != nil {
		return nil, err
	}
	err = m.fm.write(blk, m.logPage)
	if err != nil {
		return nil, err
	}
	m.freeBytes = m.fm.blkSize - n
	return blk, nil
}

func (m *logManager) flushAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.flushAllNoLock()
}

func (m *logManager) flushAllNoLock() error {
	err := m.fm.write(m.currentBlk, m.logPage)
	if err != nil {
		return err
	}
	m.lastSavedLSN = m.latestLSN
	return nil
}

func (m *logManager) flush(lsn logSeqNum) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if lsn < m.lastSavedLSN {
		return nil
	}
	return m.flushAllNoLock()
}

func (m *logManager) apply(f func(rec []byte) (bool, error)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.flushAllNoLock()
	if err != nil {
		return err
	}

	blk := m.currentBlk
	p, err := newPage(m.fm.blkSize)
	if err != nil {
		return err
	}
	err = m.fm.read(blk, p)
	if err != nil {
		return err
	}
	boundary, _, err := p.readInt64(0)
	if err != nil {
		return err
	}
	offset := int(boundary)
	for {
		if offset >= m.fm.blkSize {
			if blk.blkNum <= 0 {
				return nil
			}

			blk = newBlockID(blk.fileName, blk.blkNum-1)
			err := m.fm.read(blk, p)
			if err != nil {
				return err
			}
			boundary, _, err := p.readInt64(0)
			if err != nil {
				return err
			}
			offset = int(boundary)
		}

		rec, n, err := p.read(offset)
		if err != nil {
			return err
		}
		offset += n

		done, err := f(rec)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}
