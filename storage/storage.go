package storage

import (
	"context"
	"path/filepath"
)

type StorageConfig struct {
	DirPath     string
	LogFileName string
	BlkSize     int
	BufSize     int
}

type Storage struct {
	ctx     context.Context
	txNumCh <-chan transactionNum
	fm      *fileManager
	lm      *logManager
	bm      *bufferManager
	lockTab *lockTable
}

func InitStorage(ctx context.Context, config *StorageConfig) (*Storage, error) {
	fm, err := newFileManager(config.DirPath, config.BlkSize)
	if err != nil {
		return nil, err
	}
	lm, err := newLogManager(fm, filepath.Base(config.LogFileName))
	if err != nil {
		return nil, err
	}
	bm, err := newBufferManager(fm, lm, config.BufSize)
	if err != nil {
		return nil, err
	}

	return &Storage{
		ctx:     ctx,
		txNumCh: runTransactionNumIssuer(ctx),
		fm:      fm,
		lm:      lm,
		bm:      bm,
		lockTab: newLockTable(),
	}, nil
}

func (s *Storage) NewTransaction() (*Transaction, error) {
	txNum := <-s.txNumCh
	return newTransaction(s.ctx, txNum, s.fm, s.lm, s.bm, s.lockTab)
}
