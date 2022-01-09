package table

import (
	"errors"
	"fmt"

	"github.com/nihei9/simple-db/storage"
)

type TableScanner struct {
	tx            *storage.Transaction
	tableFileName string
	layout        *layout
	recPage       *recordPage
	currentSlot   slotNum
}

func NewTableScanner(tx *storage.Transaction, tableName string, layout *layout) (*TableScanner, error) {
	s := &TableScanner{
		tx:            tx,
		tableFileName: fmt.Sprintf("%v.tbl", tableName),
		layout:        layout,
		currentSlot:   -1,
	}

	c, err := tx.BlockCount(s.tableFileName)
	if err != nil {
		return nil, err
	}
	if c == 0 {
		err = s.moveToNewBlock()
	} else {
		err = s.moveToBlock(0)
	}
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *TableScanner) Close() error {
	if s.recPage == nil {
		return nil
	}
	return s.tx.Unpin(s.recPage.blk)
}

func (s *TableScanner) BeforeFirst() error {
	return s.moveToBlock(0)
}

func (s *TableScanner) Next() (bool, error) {
	for {
		nextSlot, err := s.recPage.nextUsedSlotAfter(s.currentSlot)
		if err == nil {
			s.currentSlot = nextSlot
			return true, nil
		}
		if errors.Is(err, errRecPageSlotOutOfRange) {
			last, err := s.atLastBlock()
			if err != nil {
				return false, err
			}
			if last {
				return false, nil
			}

			err = s.moveToBlock(s.recPage.blk.BlkNum + 1)
			if err != nil {
				return false, err
			}
			continue
		}
		return false, err
	}
}

func (s *TableScanner) ReadInt64(fieldName string) (int64, error) {
	return s.recPage.readInt64(s.currentSlot, fieldName)
}

func (s *TableScanner) ReadUint64(fieldName string) (uint64, error) {
	return s.recPage.readUint64(s.currentSlot, fieldName)
}

func (s *TableScanner) ReadString(fieldName string) (string, error) {
	return s.recPage.readString(s.currentSlot, fieldName)
}

func (s *TableScanner) WriteInt64(fieldName string, val int64) error {
	return s.recPage.writeInt64(s.currentSlot, fieldName, val)
}

func (s *TableScanner) WriteUint64(fieldName string, val uint64) error {
	return s.recPage.writeUint64(s.currentSlot, fieldName, val)
}

func (s *TableScanner) WriteString(fieldName string, val string) error {
	return s.recPage.writeString(s.currentSlot, fieldName, val)
}

func (s *TableScanner) Insert() error {
	for {
		newSlot, err := s.recPage.insertAfter(s.currentSlot)
		if err == nil {
			s.currentSlot = newSlot
			return nil
		}
		if errors.Is(err, errRecPageSlotOutOfRange) {
			last, err := s.atLastBlock()
			if err != nil {
				return err
			}
			if last {
				err = s.moveToNewBlock()
			} else {
				err = s.moveToBlock(s.recPage.blk.BlkNum + 1)
			}
			if err != nil {
				return err
			}
			continue
		}
		return err
	}
}

func (s *TableScanner) Delete() error {
	return s.recPage.delete(s.currentSlot)
}

func (s *TableScanner) contain(fieldName string) bool {
	for _, f := range s.layout.schema.fields {
		if f.name == fieldName {
			return true
		}
	}
	return false
}

func (s *TableScanner) moveToBlock(blkNum int) error {
	err := s.Close()
	if err != nil {
		return err
	}

	blk := storage.NewBlockID(s.tableFileName, blkNum)
	rp, err := newRecordPage(s.tx, blk, s.layout)
	if err != nil {
		return err
	}
	s.recPage = rp
	s.currentSlot = -1
	return nil
}

func (s *TableScanner) moveToNewBlock() error {
	err := s.Close()
	if err != nil {
		return err
	}

	blk, err := s.tx.AllocBlock(s.tableFileName)
	if err != nil {
		return err
	}
	rp, err := newRecordPage(s.tx, blk, s.layout)
	if err != nil {
		return err
	}
	err = rp.format()
	if err != nil {
		return err
	}
	s.recPage = rp
	s.currentSlot = -1
	return nil
}

func (s *TableScanner) atLastBlock() (bool, error) {
	c, err := s.tx.BlockCount(s.tableFileName)
	if err != nil {
		return false, err
	}
	return s.recPage.blk.BlkNum == c-1, nil
}
