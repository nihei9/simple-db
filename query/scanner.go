package query

import (
	"fmt"

	"github.com/nihei9/simple-db/table"
)

type scanner interface {
	BeforeFirst() error
	Next() (bool, error)
	ReadInt64(fieldName string) (int64, error)
	ReadUint64(fieldName string) (uint64, error)
	ReadString(fieldName string) (string, error)
	Read(fieldName string) (constant, error)
	Contain(fieldName string) bool
	Close() error
}

type updateScanner interface {
	scanner

	WriteInt64(fieldName string, val int64) error
	WriteUint64(fieldName string, val uint64) error
	WriteString(fieldName string, val string) error
	Insert() error
	Delete() error
}

var (
	_ updateScanner = &tableScanner{}
	_ updateScanner = &selectScanner{}
)

var scannerNotUpdetable = fmt.Errorf("a scanner is not a updatable")

type tableScanner struct {
	*table.TableScanner

	schema *table.Schema
}

func newTableScanner(ts *table.TableScanner, sc *table.Schema) *tableScanner {
	return &tableScanner{
		TableScanner: ts,
		schema:       sc,
	}
}

func (s *tableScanner) Read(fieldName string) (constant, error) {
	f, ok := s.schema.Field(fieldName)
	if !ok {
		return nil, fmt.Errorf("invalid field name: %v", fieldName)
	}

	switch f.Ty {
	case table.FieldTypeInt64:
		v, err := s.ReadInt64(fieldName)
		if err != nil {
			return nil, err
		}
		return newInt64Constant(v), nil
	case table.FieldTypeUint64:
		v, err := s.ReadUint64(fieldName)
		if err != nil {
			return nil, err
		}
		return newUint64Constant(v), nil
	case table.FieldTypeString:
		v, err := s.ReadString(fieldName)
		if err != nil {
			return nil, err
		}
		return newStringConstant(v), nil
	default:
		return nil, fmt.Errorf("invalid field type: %v", f.Ty)
	}
}

func (s *tableScanner) Contain(fieldName string) bool {
	_, ok := s.schema.Field(fieldName)
	return ok
}

type selectScanner struct {
	scanner

	pred *predicate
}

func newSelectScanner(s scanner, pred *predicate) *selectScanner {
	return &selectScanner{
		scanner: s,
		pred:    pred,
	}
}

func (s *selectScanner) Next() (bool, error) {
	for {
		ok, err := s.scanner.Next()
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}

		ok, err = s.pred.isSatisfied(s)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
}

func (s *selectScanner) WriteInt64(fieldName string, val int64) error {
	us, ok := s.scanner.(updateScanner)
	if !ok {
		return scannerNotUpdetable
	}
	return us.WriteInt64(fieldName, val)
}

func (s *selectScanner) WriteUint64(fieldName string, val uint64) error {
	us, ok := s.scanner.(updateScanner)
	if !ok {
		return scannerNotUpdetable
	}
	return us.WriteUint64(fieldName, val)
}

func (s *selectScanner) WriteString(fieldName string, val string) error {
	us, ok := s.scanner.(updateScanner)
	if !ok {
		return scannerNotUpdetable
	}
	return us.WriteString(fieldName, val)
}

func (s *selectScanner) Insert() error {
	us, ok := s.scanner.(updateScanner)
	if !ok {
		return scannerNotUpdetable
	}
	return us.Insert()
}

func (s *selectScanner) Delete() error {
	us, ok := s.scanner.(updateScanner)
	if !ok {
		return scannerNotUpdetable
	}
	return us.Delete()
}
