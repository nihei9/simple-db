package scanner

import (
	"fmt"

	"github.com/nihei9/simple-db/table"
)

type Scanner interface {
	BeforeFirst() error
	Next() (bool, error)
	ReadInt64(fieldName string) (int64, error)
	ReadUint64(fieldName string) (uint64, error)
	ReadString(fieldName string) (string, error)
	Read(fieldName string) (Constant, error)
	Contain(fieldName string) bool
	Close() error
}

var (
	_ Scanner = &projectScanner{}
	_ Scanner = &productScanner{}
)

type UpdateScanner interface {
	Scanner

	WriteInt64(fieldName string, val int64) error
	WriteUint64(fieldName string, val uint64) error
	WriteString(fieldName string, val string) error
	Insert() error
	Delete() error
}

var (
	_ UpdateScanner = &tableScanner{}
	_ UpdateScanner = &selectScanner{}
)

var (
	errScannerNotUpdetable  = fmt.Errorf("a scanner is not a updatable")
	errScannerFieldNotFound = fmt.Errorf("a scanner does not contain a field")
)

type tableScanner struct {
	*table.TableScanner

	schema *table.Schema
}

func NewTableScanner(ts *table.TableScanner, sc *table.Schema) *tableScanner {
	return &tableScanner{
		TableScanner: ts,
		schema:       sc,
	}
}

func (s *tableScanner) Read(fieldName string) (Constant, error) {
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
		return NewInt64Constant(v), nil
	case table.FieldTypeUint64:
		v, err := s.ReadUint64(fieldName)
		if err != nil {
			return nil, err
		}
		return NewUint64Constant(v), nil
	case table.FieldTypeString:
		v, err := s.ReadString(fieldName)
		if err != nil {
			return nil, err
		}
		return NewStringConstant(v), nil
	default:
		return nil, fmt.Errorf("invalid field type: %v", f.Ty)
	}
}

func (s *tableScanner) Contain(fieldName string) bool {
	_, ok := s.schema.Field(fieldName)
	return ok
}

type selectScanner struct {
	Scanner

	pred *Predicate
}

func NewSelectScanner(s Scanner, pred *Predicate) *selectScanner {
	return &selectScanner{
		Scanner: s,
		pred:    pred,
	}
}

func (s *selectScanner) Next() (bool, error) {
	for {
		ok, err := s.Scanner.Next()
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
	us, ok := s.Scanner.(UpdateScanner)
	if !ok {
		return errScannerNotUpdetable
	}
	return us.WriteInt64(fieldName, val)
}

func (s *selectScanner) WriteUint64(fieldName string, val uint64) error {
	us, ok := s.Scanner.(UpdateScanner)
	if !ok {
		return errScannerNotUpdetable
	}
	return us.WriteUint64(fieldName, val)
}

func (s *selectScanner) WriteString(fieldName string, val string) error {
	us, ok := s.Scanner.(UpdateScanner)
	if !ok {
		return errScannerNotUpdetable
	}
	return us.WriteString(fieldName, val)
}

func (s *selectScanner) Insert() error {
	us, ok := s.Scanner.(UpdateScanner)
	if !ok {
		return errScannerNotUpdetable
	}
	return us.Insert()
}

func (s *selectScanner) Delete() error {
	us, ok := s.Scanner.(UpdateScanner)
	if !ok {
		return errScannerNotUpdetable
	}
	return us.Delete()
}

type projectScanner struct {
	Scanner

	fields map[string]struct{}
}

func NewProjectScanner(s Scanner, fields []string) *projectScanner {
	fs := map[string]struct{}{}
	for _, f := range fields {
		fs[f] = struct{}{}
	}

	return &projectScanner{
		Scanner: s,
		fields:  fs,
	}
}

func (s *projectScanner) ReadInt64(fieldName string) (int64, error) {
	if !s.Contain(fieldName) {
		return 0, errScannerFieldNotFound
	}
	return s.Scanner.ReadInt64(fieldName)
}

func (s *projectScanner) ReadUint64(fieldName string) (uint64, error) {
	if !s.Contain(fieldName) {
		return 0, errScannerFieldNotFound
	}
	return s.Scanner.ReadUint64(fieldName)
}

func (s *projectScanner) ReadString(fieldName string) (string, error) {
	if !s.Contain(fieldName) {
		return "", errScannerFieldNotFound
	}
	return s.Scanner.ReadString(fieldName)
}

func (s *projectScanner) Read(fieldName string) (Constant, error) {
	if !s.Contain(fieldName) {
		return nil, errScannerFieldNotFound
	}
	return s.Scanner.Read(fieldName)
}

func (s *projectScanner) Contain(fieldName string) bool {
	_, ok := s.fields[fieldName]
	return ok
}

type productScanner struct {
	s1 Scanner
	s2 Scanner
}

func NewProductScanner(s1, s2 Scanner) *productScanner {
	return &productScanner{
		s1: s1,
		s2: s2,
	}
}

func (s *productScanner) BeforeFirst() error {
	err := s.s1.BeforeFirst()
	if err != nil {
		return err
	}
	_, err = s.s1.Next()
	if err != nil {
		return err
	}
	err = s.s2.BeforeFirst()
	if err != nil {
		return err
	}
	return nil
}

func (s *productScanner) Next() (bool, error) {
	ok, err := s.s2.Next()
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}

	err = s.s2.BeforeFirst()
	if err != nil {
		return false, err
	}
	ok, err = s.s2.Next()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	ok, err = s.s1.Next()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return true, nil
}

func (s *productScanner) ReadInt64(fieldName string) (int64, error) {
	if s.s1.Contain(fieldName) {
		return s.s1.ReadInt64(fieldName)
	}
	return s.s2.ReadInt64(fieldName)
}

func (s *productScanner) ReadUint64(fieldName string) (uint64, error) {
	if s.s1.Contain(fieldName) {
		return s.s1.ReadUint64(fieldName)
	}
	return s.s2.ReadUint64(fieldName)
}

func (s *productScanner) ReadString(fieldName string) (string, error) {
	if s.s1.Contain(fieldName) {
		return s.s1.ReadString(fieldName)
	}
	return s.s2.ReadString(fieldName)
}

func (s *productScanner) Read(fieldName string) (Constant, error) {
	if s.s1.Contain(fieldName) {
		return s.s1.Read(fieldName)
	}
	return s.s2.Read(fieldName)
}

func (s *productScanner) Contain(fieldName string) bool {
	return s.s1.Contain(fieldName) || s.s2.Contain(fieldName)
}

func (s *productScanner) Close() error {
	err := s.s1.Close()
	if err != nil {
		return err
	}
	err = s.s2.Close()
	if err != nil {
		return err
	}
	return nil
}
