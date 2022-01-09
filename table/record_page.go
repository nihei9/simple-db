package table

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"

	"github.com/nihei9/simple-db/storage"
)

type fieldType string

const (
	fieldTypeInt64  = "int64"
	fieldTypeUint64 = "uint64"
	fieldTypeString = "string"
)

type field struct {
	ty     fieldType
	length int
}

func newInt64Field() *field {
	return &field{
		ty: fieldTypeInt64,
	}
}

func newUint64Field() *field {
	return &field{
		ty: fieldTypeUint64,
	}
}

func newStringField(length int) *field {
	return &field{
		ty:     fieldTypeString,
		length: length,
	}
}

type namedField struct {
	*field
	name string
}

func newNamedField(name string, f *field) *namedField {
	return &namedField{
		field: f,
		name:  name,
	}
}

type schema struct {
	fields []*namedField
}

func newShcema() *schema {
	return &schema{
		fields: []*namedField{},
	}
}

func (s *schema) add(name string, f *field) {
	s.fields = append(s.fields, newNamedField(name, f))
}

type layout struct {
	schema   *schema
	offsets  map[string]int
	slotSize int
}

func newLayout(schema *schema) *layout {
	offsets := map[string]int{}
	pos := storage.CalcBytesNeeded(binary.MaxVarintLen64)
	for _, f := range schema.fields {
		offsets[f.name] = pos
		var bytesNeeded int
		switch f.ty {
		case fieldTypeInt64:
			bytesNeeded = storage.CalcBytesNeeded(binary.MaxVarintLen64)
		case fieldTypeUint64:
			bytesNeeded = storage.CalcBytesNeeded(binary.MaxVarintLen64)
		case fieldTypeString:
			bytesNeeded = storage.CalcBytesNeeded(f.length * utf8.UTFMax)
		}
		pos += bytesNeeded
	}
	slotSize := pos

	return &layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

func (l *layout) offset(fieldName string) (int, error) {
	v, ok := l.offsets[fieldName]
	if !ok {
		return 0, fmt.Errorf("invalid field name: %v", fieldName)
	}
	return v, nil
}

var errRecPageSlotOutOfRange = fmt.Errorf("a slot is out of range")

type slotNum int

type recordPage struct {
	tx     *storage.Transaction
	blk    *storage.BlockID
	layout *layout
}

func newRecordPage(tx *storage.Transaction, blk *storage.BlockID, layout *layout) (*recordPage, error) {
	err := tx.Pin(blk)
	if err != nil {
		return nil, err
	}

	return &recordPage{
		tx:     tx,
		blk:    blk,
		layout: layout,
	}, nil
}

func (p *recordPage) readInt64(slot slotNum, fieldName string) (int64, error) {
	offset, err := p.offset(slot, fieldName)
	if err != nil {
		return 0, err
	}
	return p.tx.ReadInt64(p.blk.Hash, offset)
}

func (p *recordPage) readUint64(slot slotNum, fieldName string) (uint64, error) {
	offset, err := p.offset(slot, fieldName)
	if err != nil {
		return 0, err
	}
	return p.tx.ReadUint64(p.blk.Hash, offset)
}

func (p *recordPage) readString(slot slotNum, fieldName string) (string, error) {
	offset, err := p.offset(slot, fieldName)
	if err != nil {
		return "", err
	}
	return p.tx.ReadString(p.blk.Hash, offset)
}

func (p *recordPage) writeInt64(slot slotNum, fieldName string, val int64) error {
	offset, err := p.offset(slot, fieldName)
	if err != nil {
		return err
	}
	return p.tx.WriteInt64(p.blk.Hash, offset, val, true)
}

func (p *recordPage) writeUint64(slot slotNum, fieldName string, val uint64) error {
	offset, err := p.offset(slot, fieldName)
	if err != nil {
		return err
	}
	return p.tx.WriteUint64(p.blk.Hash, offset, val, true)
}

func (p *recordPage) writeString(slot slotNum, fieldName string, val string) error {
	offset, err := p.offset(slot, fieldName)
	if err != nil {
		return err
	}
	return p.tx.WriteString(p.blk.Hash, offset, val, true)
}

func (p *recordPage) delete(slot slotNum) error {
	return p.setToFree(slot, true)
}

func (p *recordPage) format() error {
	var slot slotNum = 0
	for {
		valid, err := p.validSlot(slot)
		if err != nil {
			return err
		}
		if !valid {
			break
		}
		err = p.formatSlot(slot)
		if err != nil {
			return err
		}
		slot++
	}
	return nil
}

func (p *recordPage) formatSlot(slot slotNum) error {
	err := p.setToFree(slot, false)
	if err != nil {
		return err
	}
	for _, f := range p.layout.schema.fields {
		offset, err := p.offset(slot, f.name)
		if err != nil {
			return err
		}
		switch f.ty {
		case fieldTypeInt64:
			err = p.tx.WriteInt64(p.blk.Hash, offset, 0, false)
		case fieldTypeUint64:
			err = p.tx.WriteUint64(p.blk.Hash, offset, 0, false)
		case fieldTypeString:
			err = p.tx.WriteString(p.blk.Hash, offset, "", false)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *recordPage) insertAfter(slot slotNum) (slotNum, error) {
	newSlot, err := p.findFreeSlotAfter(slot)
	if err != nil {
		return 0, err
	}
	err = p.setToUsed(newSlot, true)
	if err != nil {
		return 0, err
	}
	return newSlot, nil
}

func (p *recordPage) findFreeSlotAfter(slot slotNum) (slotNum, error) {
	return p.findSlotAfter(slot, false)
}

func (p *recordPage) nextUsedSlotAfter(slot slotNum) (slotNum, error) {
	return p.findSlotAfter(slot, true)
}

func (p *recordPage) findSlotAfter(slot slotNum, used bool) (slotNum, error) {
	s := slot + 1
	for {
		valid, err := p.validSlot(s)
		if err != nil {
			return 0, err
		}
		if !valid {
			return 0, errRecPageSlotOutOfRange
		}

		offset, err := p.offset(s, "")
		if err != nil {
			return 0, err
		}
		v, err := p.tx.ReadInt64(p.blk.Hash, offset)
		if err != nil {
			return 0, err
		}
		if used {
			if v == 1 {
				return s, nil
			}
		} else {
			if v == 0 {
				return s, nil
			}
		}

		s++
	}
}

func (p *recordPage) setToFree(slot slotNum, log bool) error {
	offset, err := p.offset(slot, "")
	if err != nil {
		return err
	}
	return p.tx.WriteInt64(p.blk.Hash, offset, 0, log)
}

func (p *recordPage) setToUsed(slot slotNum, log bool) error {
	offset, err := p.offset(slot, "")
	if err != nil {
		return err
	}
	return p.tx.WriteInt64(p.blk.Hash, offset, 1, log)
}

func (p *recordPage) offset(slot slotNum, fieldName string) (int, error) {
	if slot < 0 {
		return 0, fmt.Errorf("a negative slot number is invalid: %v", slot)
	}

	slotOffset := int(slot) * p.layout.slotSize
	if fieldName == "" {
		return slotOffset, nil
	}
	fieldOffset, err := p.layout.offset(fieldName)
	if err != nil {
		return 0, err
	}
	return slotOffset + fieldOffset, nil
}

func (p *recordPage) validSlot(slot slotNum) (bool, error) {
	offset, err := p.offset(slot+1, "")
	if err != nil {
		return false, err
	}
	return offset <= p.tx.BlockSize(), nil
}
