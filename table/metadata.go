package table

import (
	"fmt"

	"github.com/nihei9/simple-db/storage"
)

type MetadataManager struct {
	tm *tableManager
}

func NewMetadataManager(isNew bool, tx *storage.Transaction) (*MetadataManager, error) {
	tm, err := newTableManager(isNew, tx)
	if err != nil {
		return nil, err
	}

	return &MetadataManager{
		tm: tm,
	}, nil
}

func (m *MetadataManager) CreateTable(tx *storage.Transaction, tabName string, sc *schema) error {
	return m.tm.createTable(tx, tabName, sc)
}

func (m *MetadataManager) FindLayout(tx *storage.Transaction, tabName string) (*layout, error) {
	return m.tm.findLayout(tx, tabName)
}

type tableManager struct {
	tabCatLayout *layout
	fldCatLayout *layout
}

func newTableManager(isNew bool, tx *storage.Transaction) (*tableManager, error) {
	tabCatSchema := newShcema()
	tabCatSchema.add("table_name", newStringField(64))
	tabCatSchema.add("slot_size", newInt64Field())

	fldCatSchema := newShcema()
	fldCatSchema.add("table_name", newStringField(64))
	fldCatSchema.add("field_name", newStringField(64))
	fldCatSchema.add("type", newInt64Field())
	fldCatSchema.add("length", newInt64Field())
	fldCatSchema.add("offset", newInt64Field())

	m := &tableManager{
		tabCatLayout: newLayout(tabCatSchema),
		fldCatLayout: newLayout(fldCatSchema),
	}

	if isNew {
		err := m.createTable(tx, "table_catalog", tabCatSchema)
		if err != nil {
			return nil, err
		}
		err = m.createTable(tx, "field_catalog", fldCatSchema)
		if err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *tableManager) createTable(tx *storage.Transaction, tabName string, sc *schema) error {
	la := newLayout(sc)

	tabCat, err := NewTableScanner(tx, "table_catalog", m.tabCatLayout)
	if err != nil {
		return err
	}
	defer tabCat.Close()

	err = tabCat.Insert()
	if err != nil {
		return err
	}
	err = tabCat.WriteString("table_name", tabName)
	if err != nil {
		return err
	}
	err = tabCat.WriteInt64("slot_size", int64(la.slotSize))
	if err != nil {
		return err
	}

	fldCat, err := NewTableScanner(tx, "field_catalog", m.fldCatLayout)
	if err != nil {
		return err
	}
	defer fldCat.Close()
	for _, f := range sc.fields {
		err := fldCat.Insert()
		if err != nil {
			return err
		}

		err = fldCat.WriteString("table_name", tabName)
		if err != nil {
			return err
		}
		err = fldCat.WriteString("field_name", f.name)
		if err != nil {
			return err
		}
		err = fldCat.WriteString("type", string(f.ty))
		if err != nil {
			return err
		}
		err = fldCat.WriteInt64("length", int64(f.length))
		if err != nil {
			return err
		}
		err = fldCat.WriteInt64("offset", int64(la.offsets[f.name]))
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *tableManager) findLayout(tx *storage.Transaction, tabName string) (*layout, error) {
	var slotSize int
	{
		tabCat, err := NewTableScanner(tx, "table_catalog", m.tabCatLayout)
		if err != nil {
			return nil, err
		}
		defer tabCat.Close()
		for {
			ok, err := tabCat.Next()
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("a table was not found in the table_catalog: %v", tabName)
			}

			n, err := tabCat.ReadString("table_name")
			if err != nil {
				return nil, err
			}
			if n == tabName {
				s, err := tabCat.ReadInt64("slot_size")
				if err != nil {
					return nil, err
				}
				slotSize = int(s)
				break
			}
		}
	}

	sc := newShcema()
	offsets := map[string]int{}
	{
		fldCat, err := NewTableScanner(tx, "field_catalog", m.fldCatLayout)
		if err != nil {
			return nil, err
		}
		defer fldCat.Close()
		for {
			ok, err := fldCat.Next()
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}

			n, err := fldCat.ReadString("table_name")
			if err != nil {
				return nil, err
			}
			if n == tabName {
				name, err := fldCat.ReadString("field_name")
				if err != nil {
					return nil, err
				}
				ty, err := fldCat.ReadString("type")
				if err != nil {
					return nil, err
				}
				length, err := fldCat.ReadInt64("length")
				if err != nil {
					return nil, err
				}
				offset, err := fldCat.ReadInt64("offset")
				if err != nil {
					return nil, err
				}

				var fld *field
				switch ty {
				case fieldTypeInt64:
					fld = newInt64Field()
				case fieldTypeUint64:
					fld = newUint64Field()
				case fieldTypeString:
					fld = newStringField(int(length))
				default:
					return nil, fmt.Errorf("invalid field type: %v", ty)
				}
				sc.add(name, fld)

				offsets[name] = int(offset)
			}
		}
	}

	return &layout{
		schema:   sc,
		offsets:  offsets,
		slotSize: slotSize,
	}, nil
}
