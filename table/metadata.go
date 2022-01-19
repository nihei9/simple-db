package table

import (
	"fmt"
	"sync"

	"github.com/nihei9/simple-db/storage"
)

type MetadataManager struct {
	tm *tableManager
	vm *viewManager
	sm *statisticManager
}

func NewMetadataManager(isNew bool, tx *storage.Transaction) (*MetadataManager, error) {
	tm, err := newTableManager(isNew, tx)
	if err != nil {
		return nil, err
	}

	vm, err := newViewManager(isNew, tx, tm)
	if err != nil {
		return nil, err
	}

	sm := newStatisticManager(tx, tm)
	if err != nil {
		return nil, err
	}

	return &MetadataManager{
		tm: tm,
		vm: vm,
		sm: sm,
	}, nil
}

func (m *MetadataManager) CreateTable(tx *storage.Transaction, tabName string, sc *Schema) error {
	return m.tm.createTable(tx, tabName, sc)
}

func (m *MetadataManager) FindLayout(tx *storage.Transaction, tabName string) (*Layout, error) {
	return m.tm.findLayout(tx, tabName)
}

func (m *MetadataManager) CreateView(tx *storage.Transaction, viewName string, viewDef string) error {
	return m.vm.createView(tx, viewName, viewDef)
}

func (m *MetadataManager) FindViewDef(tx *storage.Transaction, viewName string) (string, error) {
	return m.vm.findViewDef(tx, viewName)
}

func (m *MetadataManager) TableStatistic(tx *storage.Transaction, tableName string) (*TableStat, error) {
	return m.sm.tableStat(tx, tableName)
}

type tableManager struct {
	tabCatLayout *Layout
	fldCatLayout *Layout
}

func newTableManager(isNew bool, tx *storage.Transaction) (*tableManager, error) {
	tabCatSchema := NewShcema()
	tabCatSchema.Add("table_name", NewStringField(64))
	tabCatSchema.Add("slot_size", NewInt64Field())

	fldCatSchema := NewShcema()
	fldCatSchema.Add("table_name", NewStringField(64))
	fldCatSchema.Add("field_name", NewStringField(64))
	fldCatSchema.Add("type", NewInt64Field())
	fldCatSchema.Add("length", NewInt64Field())
	fldCatSchema.Add("offset", NewInt64Field())

	m := &tableManager{
		tabCatLayout: NewLayout(tabCatSchema),
		fldCatLayout: NewLayout(fldCatSchema),
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

func (m *tableManager) createTable(tx *storage.Transaction, tabName string, sc *Schema) error {
	la := NewLayout(sc)

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
		err = fldCat.WriteString("type", string(f.Ty))
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

func (m *tableManager) findLayout(tx *storage.Transaction, tabName string) (*Layout, error) {
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

	sc := NewShcema()
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

				var fld *Field
				switch FieldType(ty) {
				case FieldTypeInt64:
					fld = NewInt64Field()
				case FieldTypeUint64:
					fld = NewUint64Field()
				case FieldTypeString:
					fld = NewStringField(int(length))
				default:
					return nil, fmt.Errorf("invalid field type: %v", ty)
				}
				sc.Add(name, fld)

				offsets[name] = int(offset)
			}
		}
	}

	return &Layout{
		Schema:   sc,
		offsets:  offsets,
		slotSize: slotSize,
	}, nil
}

type viewManager struct {
	tm *tableManager
}

func newViewManager(isNew bool, tx *storage.Transaction, tm *tableManager) (*viewManager, error) {
	if isNew {
		sc := NewShcema()
		sc.Add("view_name", NewStringField(100))
		sc.Add("view_def", NewStringField(100))
		err := tm.createTable(tx, "view_catalog", sc)
		if err != nil {
			return nil, err
		}
	}

	return &viewManager{
		tm: tm,
	}, nil
}

func (m *viewManager) createView(tx *storage.Transaction, viewName string, viewDef string) error {
	la, err := m.tm.findLayout(tx, "view_catalog")
	if err != nil {
		return err
	}
	viewCat, err := NewTableScanner(tx, "view_catalog", la)
	if err != nil {
		return err
	}
	defer viewCat.Close()
	err = viewCat.BeforeFirst()
	if err != nil {
		return err
	}
	err = viewCat.Insert()
	if err != nil {
		return err
	}
	err = viewCat.WriteString("view_name", viewName)
	if err != nil {
		return err
	}
	err = viewCat.WriteString("view_def", viewDef)
	if err != nil {
		return err
	}
	return nil
}

func (m *viewManager) findViewDef(tx *storage.Transaction, viewName string) (string, error) {
	la, err := m.tm.findLayout(tx, "view_catalog")
	if err != nil {
		return "", err
	}
	viewCat, err := NewTableScanner(tx, "view_catalog", la)
	if err != nil {
		return "", err
	}
	defer viewCat.Close()
	err = viewCat.BeforeFirst()
	if err != nil {
		return "", err
	}
	for {
		ok, err := viewCat.Next()
		if err != nil {
			return "", err
		}
		if !ok {
			return "", nil
		}
		name, err := viewCat.ReadString("view_name")
		if err != nil {
			return "", err
		}
		if name != viewName {
			continue
		}
		return viewCat.ReadString("view_def")
	}
}

type TableStat struct {
	BlockCount         int
	RecordCount        int
	DistinctValueCount int
}

type statisticManager struct {
	tm        *tableManager
	mu        sync.Mutex
	stats     map[string]*TableStat
	callCount int
}

func newStatisticManager(tx *storage.Transaction, tm *tableManager) *statisticManager {
	return &statisticManager{
		tm:        tm,
		callCount: 0,
	}
}

func (m *statisticManager) tableStat(tx *storage.Transaction, tableName string) (*TableStat, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCount++
	if m.callCount > 100 {
		err := m.refreshStatistics(tx)
		if err != nil {
			return nil, err
		}
	}
	stat, ok := m.stats[tableName]
	if !ok {
		la, err := m.tm.findLayout(tx, tableName)
		if err != nil {
			return nil, err
		}
		stat, err = m.calcTableStat(tx, tableName, la)
		if err != nil {
			return nil, err
		}
	}
	return stat, nil
}

func (m *statisticManager) refreshStatistics(tx *storage.Transaction) error {
	tabCatLayout, err := m.tm.findLayout(tx, "table_catalog")
	if err != nil {
		return err
	}
	tabCat, err := NewTableScanner(tx, "table_catalog", tabCatLayout)
	if err != nil {
		return err
	}
	defer tabCat.Close()
	stats := map[string]*TableStat{}
	for {
		ok, err := tabCat.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		tabName, err := tabCat.ReadString("table_name")
		if err != nil {
			return err
		}
		la, err := m.tm.findLayout(tx, tabName)
		if err != nil {
			break
		}
		stat, err := m.calcTableStat(tx, tabName, la)
		if err != nil {
			return err
		}
		stats[tabName] = stat
	}
	m.stats = stats
	m.callCount = 0
	return nil
}

func (m *statisticManager) calcTableStat(tx *storage.Transaction, tableName string, layout *Layout) (*TableStat, error) {
	blkCount := 0
	recCount := 0
	tab, err := NewTableScanner(tx, tableName, layout)
	if err != nil {
		return nil, err
	}
	defer tab.Close()
	for {
		ok, err := tab.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		recCount++
		rid, ok := tab.RecordID()
		if !ok {
			return nil, fmt.Errorf("failed to get a record id")
		}
		blkCount = rid.blkNum + 1
	}
	return &TableStat{
		BlockCount:         blkCount,
		RecordCount:        recCount,
		DistinctValueCount: 1 + (recCount / 3), // FIXME
	}, nil
}
