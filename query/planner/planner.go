package planner

import (
	"fmt"
	"io"
	"strings"

	"github.com/nihei9/simple-db/query/parser"
	"github.com/nihei9/simple-db/query/scanner"
	"github.com/nihei9/simple-db/storage"
	"github.com/nihei9/simple-db/table"
)

type BasicQueryPlanner struct {
	mm *table.MetadataManager
}

func NewBasicQueryPlanner(mm *table.MetadataManager) *BasicQueryPlanner {
	return &BasicQueryPlanner{
		mm: mm,
	}
}

func (p *BasicQueryPlanner) createPlan(tx *storage.Transaction, stmt *parser.SelectStament) (Plan, error) {
	tabPlans := make([]Plan, len(stmt.Tables))
	for i, tab := range stmt.Tables {
		viewDef, err := p.mm.FindViewDef(tx, tab)
		if err != nil {
			return nil, err
		}
		if viewDef != "" {
			viewQuery, err := parser.Parse(strings.NewReader(viewDef))
			if err != nil {
				return nil, err
			}
			tabPlans[i], err = p.createPlan(tx, viewQuery.(*parser.SelectStament))
			if err != nil {
				return nil, err
			}
		} else {
			var err error
			tabPlans[i], err = NewTablePlan(tx, tab, p.mm)
			if err != nil {
				return nil, err
			}
		}
	}

	prodPlan := tabPlans[0]
	for _, tabPlan := range tabPlans[1:] {
		p1, err := NewProductPlan(prodPlan, tabPlan)
		if err != nil {
			return nil, err
		}
		p2, err := NewProductPlan(tabPlan, prodPlan)
		if err != nil {
			return nil, err
		}
		if p1.BlockCount() <= p2.BlockCount() {
			prodPlan = p1
		} else {
			prodPlan = p2
		}
	}

	if stmt.Predicate == nil {
		return prodPlan, nil
	}

	selectPlan, err := NewSelectPlan(prodPlan, stmt.Predicate)
	if err != nil {
		return nil, err
	}

	return NewProjectPlan(selectPlan, stmt.Fields)
}

type BasicUpdatePlanner struct {
	mm *table.MetadataManager
}

func NewBasicUpdatePlanner(mm *table.MetadataManager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{
		mm: mm,
	}
}

func (p *BasicUpdatePlanner) executeCreateTable(tx *storage.Transaction, stmt *parser.CreateTableStatement) (int, error) {
	return 0, p.mm.CreateTable(tx, stmt.Table, stmt.Schema)
}

func (p *BasicUpdatePlanner) executeCreateView(tx *storage.Transaction, stmt *parser.CreateViewStatement) (int, error) {
	viewDef, err := stmt.Query.QueryString()
	if err != nil {
		return 0, err
	}
	return 0, p.mm.CreateView(tx, stmt.View, viewDef)
}

func (p *BasicUpdatePlanner) executeInsert(tx *storage.Transaction, stmt *parser.InsertStatement) (int, error) {
	plan, err := NewTablePlan(tx, stmt.Table, p.mm)
	if err != nil {
		return 0, err
	}
	s, err := plan.Open()
	if err != nil {
		return 0, err
	}
	defer s.Close()
	tab := s.(scanner.UpdateScanner)
	err = tab.BeforeFirst()
	if err != nil {
		return 0, err
	}
	err = tab.Insert()
	if err != nil {
		return 0, err
	}
	for i, f := range stmt.Fields {
		val := stmt.Values[i]
		if v, ok := val.AsInt64(); ok {
			err := tab.WriteInt64(f, v)
			if err != nil {
				return 0, err
			}
		}
		if v, ok := val.AsUint64(); ok {
			err := tab.WriteUint64(f, v)
			if err != nil {
				return 0, err
			}
		}
		if v, ok := val.AsString(); ok {
			err := tab.WriteString(f, v)
			if err != nil {
				return 0, err
			}
		}
	}
	return 1, nil
}

type Planner struct {
	qp *BasicQueryPlanner
	up *BasicUpdatePlanner
}

func NewPlanner(qp *BasicQueryPlanner, up *BasicUpdatePlanner) *Planner {
	return &Planner{
		qp: qp,
		up: up,
	}
}

func (p *Planner) CreateQueryPlan(tx *storage.Transaction, cmd io.Reader) (Plan, error) {
	q, err := parser.Parse(cmd)
	if err != nil {
		return nil, err
	}
	stmt, ok := q.(*parser.SelectStament)
	if !ok {
		return nil, fmt.Errorf("invalid query tyqe: %T", q)
	}
	return p.qp.createPlan(tx, stmt)
}

func (p *Planner) ExecuteUpdate(tx *storage.Transaction, cmd io.Reader) (int, error) {
	q, err := parser.Parse(cmd)
	if err != nil {
		return 0, err
	}
	switch stmt := q.(type) {
	case *parser.CreateTableStatement:
		return p.up.executeCreateTable(tx, stmt)
	case *parser.CreateViewStatement:
		return p.up.executeCreateView(tx, stmt)
	case *parser.InsertStatement:
		return p.up.executeInsert(tx, stmt)
	}
	return 0, fmt.Errorf("invalid query type: %T", q)
}
