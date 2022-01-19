package planner

import (
	"io"
	"strings"

	"github.com/nihei9/simple-db/query/parser"
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

func (p *BasicQueryPlanner) createPlan(tx *storage.Transaction, query *parser.Query) (Plan, error) {
	tabPlans := make([]Plan, len(query.Tables))
	for i, tab := range query.Tables {
		viewDef, err := p.mm.FindViewDef(tx, tab)
		if err != nil {
			return nil, err
		}
		if viewDef != "" {
			viewQuery, err := parser.Parse(strings.NewReader(viewDef))
			if err != nil {
				return nil, err
			}
			tabPlans[i], err = p.createPlan(tx, viewQuery)
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
	for _, tabPlan := range tabPlans {
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

	selectPlan, err := NewSelectPlan(prodPlan, query.Predicate)
	if err != nil {
		return nil, err
	}

	return NewProjectPlan(selectPlan, query.Fields)
}

type Planner struct {
	qp *BasicQueryPlanner
}

func NewPlanner(qp *BasicQueryPlanner) *Planner {
	return &Planner{
		qp: qp,
	}
}

func (p *Planner) CreateQueryPlan(tx *storage.Transaction, cmd io.Reader) (Plan, error) {
	q, err := parser.Parse(cmd)
	if err != nil {
		return nil, err
	}
	return p.qp.createPlan(tx, q)
}
