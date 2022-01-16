package planner

import (
	"github.com/nihei9/simple-db/query/scanner"
	"github.com/nihei9/simple-db/storage"
	"github.com/nihei9/simple-db/table"
)

type Plan interface {
	Open() (scanner.Scanner, error)
	BlockCount() int
	RecordCount() int
	DistinctValueCount(fieldName string) int
	Schema() *table.Schema
}

var (
	_ Plan = &tablePlan{}
	_ Plan = &selectPlan{}
)

type tablePlan struct {
	tx        *storage.Transaction
	tableName string
	layout    *table.Layout
	stat      *table.TableStat
}

func NewTablePlan(tx *storage.Transaction, tableName string, mm *table.MetadataManager) (*tablePlan, error) {
	layout, err := mm.FindLayout(tx, tableName)
	if err != nil {
		return nil, err
	}
	stat, err := mm.TableStatistic(tx, tableName)
	if err != nil {
		return nil, err
	}
	return &tablePlan{
		tx:        tx,
		tableName: tableName,
		layout:    layout,
		stat:      stat,
	}, nil
}

func (p *tablePlan) Open() (scanner.Scanner, error) {
	ts, err := table.NewTableScanner(p.tx, p.tableName, p.layout)
	if err != nil {
		return nil, err
	}
	return scanner.NewTableScanner(ts, p.layout.Schema), nil
}

func (p *tablePlan) BlockCount() int {
	return p.stat.BlockCount
}

func (p *tablePlan) RecordCount() int {
	return p.stat.RecordCount
}

func (p *tablePlan) DistinctValueCount(fieldName string) int {
	return p.stat.DistinctValueCount
}

func (p *tablePlan) Schema() *table.Schema {
	return p.layout.Schema
}

type selectPlan struct {
	plan Plan
	pred *scanner.Predicate
}

func NewSelectPlan(plan Plan, pred *scanner.Predicate) (*selectPlan, error) {
	return &selectPlan{
		plan: plan,
		pred: pred,
	}, nil
}

func (p *selectPlan) Open() (scanner.Scanner, error) {
	s, err := p.plan.Open()
	if err != nil {
		return nil, err
	}
	return scanner.NewSelectScanner(s, p.pred), nil
}

func (p *selectPlan) BlockCount() int {
	return p.plan.BlockCount()
}

func (p *selectPlan) RecordCount() int {
	return p.plan.RecordCount() / reductionFactor(p.pred, p.plan)
}

func (p *selectPlan) DistinctValueCount(fieldName string) int {
	if p.pred.EquateWithConstant(fieldName) != nil {
		return 1
	}
	if f := p.pred.EquateWithField(fieldName); f != "" {
		c1 := p.plan.DistinctValueCount(fieldName)
		c2 := p.plan.DistinctValueCount(f)
		if c1 >= c2 {
			return c1
		}
		return c2
	}
	return p.plan.DistinctValueCount(fieldName)
}

func (p *selectPlan) Schema() *table.Schema {
	return p.plan.Schema()
}

const (
	maxUint = ^uint(0)
	maxInt  = int(maxUint >> 1)
)

func reductionFactor(pred *scanner.Predicate, plan Plan) int {
	factor := 1
	for _, t := range pred.Terms {
		var f int
		lf, isLHSFieldName := t.LHS.AsFieldName()
		rf, isRHSFieldName := t.RHS.AsFieldName()
		switch {
		case isLHSFieldName && isRHSFieldName:
			lc := plan.DistinctValueCount(lf)
			rc := plan.DistinctValueCount(rf)
			if lc >= rc {
				f = lc
			} else {
				f = rc
			}
		case isLHSFieldName:
			f = plan.DistinctValueCount(lf)
		case isRHSFieldName:
			f = plan.DistinctValueCount(rf)
		default:
			lc, isLHSConstant := t.LHS.AsConstant()
			rc, isRHSConstant := t.RHS.AsConstant()
			if isLHSConstant && isRHSConstant && lc.Equal(rc) {
				f = 1
			} else {
				f = maxInt
			}
		}
		factor *= f
	}
	return factor
}
