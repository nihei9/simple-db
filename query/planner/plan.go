package planner

import (
	"fmt"

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

type projectPlan struct {
	plan   Plan
	schema *table.Schema
}

func NewProjectPlan(plan Plan, fields []string) (*projectPlan, error) {
	sc := table.NewShcema()
	psc := plan.Schema()
	for _, name := range fields {
		f, ok := psc.Field(name)
		if !ok {
			return nil, fmt.Errorf("a field was not found: %v", name)
		}
		sc.Add(name, f)
	}

	return &projectPlan{
		plan:   plan,
		schema: sc,
	}, nil
}

func (p *projectPlan) Open() (scanner.Scanner, error) {
	s, err := p.plan.Open()
	if err != nil {
		return nil, err
	}
	return scanner.NewProjectScanner(s, p.schema.FieldNames()), nil
}

func (p *projectPlan) BlockCount() int {
	return p.plan.BlockCount()
}

func (p *projectPlan) RecordCount() int {
	return p.plan.RecordCount()
}

func (p *projectPlan) DistinctValueCount(fieldName string) int {
	return p.plan.DistinctValueCount(fieldName)
}

func (p *projectPlan) Schema() *table.Schema {
	return p.schema
}

type productPlan struct {
	plan1  Plan
	plan2  Plan
	schema *table.Schema
}

func NewProductPlan(plan1, plan2 Plan) (*productPlan, error) {
	sc := table.NewShcema()
	sc.AddSchema(plan1.Schema())
	sc.AddSchema(plan2.Schema())

	return &productPlan{
		plan1:  plan1,
		plan2:  plan2,
		schema: sc,
	}, nil
}

func (p *productPlan) Open() (scanner.Scanner, error) {
	s1, err := p.plan1.Open()
	if err != nil {
		return nil, err
	}
	s2, err := p.plan2.Open()
	if err != nil {
		return nil, err
	}
	return scanner.NewProductScanner(s1, s2), nil
}

func (p *productPlan) BlockCount() int {
	return p.plan1.BlockCount() + (p.plan1.RecordCount() * p.plan2.BlockCount())
}

func (p *productPlan) RecordCount() int {
	return p.plan1.RecordCount() * p.plan2.RecordCount()
}

func (p *productPlan) DistinctValueCount(fieldName string) int {
	if _, ok := p.plan1.Schema().Field(fieldName); ok {
		return p.plan1.DistinctValueCount(fieldName)
	}
	return p.plan2.DistinctValueCount(fieldName)
}

func (p *productPlan) Schema() *table.Schema {
	return p.schema
}
