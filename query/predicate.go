package query

type constant interface {
	asInt64() (int64, bool)
	asUint64() (uint64, bool)
	asString() (string, bool)
	equal(v constant) bool
}

var (
	_ constant = newInt64Constant(0)
	_ constant = newUint64Constant(0)
	_ constant = newStringConstant("")
)

type (
	int64Constant  int64
	uint64Constant uint64
	stringConstant string
)

func newInt64Constant(v int64) int64Constant {
	return int64Constant(v)
}

func (c int64Constant) asInt64() (int64, bool) {
	return int64(c), true
}

func (c int64Constant) asUint64() (uint64, bool) {
	return 0, false
}

func (c int64Constant) asString() (string, bool) {
	return "", false
}

func (c int64Constant) equal(v constant) bool {
	a, _ := c.asInt64()
	b, ok := v.asInt64()
	return ok && a == b
}

func newUint64Constant(v uint64) uint64Constant {
	return uint64Constant(v)
}

func (c uint64Constant) asInt64() (int64, bool) {
	return 0, false
}

func (c uint64Constant) asUint64() (uint64, bool) {
	return uint64(c), true
}

func (c uint64Constant) asString() (string, bool) {
	return "", false
}

func (c uint64Constant) equal(v constant) bool {
	a, _ := c.asUint64()
	b, ok := v.asUint64()
	return ok && a == b
}

func newStringConstant(v string) stringConstant {
	return stringConstant(v)
}

func (c stringConstant) asInt64() (int64, bool) {
	return 0, false
}

func (c stringConstant) asUint64() (uint64, bool) {
	return 0, false
}

func (c stringConstant) asString() (string, bool) {
	return string(c), true
}

func (c stringConstant) equal(v constant) bool {
	a, _ := c.asString()
	b, ok := v.asString()
	return ok && a == b
}

type expression interface {
	asConstant() (constant, bool)
	asFieldName() (string, bool)
	evaluate(s scanner) (constant, error)
}

var (
	_ expression = &constantExpression{}
	_ expression = &fieldNameExpression{}
)

type constantExpression struct {
	c constant
}

func newConstantExpression(c constant) *constantExpression {
	return &constantExpression{
		c: c,
	}
}

func (e *constantExpression) asConstant() (constant, bool) {
	return e.c, true
}

func (e *constantExpression) asFieldName() (string, bool) {
	return "", false
}

func (e *constantExpression) evaluate(_ scanner) (constant, error) {
	return e.c, nil
}

type fieldNameExpression struct {
	name string
}

func newFieldNameExpression(name string) *fieldNameExpression {
	return &fieldNameExpression{
		name: name,
	}
}

func (e *fieldNameExpression) asConstant() (constant, bool) {
	return nil, false
}

func (e *fieldNameExpression) asFieldName() (string, bool) {
	return e.name, true
}

func (e *fieldNameExpression) evaluate(s scanner) (constant, error) {
	return s.Read(e.name)
}

type term struct {
	lhs expression
	rhs expression
}

func newTerm(lhs, rhs expression) *term {
	return &term{
		lhs: lhs,
		rhs: rhs,
	}
}

func (t *term) isSatisfied(s scanner) (bool, error) {
	l, err := t.lhs.evaluate(s)
	if err != nil {
		return false, err
	}
	r, err := t.rhs.evaluate(s)
	if err != nil {
		return false, err
	}
	return l.equal(r), nil
}

type predicate struct {
	terms []*term
}

func newPredicate(t *term) *predicate {
	return &predicate{
		terms: []*term{t},
	}
}

func (p *predicate) isSatisfied(s scanner) (bool, error) {
	for _, t := range p.terms {
		ok, err := t.isSatisfied(s)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}
