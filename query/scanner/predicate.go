package scanner

type Constant interface {
	AsInt64() (int64, bool)
	AsUint64() (uint64, bool)
	AsString() (string, bool)
	Equal(v Constant) bool
}

var (
	_ Constant = NewInt64Constant(0)
	_ Constant = NewUint64Constant(0)
	_ Constant = NewStringConstant("")
)

type (
	int64Constant  int64
	uint64Constant uint64
	stringConstant string
)

func NewInt64Constant(v int64) int64Constant {
	return int64Constant(v)
}

func (c int64Constant) AsInt64() (int64, bool) {
	return int64(c), true
}

func (c int64Constant) AsUint64() (uint64, bool) {
	return 0, false
}

func (c int64Constant) AsString() (string, bool) {
	return "", false
}

func (c int64Constant) Equal(v Constant) bool {
	a, _ := c.AsInt64()
	b, ok := v.AsInt64()
	return ok && a == b
}

func NewUint64Constant(v uint64) uint64Constant {
	return uint64Constant(v)
}

func (c uint64Constant) AsInt64() (int64, bool) {
	return 0, false
}

func (c uint64Constant) AsUint64() (uint64, bool) {
	return uint64(c), true
}

func (c uint64Constant) AsString() (string, bool) {
	return "", false
}

func (c uint64Constant) Equal(v Constant) bool {
	a, _ := c.AsUint64()
	b, ok := v.AsUint64()
	return ok && a == b
}

func NewStringConstant(v string) stringConstant {
	return stringConstant(v)
}

func (c stringConstant) AsInt64() (int64, bool) {
	return 0, false
}

func (c stringConstant) AsUint64() (uint64, bool) {
	return 0, false
}

func (c stringConstant) AsString() (string, bool) {
	return string(c), true
}

func (c stringConstant) Equal(v Constant) bool {
	a, _ := c.AsString()
	b, ok := v.AsString()
	return ok && a == b
}

type Expression interface {
	AsConstant() (Constant, bool)
	AsFieldName() (string, bool)
	Evaluate(s Scanner) (Constant, error)
}

var (
	_ Expression = &constantExpression{}
	_ Expression = &fieldNameExpression{}
)

type constantExpression struct {
	c Constant
}

func NewConstantExpression(c Constant) *constantExpression {
	return &constantExpression{
		c: c,
	}
}

func (e *constantExpression) AsConstant() (Constant, bool) {
	return e.c, true
}

func (e *constantExpression) AsFieldName() (string, bool) {
	return "", false
}

func (e *constantExpression) Evaluate(_ Scanner) (Constant, error) {
	return e.c, nil
}

type fieldNameExpression struct {
	name string
}

func NewFieldNameExpression(name string) *fieldNameExpression {
	return &fieldNameExpression{
		name: name,
	}
}

func (e *fieldNameExpression) AsConstant() (Constant, bool) {
	return nil, false
}

func (e *fieldNameExpression) AsFieldName() (string, bool) {
	return e.name, true
}

func (e *fieldNameExpression) Evaluate(s Scanner) (Constant, error) {
	return s.Read(e.name)
}

type Term struct {
	lhs Expression
	rhs Expression
}

func NewTerm(lhs, rhs Expression) *Term {
	return &Term{
		lhs: lhs,
		rhs: rhs,
	}
}

func (t *Term) isSatisfied(s Scanner) (bool, error) {
	l, err := t.lhs.Evaluate(s)
	if err != nil {
		return false, err
	}
	r, err := t.rhs.Evaluate(s)
	if err != nil {
		return false, err
	}
	return l.Equal(r), nil
}

type Predicate struct {
	terms []*Term
}

func NewPredicate(t *Term) *Predicate {
	return &Predicate{
		terms: []*Term{t},
	}
}

func (p *Predicate) isSatisfied(s Scanner) (bool, error) {
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
