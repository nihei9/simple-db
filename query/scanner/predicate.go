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
	LHS Expression
	RHS Expression
}

func NewTerm(lhs, rhs Expression) *Term {
	return &Term{
		LHS: lhs,
		RHS: rhs,
	}
}

func (t *Term) isSatisfied(s Scanner) (bool, error) {
	l, err := t.LHS.Evaluate(s)
	if err != nil {
		return false, err
	}
	r, err := t.RHS.Evaluate(s)
	if err != nil {
		return false, err
	}
	return l.Equal(r), nil
}

func (t *Term) equateWithConstant(fieldName string) Constant {
	lf, isLHSFieldName := t.LHS.AsFieldName()
	rc, isRHSConstant := t.RHS.AsConstant()
	if isLHSFieldName && lf == fieldName && isRHSConstant {
		return rc
	}
	lc, isLHSConstant := t.LHS.AsConstant()
	rf, isRHSFieldName := t.RHS.AsFieldName()
	if isRHSFieldName && rf == fieldName && isLHSConstant {
		return lc
	}
	return nil
}

func (t *Term) equateWithField(fieldName string) string {
	lf, isLHSFieldName := t.LHS.AsFieldName()
	rf, isRHSFieldName := t.RHS.AsFieldName()
	if isLHSFieldName && lf == fieldName && isRHSFieldName {
		return rf
	}
	if isRHSFieldName && rf == fieldName && isLHSFieldName {
		return lf
	}
	return ""
}

type Predicate struct {
	Terms []*Term
}

func NewPredicate(t *Term) *Predicate {
	terms := []*Term{}
	if t != nil {
		terms = append(terms, t)
	}
	return &Predicate{
		Terms: terms,
	}
}

func (p *Predicate) AppendTerm(t *Term) {
	if t == nil {
		return
	}
	p.Terms = append(p.Terms, t)
}

func (p *Predicate) isSatisfied(s Scanner) (bool, error) {
	for _, t := range p.Terms {
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

func (p *Predicate) EquateWithConstant(fieldName string) Constant {
	for _, t := range p.Terms {
		c := t.equateWithConstant(fieldName)
		if c != nil {
			return c
		}
	}
	return nil
}

func (p *Predicate) EquateWithField(fieldName string) string {
	for _, t := range p.Terms {
		f := t.equateWithField(fieldName)
		if f != "" {
			return f
		}
	}
	return ""
}
