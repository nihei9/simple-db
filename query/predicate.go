package query

type constant interface {
	asInt64() (int64, bool)
	asUint64() (uint64, bool)
	asString() (string, bool)
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

type expression interface {
	asConstant() (constant, bool)
	asFieldName() (string, bool)
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
