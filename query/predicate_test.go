package query

import "testing"

func TestInt64Constant(t *testing.T) {
	c := newInt64Constant(-999)

	v, ok := c.asInt64()
	if !ok {
		t.Fatal("asInt64 must return `true`")
	}
	if v != -999 {
		t.Fatalf("unexpected value: want: %v, got: %v", -999, v)
	}

	_, ok = c.asUint64()
	if ok {
		t.Fatal("asUint64 must return `false`")
	}

	_, ok = c.asString()
	if ok {
		t.Fatal("asString must return `false`")
	}
}

func TestUint64Constant(t *testing.T) {
	c := newUint64Constant(2022)

	v, ok := c.asUint64()
	if !ok {
		t.Fatal("asUint64 must return `true`")
	}
	if v != 2022 {
		t.Fatalf("unexpected value: want: %v, got: %v", 2022, v)
	}

	_, ok = c.asInt64()
	if ok {
		t.Fatal("asInt64 must return `false`")
	}

	_, ok = c.asString()
	if ok {
		t.Fatal("asString must return `false`")
	}
}

func TestStringConstant(t *testing.T) {
	c := newStringConstant("Hello")

	v, ok := c.asString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if v != "Hello" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Hello", v)
	}

	_, ok = c.asInt64()
	if ok {
		t.Fatal("asInt64 must return `false`")
	}

	_, ok = c.asUint64()
	if ok {
		t.Fatal("asUint64 must return `false`")
	}
}

func TestConstantExpression(t *testing.T) {
	e := newConstantExpression(newStringConstant("Hello"))

	v, ok := e.asConstant()
	if !ok {
		t.Fatal("asConstant must return `true`")
	}
	if s, ok := v.asString(); !ok || s != "Hello" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Hello", s)
	}

	_, ok = e.asFieldName()
	if ok {
		t.Fatal("asFieldName must return `false`")
	}
}

func TestFieldNameExpression(t *testing.T) {
	e := newFieldNameExpression("Fox")

	v, ok := e.asFieldName()
	if !ok {
		t.Fatal("asFieldName must return `true`")
	}
	if v != "Fox" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Fox", v)
	}

	_, ok = e.asConstant()
	if ok {
		t.Fatal("asConstant must return `false`")
	}
}
