package parser

import (
	"strings"
	"testing"
)

func TestParse_Select(t *testing.T) {
	tests := []struct {
		src             string
		isInvalidSyntax bool
	}{
		{
			src: `select foo from bar`,
		},
		{
			src: `select foo, bar from baz`,
		},
		{
			src: `select foo, bar from baz, bra`,
		},
		{
			src: `select foo from bar where baz = 100`,
		},
		{
			src: `select foo from bar where baz = 'xxx'`,
		},
		{
			src: `select foo from bar where baz = bra`,
		},
		{
			src: `select foo from bar where baz = 100 and bra = 'xxx'`,
		},
		{
			src: `select foo from bar where baz = bra`,
		},
		{
			src:             ``,
			isInvalidSyntax: true,
		},
		{
			src:             `select`,
			isInvalidSyntax: true,
		},
		{
			src:             `select foo`,
			isInvalidSyntax: true,
		},
		{
			src:             `select foo from`,
			isInvalidSyntax: true,
		},
		{
			src:             `select foo, from bar`,
			isInvalidSyntax: true,
		},
		{
			src:             `select , foo from bar`,
			isInvalidSyntax: true,
		},
		{
			src:             `select foo from bar,`,
			isInvalidSyntax: true,
		},
		{
			src:             `select foo from , bar`,
			isInvalidSyntax: true,
		},
		{
			src:             `select foo from bar where`,
			isInvalidSyntax: true,
		},
		{
			src:             `select foo from bar where baz`,
			isInvalidSyntax: true,
		},
		{
			src:             `select foo from bar where baz =`,
			isInvalidSyntax: true,
		},
		{
			src:             `select foo from bar where = baz`,
			isInvalidSyntax: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tt.src))
			if tt.isInvalidSyntax && err == nil {
				t.Fatal("Parse must return an error")
			} else if !tt.isInvalidSyntax && err != nil {
				t.Fatalf("Parse must not return an error: %v", err)
			}
		})
	}
}

func TestParse_CreateTable(t *testing.T) {
	tests := []struct {
		src             string
		isInvalidSyntax bool
	}{
		{
			src: `create table foo (bar int)`,
		},
		{
			src: `create table foo (bar int, baz varchar(100))`,
		},
		{
			src:             `create`,
			isInvalidSyntax: true,
		},
		{
			src:             `create table`,
			isInvalidSyntax: true,
		},
		{
			src:             `create table foo`,
			isInvalidSyntax: true,
		},
		{
			src:             `create table foo bar int`,
			isInvalidSyntax: true,
		},
		{
			src:             `create table foo (bar)`,
			isInvalidSyntax: true,
		},
		{
			src:             `create table foo (bar int,)`,
			isInvalidSyntax: true,
		},
		{
			src:             `create table foo (bar varchar)`,
			isInvalidSyntax: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tt.src))
			if tt.isInvalidSyntax && err == nil {
				t.Fatal("Parse must return an error")
			} else if !tt.isInvalidSyntax && err != nil {
				t.Fatalf("Parse must not return an error: %v", err)
			}
		})
	}
}

func TestParse_CreateView(t *testing.T) {
	tests := []struct {
		src             string
		isInvalidSyntax bool
	}{
		{
			src: `create view foo as select bar from baz where bra = 100`,
		},
		{
			src:             `create`,
			isInvalidSyntax: true,
		},
		{
			src:             `create view`,
			isInvalidSyntax: true,
		},
		{
			src:             `create view foo`,
			isInvalidSyntax: true,
		},
		{
			src:             `create view foo as`,
			isInvalidSyntax: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tt.src))
			if tt.isInvalidSyntax && err == nil {
				t.Fatal("Parse must return an error")
			} else if !tt.isInvalidSyntax && err != nil {
				t.Fatalf("Parse must not return an error: %v", err)
			}
		})
	}
}

func TestParse_Insert(t *testing.T) {
	tests := []struct {
		src             string
		isInvalidSyntax bool
	}{
		{
			src: `insert into foo(bar) values(100)`,
		},
		{
			src: `insert into foo(bar, baz) values(100, 'X')`,
		},
		{
			src:             `insert`,
			isInvalidSyntax: true,
		},
		{
			src:             `insert into`,
			isInvalidSyntax: true,
		},
		{
			src:             `insert into foo`,
			isInvalidSyntax: true,
		},
		{
			src:             `insert into foo(bar)`,
			isInvalidSyntax: true,
		},
		{
			src:             `insert into foo() values(100)`,
			isInvalidSyntax: true,
		},
		{
			src:             `insert into foo(bar) values()`,
			isInvalidSyntax: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tt.src))
			if tt.isInvalidSyntax && err == nil {
				t.Fatal("Parse must return an error")
			} else if !tt.isInvalidSyntax && err != nil {
				t.Fatalf("Parse must not return an error: %v", err)
			}
		})
	}
}

func TestParse_Delete(t *testing.T) {
	tests := []struct {
		src             string
		isInvalidSyntax bool
	}{
		{
			src: `delete from foo`,
		},
		{
			src: `delete from foo where bar = 100`,
		},
		{
			src:             `delete`,
			isInvalidSyntax: true,
		},
		{
			src:             `delete from`,
			isInvalidSyntax: true,
		},
		{
			src:             `delete from foo where`,
			isInvalidSyntax: true,
		},
		{
			src:             `delete from foo where bar`,
			isInvalidSyntax: true,
		},
		{
			src:             `delete from foo where bar =`,
			isInvalidSyntax: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tt.src))
			if tt.isInvalidSyntax && err == nil {
				t.Fatal("Parse must return an error")
			} else if !tt.isInvalidSyntax && err != nil {
				t.Fatalf("Parse must not return an error: %v", err)
			}
		})
	}
}

func TestParse_Update(t *testing.T) {
	tests := []struct {
		src             string
		isInvalidSyntax bool
	}{
		{
			src: `update foo set bar = 100`,
		},
		{
			src: `update foo set bar = 100 where baz = 'X'`,
		},
		{
			src:             `update`,
			isInvalidSyntax: true,
		},
		{
			src:             `update foo`,
			isInvalidSyntax: true,
		},
		{
			src:             `update foo set`,
			isInvalidSyntax: true,
		},
		{
			src:             `update foo set bar`,
			isInvalidSyntax: true,
		},
		{
			src:             `update foo set bar =`,
			isInvalidSyntax: true,
		},
		{
			src:             `update foo set bar = 100 where`,
			isInvalidSyntax: true,
		},
		{
			src:             `update foo set bar = 100 where baz`,
			isInvalidSyntax: true,
		},
		{
			src:             `update foo set bar = 100 where baz =`,
			isInvalidSyntax: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tt.src))
			if tt.isInvalidSyntax && err == nil {
				t.Fatal("Parse must return an error")
			} else if !tt.isInvalidSyntax && err != nil {
				t.Fatalf("Parse must not return an error: %v", err)
			}
		})
	}
}
