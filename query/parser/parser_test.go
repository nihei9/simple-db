package parser

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
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
				t.Fatal("Parse must not return an error")
			}
		})
	}
}
