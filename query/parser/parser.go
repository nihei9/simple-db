//go:generate vartan compile -g query.vr -o query.json

package parser

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/nihei9/vartan/driver"
	"github.com/nihei9/vartan/spec"
)

//go:embed query.json
var grammarJSON []byte

var grammar *spec.CompiledGrammar

func init() {
	var g spec.CompiledGrammar
	_ = json.Unmarshal(grammarJSON, &g)
	grammar = &g
}

type Constant struct {
	ValInt    *int
	ValString *string
}

type Expression struct {
	Field    string
	Constant *Constant
}

type Term struct {
	LHS *Expression
	RHS *Expression
}

type Query struct {
	Fields    []string
	Tables    []string
	Predicate []*Term
}

func Parse(src io.Reader) (*Query, error) {
	treeAct := driver.NewSyntaxTreeActionSet(grammar, true, false)
	opts := []driver.ParserOption{
		driver.SemanticAction(treeAct),
	}
	p, err := driver.NewParser(grammar, src, opts...)
	if err != nil {
		return nil, err
	}
	err = p.Parse()
	if err != nil {
		return nil, err
	}
	synErrs := p.SyntaxErrors()
	if len(synErrs) > 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "syntax error:")
		for _, synErr := range synErrs {
			fmt.Fprintf(&b, "\n%v:%v: %v", synErr.Row, synErr.Col, synErr.Message)
		}
		return nil, errors.New(b.String())
	}
	return astToQuery(treeAct.AST())
}

func astToQuery(root *driver.Node) (*Query, error) {
	q := &Query{}
	selectStmt := root.Children[0]
	fieldList := selectStmt.Children[0]
	for _, field := range fieldList.Children {
		q.Fields = append(q.Fields, field.Children[0].Text)
	}
	tableList := selectStmt.Children[1]
	for _, table := range tableList.Children {
		q.Tables = append(q.Tables, table.Text)
	}
	if len(selectStmt.Children) >= 3 {
		predicate := selectStmt.Children[2]
		for _, term := range predicate.Children {
			lExp, err := astToExpression(term.Children[0])
			if err != nil {
				return nil, err
			}
			rExp, err := astToExpression(term.Children[1])
			if err != nil {
				return nil, err
			}
			q.Predicate = append(q.Predicate, &Term{
				LHS: lExp,
				RHS: rExp,
			})
		}
	}
	return q, nil
}

func astToExpression(ast *driver.Node) (*Expression, error) {
	switch ast.Children[0].KindName {
	case "field":
		return &Expression{
			Field: ast.Children[0].Children[0].Text,
		}, nil
	case "constant":
		switch ast.Children[0].Children[0].KindName {
		case "string":
			return &Expression{
				Constant: &Constant{
					ValString: &ast.Children[0].Children[0].Text,
				},
			}, nil
		case "integer":
			v, err := strconv.Atoi(ast.Children[0].Children[0].Text)
			if err != nil {
				return nil, err
			}
			return &Expression{
				Constant: &Constant{
					ValInt: &v,
				},
			}, nil
		default:
			return nil, fmt.Errorf("invalid constant type")
		}
	default:
		return nil, fmt.Errorf("invalid node type")
	}
}
