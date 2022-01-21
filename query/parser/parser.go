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

	"github.com/nihei9/simple-db/query/scanner"
	"github.com/nihei9/simple-db/table"
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

type Query struct {
	Fields    []string
	Tables    []string
	Predicate *scanner.Predicate
}

type CreateTable struct {
	Table  string
	Schema *table.Schema
}

func Parse(src io.Reader) (*Query, *CreateTable, error) {
	treeAct := driver.NewSyntaxTreeActionSet(grammar, true, false)
	opts := []driver.ParserOption{
		driver.SemanticAction(treeAct),
	}
	p, err := driver.NewParser(grammar, src, opts...)
	if err != nil {
		return nil, nil, err
	}
	err = p.Parse()
	if err != nil {
		return nil, nil, err
	}
	synErrs := p.SyntaxErrors()
	if len(synErrs) > 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "syntax error:")
		for _, synErr := range synErrs {
			fmt.Fprintf(&b, "\n%v:%v: %v", synErr.Row, synErr.Col, synErr.Message)
		}
		return nil, nil, errors.New(b.String())
	}
	root := treeAct.AST()
	switch root.Children[0].KindName {
	case "select_statement":
		q, err := astToQuery(root)
		if err != nil {
			return nil, nil, err
		}
		return q, nil, nil
	case "create_table_statement":
		ct, err := astToCreateTable(root)
		if err != nil {
			return nil, nil, err
		}
		return nil, ct, nil
	}
	return nil, nil, fmt.Errorf("invalid command")
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
		q.Predicate = scanner.NewPredicate(nil)
		for _, term := range predicate.Children {
			lExp, err := astToExpression(term.Children[0])
			if err != nil {
				return nil, err
			}
			rExp, err := astToExpression(term.Children[1])
			if err != nil {
				return nil, err
			}
			q.Predicate.AppendTerm(scanner.NewTerm(lExp, rExp))
		}
	}
	return q, nil
}

func astToExpression(ast *driver.Node) (scanner.Expression, error) {
	switch ast.Children[0].KindName {
	case "field":
		return scanner.NewFieldNameExpression(ast.Children[0].Children[0].Text), nil
	case "constant":
		switch ast.Children[0].Children[0].KindName {
		case "string":
			return scanner.NewConstantExpression(scanner.NewStringConstant(ast.Children[0].Children[0].Text)), nil
		case "integer":
			v, err := strconv.Atoi(ast.Children[0].Children[0].Text)
			if err != nil {
				return nil, err
			}
			return scanner.NewConstantExpression(scanner.NewInt64Constant(int64(v))), nil
		default:
			return nil, fmt.Errorf("invalid constant type")
		}
	default:
		return nil, fmt.Errorf("invalid node type")
	}
}

func astToCreateTable(root *driver.Node) (*CreateTable, error) {
	ct := &CreateTable{
		Schema: table.NewShcema(),
	}

	createTableStmt := root.Children[0]
	ct.Table = createTableStmt.Children[0].Text

	if len(createTableStmt.Children) <= 3 {
		return ct, nil
	}

	fieldDefList := createTableStmt.Children[1]
	for _, fieldDef := range fieldDefList.Children {
		typeDef := fieldDef.Children[1]
		var f *table.Field
		switch typeDef.Children[0].KindName {
		case "int":
			f = table.NewInt64Field()
		case "varchar":
			n, err := strconv.Atoi(typeDef.Children[1].Text)
			if err != nil {
				return nil, err
			}
			f = table.NewStringField(n)
		}
		ct.Schema.Add(fieldDef.Children[0].Text, f)
	}

	return ct, nil
}
