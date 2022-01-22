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

type QueryStringer interface {
	QueryString() (string, error)
}

var (
	_ QueryStringer = &SelectStament{}
	_ QueryStringer = &CreateTableStatement{}
	_ QueryStringer = &CreateViewStatement{}
)

type SelectStament struct {
	Fields    []string
	Tables    []string
	Predicate *scanner.Predicate
}

func (s *SelectStament) QueryString() (string, error) {
	var b strings.Builder

	fmt.Fprintf(&b, "select %v", s.Fields[0])
	for _, f := range s.Fields[1:] {
		fmt.Fprintf(&b, ", %v", f)
	}
	fmt.Fprintf(&b, "from %v", s.Tables[0])
	for _, t := range s.Tables[1:] {
		fmt.Fprintf(&b, ", %v", t)
	}
	if s.Predicate != nil {
		lhs, err := expressionToString(s.Predicate.Terms[0].LHS)
		if err != nil {
			return "", fmt.Errorf("failed to convert an expression to a string: %v", err)
		}
		rhs, err := expressionToString(s.Predicate.Terms[0].RHS)
		if err != nil {
			return "", fmt.Errorf("failed to convert an expression to a string: %v", err)
		}
		fmt.Fprintf(&b, "where %v = %v", lhs, rhs)
		for _, term := range s.Predicate.Terms[1:] {
			lhs, err := expressionToString(term.LHS)
			if err != nil {
				return "", fmt.Errorf("failed to convert an expression to a string: %v", err)
			}
			rhs, err := expressionToString(term.RHS)
			if err != nil {
				return "", fmt.Errorf("failed to convert an expression to a string: %v", err)
			}
			fmt.Fprintf(&b, "and %v = %v", lhs, rhs)
		}
	}

	return b.String(), nil
}

func expressionToString(expr scanner.Expression) (string, error) {
	if c, ok := expr.AsConstant(); ok {
		if v, ok := c.AsInt64(); ok {
			return strconv.FormatInt(v, 10), nil
		}
		if v, ok := c.AsUint64(); ok {
			return strconv.FormatUint(v, 10), nil
		}
		if v, ok := c.AsString(); ok {
			return v, nil
		}
	}
	if f, ok := expr.AsFieldName(); ok {
		return f, nil
	}
	return "", fmt.Errorf("invalid expression type: %T", expr)
}

type CreateTableStatement struct {
	Table  string
	Schema *table.Schema
}

func (s *CreateTableStatement) QueryString() (string, error) {
	return "", nil
}

type CreateViewStatement struct {
	View  string
	Query QueryStringer
}

func (s *CreateViewStatement) QueryString() (string, error) {
	return "", nil
}

type InsertStatement struct {
	Table  string
	Fields []string
	Values []scanner.Constant
}

func (s *InsertStatement) QueryString() (string, error) {
	return "", nil
}

func Parse(src io.Reader) (QueryStringer, error) {
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
	root := treeAct.AST()
	switch root.Children[0].KindName {
	case "select_statement":
		return astToSelectStatement(root.Children[0])
	case "create_table_statement":
		return astToCreateTableStatement(root.Children[0])
	case "create_view_statement":
		return astToCreateViewStatement(root.Children[0])
	case "insert_statement":
		return astToInsertStatement(root.Children[0])
	}
	return nil, fmt.Errorf("invalid command")
}

func astToSelectStatement(selectStmt *driver.Node) (*SelectStament, error) {
	stmt := &SelectStament{}
	fieldList := selectStmt.Children[0]
	for _, field := range fieldList.Children {
		stmt.Fields = append(stmt.Fields, field.Children[0].Text)
	}
	tableList := selectStmt.Children[1]
	for _, table := range tableList.Children {
		stmt.Tables = append(stmt.Tables, table.Text)
	}
	if len(selectStmt.Children) >= 3 {
		predicate := selectStmt.Children[2]
		stmt.Predicate = scanner.NewPredicate(nil)
		for _, term := range predicate.Children {
			lExp, err := astToExpression(term.Children[0])
			if err != nil {
				return nil, err
			}
			rExp, err := astToExpression(term.Children[1])
			if err != nil {
				return nil, err
			}
			stmt.Predicate.AppendTerm(scanner.NewTerm(lExp, rExp))
		}
	}
	return stmt, nil
}

func astToCreateTableStatement(createTableStmt *driver.Node) (*CreateTableStatement, error) {
	stmt := &CreateTableStatement{
		Schema: table.NewShcema(),
	}

	stmt.Table = createTableStmt.Children[0].Text

	fieldDefList := createTableStmt.Children[1]
	for _, fieldDef := range fieldDefList.Children {
		typeDef := fieldDef.Children[1]
		var f *table.Field
		switch typeDef.Children[0].KindName {
		case "kw_int":
			f = table.NewInt64Field()
		case "kw_varchar":
			n, err := strconv.Atoi(typeDef.Children[1].Text)
			if err != nil {
				return nil, err
			}
			f = table.NewStringField(n)
		default:
			return nil, fmt.Errorf("invalid field type: %T", typeDef.Children[0].Text)
		}
		stmt.Schema.Add(fieldDef.Children[0].Text, f)
	}

	return stmt, nil
}

func astToCreateViewStatement(createViewStmt *driver.Node) (*CreateViewStatement, error) {
	stmt := &CreateViewStatement{}

	stmt.View = createViewStmt.Children[0].Text

	selectStmt := createViewStmt.Children[1]
	var err error
	stmt.Query, err = astToSelectStatement(selectStmt)
	if err != nil {
		return nil, err
	}

	return stmt, nil
}

func astToInsertStatement(insertStmt *driver.Node) (*InsertStatement, error) {
	stmt := &InsertStatement{
		Fields: make([]string, len(insertStmt.Children[1].Children)),
		Values: make([]scanner.Constant, len(insertStmt.Children[2].Children)),
	}

	stmt.Table = insertStmt.Children[0].Text

	fieldList := insertStmt.Children[1]
	for i, field := range fieldList.Children {
		stmt.Fields[i] = field.Children[0].Text
	}

	constantList := insertStmt.Children[2]
	for i, constant := range constantList.Children {
		c, err := astToConstant(constant)
		if err != nil {
			return nil, err
		}
		stmt.Values[i] = c
	}

	return stmt, nil
}

func astToExpression(ast *driver.Node) (scanner.Expression, error) {
	switch ast.Children[0].KindName {
	case "field":
		return scanner.NewFieldNameExpression(ast.Children[0].Children[0].Text), nil
	case "constant":
		c, err := astToConstant(ast.Children[0])
		if err != nil {
			return nil, err
		}
		return scanner.NewConstantExpression(c), nil
	default:
		return nil, fmt.Errorf("invalid node type")
	}
}

func astToConstant(ast *driver.Node) (scanner.Constant, error) {
	switch ast.Children[0].KindName {
	case "string":
		text := ast.Children[0].Text
		return scanner.NewStringConstant(strings.TrimRight(strings.TrimLeft(text, "'"), "'")), nil
	case "integer":
		v, err := strconv.Atoi(ast.Children[0].Text)
		if err != nil {
			return nil, err
		}
		return scanner.NewInt64Constant(int64(v)), nil
	default:
		return nil, fmt.Errorf("invalid constant type")
	}
}
