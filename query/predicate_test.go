package query

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nihei9/simple-db/storage"
	"github.com/nihei9/simple-db/table"
)

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

func TestPredicate(t *testing.T) {
	testDir, err := os.MkdirTemp("", "simple-db-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	var logFileName string
	var tmpTableName string
	{
		logFilePath, dbFilePath, err := makeTestLogFileAndDBFile(testDir)
		if err != nil {
			t.Fatal(err)
		}
		logFileName = filepath.Base(logFilePath)
		tmpTableName = strings.TrimSuffix(filepath.Base(dbFilePath), ".tbl")
	}

	st, err := storage.InitStorage(context.Background(), &storage.StorageConfig{
		DirPath:     testDir,
		LogFileName: logFileName,
		BlkSize:     400,
		BufSize:     10,
	})
	if err != nil {
		t.Fatal(err)
	}

	sc := table.NewShcema()
	sc.Add("name", table.NewStringField(10))

	la := table.NewLayout(sc)

	tx, err := st.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	// Write test data
	{
		s, err := table.NewTableScanner(tx, tmpTableName, la)
		if err != nil {
			t.Fatal(err)
		}
		err = s.BeforeFirst()
		if err != nil {
			t.Fatal(err)
		}

		err = s.Insert()
		if err != nil {
			t.Fatal(err)
		}
		err = s.WriteString("name", "John Doggett")
		if err != nil {
			t.Fatal(err)
		}

		err = s.Insert()
		if err != nil {
			t.Fatal(err)
		}
		err = s.WriteString("name", "Monica Reyes")
		if err != nil {
			t.Fatal(err)
		}
	}

	var ts scanner
	{
		s, err := table.NewTableScanner(tx, tmpTableName, la)
		if err != nil {
			t.Fatal(err)
		}
		ts = newTableScanner(s, sc)
	}

	pred := newPredicate(newTerm(
		newFieldNameExpression("name"),
		newConstantExpression(newStringConstant("John Doggett")),
	))

	err = ts.BeforeFirst()
	if err != nil {
		t.Fatal(err)
	}

	ok, err := ts.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a record was not found")
	}
	ok, err = pred.isSatisfied(ts)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("isSatisfied must return `true`")
	}

	ok, err = ts.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a record was not found")
	}
	ok, err = pred.isSatisfied(ts)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("isSatisfied must return `false`")
	}
}
