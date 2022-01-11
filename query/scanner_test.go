package query

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nihei9/simple-db/storage"
	"github.com/nihei9/simple-db/table"
)

func TestTableScanner(t *testing.T) {
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
	sc.Add("A", table.NewInt64Field())
	sc.Add("B", table.NewUint64Field())
	sc.Add("C", table.NewStringField(10))

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
		err = s.WriteInt64("A", -999)
		if err != nil {
			t.Fatal(err)
		}
		err = s.Insert()
		if err != nil {
			t.Fatal(err)
		}
		err = s.WriteUint64("B", 2022)
		if err != nil {
			t.Fatal(err)
		}
		err = s.Insert()
		if err != nil {
			t.Fatal(err)
		}
		err = s.WriteString("C", "Hello")
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

	if !ts.Contain("A") {
		t.Fatal("Contain must return `true`")
	}
	if !ts.Contain("B") {
		t.Fatal("Contain must return `true`")
	}
	if !ts.Contain("C") {
		t.Fatal("Contain must return `true`")
	}
	if ts.Contain("a") {
		t.Fatal("Contain must return `false`")
	}

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
	constA, err := ts.Read("A")
	if err != nil {
		t.Fatal(err)
	}
	valA, ok := constA.asInt64()
	if !ok {
		t.Fatal("asInt64 must return `true`")
	}
	if valA != -999 {
		t.Fatalf("unexpected value: want: %v, got: %v", -999, valA)
	}

	ok, err = ts.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a record was not found")
	}
	constB, err := ts.Read("B")
	if err != nil {
		t.Fatal(err)
	}
	valB, ok := constB.asUint64()
	if !ok {
		t.Fatal("asUint64 must return `true`")
	}
	if valB != 2022 {
		t.Fatalf("unexpected value: want: %v, got: %v", 2022, valB)
	}

	ok, err = ts.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a record was not found")
	}
	constC, err := ts.Read("C")
	if err != nil {
		t.Fatal(err)
	}
	valC, ok := constC.asString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if valC != "Hello" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Hello", valC)
	}
}

func makeTestLogFileAndDBFile(dir string) (string, string, error) {
	logFile, err := ioutil.TempFile(dir, "*.log")
	if err != nil {
		return "", "", err
	}
	dbFile, err := ioutil.TempFile(dir, "*.tbl")
	if err != nil {
		return "", "", err
	}
	_, err = os.Create(filepath.Join(dir, "table_catalog.tbl"))
	if err != nil {
		return "", "", err
	}
	_, err = os.Create(filepath.Join(dir, "field_catalog.tbl"))
	if err != nil {
		return "", "", err
	}
	return logFile.Name(), dbFile.Name(), nil
}
