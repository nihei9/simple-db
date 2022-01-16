package scanner

import (
	"context"
	"errors"
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

	var ts Scanner
	{
		s, err := table.NewTableScanner(tx, tmpTableName, la)
		if err != nil {
			t.Fatal(err)
		}
		ts = NewTableScanner(s, sc)
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
	valA, ok := constA.AsInt64()
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
	valB, ok := constB.AsUint64()
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
	valC, ok := constC.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if valC != "Hello" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Hello", valC)
	}
}

func TestSelectScanner(t *testing.T) {
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
		err = s.WriteString("name", "Richard Langly")
		if err != nil {
			t.Fatal(err)
		}

		err = s.Insert()
		if err != nil {
			t.Fatal(err)
		}
		err = s.WriteString("name", "Melvin Frohike")
		if err != nil {
			t.Fatal(err)
		}

		err = s.Insert()
		if err != nil {
			t.Fatal(err)
		}
		err = s.WriteString("name", "John Byers")
		if err != nil {
			t.Fatal(err)
		}
	}

	var ss Scanner
	{
		s, err := table.NewTableScanner(tx, tmpTableName, la)
		if err != nil {
			t.Fatal(err)
		}
		ts := NewTableScanner(s, sc)

		pred := NewPredicate(NewTerm(
			NewFieldNameExpression("name"),
			NewConstantExpression(NewStringConstant("Melvin Frohike")),
		))
		ss = NewSelectScanner(ts, pred)
	}

	err = ss.BeforeFirst()
	if err != nil {
		t.Fatal(err)
	}

	ok, err := ss.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a record was not found")
	}
	v, err := ss.Read("name")
	if err != nil {
		t.Fatal(err)
	}
	s, ok := v.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if s != "Melvin Frohike" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Melvin Frohike", s)
	}

	ok, err = ss.Next()
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("Next must return `false`")
	}
}

func TestProjectScanner(t *testing.T) {
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
	sc.Add("id", table.NewInt64Field())
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
		err = s.WriteInt64("id", 1)
		if err != nil {
			t.Fatal(err)
		}
		err = s.WriteString("name", "Richard Langly")
		if err != nil {
			t.Fatal(err)
		}

		err = s.Insert()
		if err != nil {
			t.Fatal(err)
		}
		err = s.WriteInt64("id", 2)
		if err != nil {
			t.Fatal(err)
		}
		err = s.WriteString("name", "Melvin Frohike")
		if err != nil {
			t.Fatal(err)
		}

		err = s.Insert()
		if err != nil {
			t.Fatal(err)
		}
		err = s.WriteInt64("id", 3)
		if err != nil {
			t.Fatal(err)
		}
		err = s.WriteString("name", "John Byers")
		if err != nil {
			t.Fatal(err)
		}
	}

	var ps Scanner
	{
		s, err := table.NewTableScanner(tx, tmpTableName, la)
		if err != nil {
			t.Fatal(err)
		}
		ts := NewTableScanner(s, sc)

		ps = NewProjectScanner(ts, []string{"name"})
	}

	err = ps.BeforeFirst()
	if err != nil {
		t.Fatal(err)
	}

	ok, err := ps.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a record was not found")
	}
	v, err := ps.Read("id")
	if !errors.Is(err, errScannerFieldNotFound) {
		t.Fatalf("unexpected error: want: %v, got: %v", errScannerFieldNotFound, err)
	}
	if v != nil {
		t.Fatalf("Read must return the nil: got: %v", v)
	}
	v, err = ps.Read("name")
	if err != nil {
		t.Fatal(err)
	}
	s, ok := v.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if s != "Richard Langly" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Richard Langly", s)
	}

	ok, err = ps.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a record was not found")
	}
	v, err = ps.Read("id")
	if !errors.Is(err, errScannerFieldNotFound) {
		t.Fatalf("unexpected error: want: %v, got: %v", errScannerFieldNotFound, err)
	}
	if v != nil {
		t.Fatalf("Read must return the nil: got: %v", v)
	}
	v, err = ps.Read("name")
	if err != nil {
		t.Fatal(err)
	}
	s, ok = v.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if s != "Melvin Frohike" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Melvin Frohike", s)
	}

	ok, err = ps.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a record was not found")
	}
	v, err = ps.Read("id")
	if !errors.Is(err, errScannerFieldNotFound) {
		t.Fatalf("unexpected error: want: %v, got: %v", errScannerFieldNotFound, err)
	}
	if v != nil {
		t.Fatalf("Read must return the nil: got: %v", v)
	}
	v, err = ps.Read("name")
	if err != nil {
		t.Fatal(err)
	}
	s, ok = v.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if s != "John Byers" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "John Byers", s)
	}

	ok, err = ps.Next()
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("Next must return `false`")
	}
}

func TestProductScanner(t *testing.T) {
	testDir, err := os.MkdirTemp("", "simple-db-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	var logFileName string
	var tmpTable1Name string
	var tmpTable2Name string
	{
		logFilePath, err := makeTestLogFile(testDir)
		if err != nil {
			t.Fatal(err)
		}
		logFileName = filepath.Base(logFilePath)

		err = makeTestMetaDataDBFiles(testDir)
		if err != nil {
			t.Fatal(err)
		}

		dbFile1Path, err := makeTestDBFile(testDir)
		if err != nil {
			t.Fatal(err)
		}
		tmpTable1Name = strings.TrimSuffix(filepath.Base(dbFile1Path), ".tbl")

		dbFile2Path, err := makeTestDBFile(testDir)
		if err != nil {
			t.Fatal(err)
		}
		tmpTable2Name = strings.TrimSuffix(filepath.Base(dbFile2Path), ".tbl")
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

	var ts1 *tableScanner
	{
		sc := table.NewShcema()
		sc.Add("character_id", table.NewInt64Field())
		sc.Add("character_name", table.NewStringField(10))

		la := table.NewLayout(sc)

		tx, err := st.NewTransaction()
		if err != nil {
			t.Fatal(err)
		}

		// Write test data
		{
			s, err := table.NewTableScanner(tx, tmpTable1Name, la)
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
			err = s.WriteInt64("character_id", 1)
			if err != nil {
				t.Fatal(err)
			}
			err = s.WriteString("character_name", "John Doggett")
			if err != nil {
				t.Fatal(err)
			}

			err = s.Insert()
			if err != nil {
				t.Fatal(err)
			}
			err = s.WriteInt64("character_id", 2)
			if err != nil {
				t.Fatal(err)
			}
			err = s.WriteString("character_name", "Monica Reyes")
			if err != nil {
				t.Fatal(err)
			}
		}

		s, err := table.NewTableScanner(tx, tmpTable1Name, la)
		if err != nil {
			t.Fatal(err)
		}
		ts1 = NewTableScanner(s, sc)
	}

	var ts2 *tableScanner
	{
		sc := table.NewShcema()
		sc.Add("actor_id", table.NewInt64Field())
		sc.Add("actor_name", table.NewStringField(10))

		la := table.NewLayout(sc)

		tx, err := st.NewTransaction()
		if err != nil {
			t.Fatal(err)
		}

		// Write test data
		{
			s, err := table.NewTableScanner(tx, tmpTable2Name, la)
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
			err = s.WriteInt64("actor_id", 101)
			if err != nil {
				t.Fatal(err)
			}
			err = s.WriteString("actor_name", "Robert Patrick")
			if err != nil {
				t.Fatal(err)
			}

			err = s.Insert()
			if err != nil {
				t.Fatal(err)
			}
			err = s.WriteInt64("actor_id", 102)
			if err != nil {
				t.Fatal(err)
			}
			err = s.WriteString("actor_name", "Annabeth Gish")
			if err != nil {
				t.Fatal(err)
			}
		}

		s, err := table.NewTableScanner(tx, tmpTable2Name, la)
		if err != nil {
			t.Fatal(err)
		}
		ts2 = NewTableScanner(s, sc)
	}

	ps := NewProductScanner(ts1, ts2)

	err = ps.BeforeFirst()
	if err != nil {
		t.Fatal(err)
	}

	ok, err := ps.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a record was not found")
	}
	v, err := ps.Read("character_id")
	if err != nil {
		t.Fatal(err)
	}
	i, ok := v.AsInt64()
	if !ok {
		t.Fatal("asInt64 must return `true`")
	}
	if i != 1 {
		t.Fatalf("unexpected value: want: %v, got: %v", 1, i)
	}
	v, err = ps.Read("character_name")
	if err != nil {
		t.Fatal(err)
	}
	s, ok := v.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if s != "John Doggett" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "John Doggett", s)
	}
	v, err = ps.Read("actor_id")
	if err != nil {
		t.Fatal(err)
	}
	i, ok = v.AsInt64()
	if !ok {
		t.Fatal("asInt64 must return `true`")
	}
	if i != 101 {
		t.Fatalf("unexpected value: want: %v, got: %v", 101, i)
	}
	v, err = ps.Read("actor_name")
	if err != nil {
		t.Fatal(err)
	}
	s, ok = v.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if s != "Robert Patrick" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Robert Patrick", s)
	}

	ok, err = ps.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a record was not found")
	}
	v, err = ps.Read("character_id")
	if err != nil {
		t.Fatal(err)
	}
	i, ok = v.AsInt64()
	if !ok {
		t.Fatal("asInt64 must return `true`")
	}
	if i != 1 {
		t.Fatalf("unexpected value: want: %v, got: %v", 1, i)
	}
	v, err = ps.Read("character_name")
	if err != nil {
		t.Fatal(err)
	}
	s, ok = v.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if s != "John Doggett" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "John Doggett", s)
	}
	v, err = ps.Read("actor_id")
	if err != nil {
		t.Fatal(err)
	}
	i, ok = v.AsInt64()
	if !ok {
		t.Fatal("asInt64 must return `true`")
	}
	if i != 102 {
		t.Fatalf("unexpected value: want: %v, got: %v", 102, i)
	}
	v, err = ps.Read("actor_name")
	if err != nil {
		t.Fatal(err)
	}
	s, ok = v.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if s != "Annabeth Gish" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Annabeth Gish", s)
	}

	ok, err = ps.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a record was not found")
	}
	v, err = ps.Read("character_id")
	if err != nil {
		t.Fatal(err)
	}
	i, ok = v.AsInt64()
	if !ok {
		t.Fatal("asInt64 must return `true`")
	}
	if i != 2 {
		t.Fatalf("unexpected value: want: %v, got: %v", 2, i)
	}
	v, err = ps.Read("character_name")
	if err != nil {
		t.Fatal(err)
	}
	s, ok = v.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if s != "Monica Reyes" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Monica Reyes", s)
	}
	v, err = ps.Read("actor_id")
	if err != nil {
		t.Fatal(err)
	}
	i, ok = v.AsInt64()
	if !ok {
		t.Fatal("asInt64 must return `true`")
	}
	if i != 101 {
		t.Fatalf("unexpected value: want: %v, got: %v", 101, i)
	}
	v, err = ps.Read("actor_name")
	if err != nil {
		t.Fatal(err)
	}
	s, ok = v.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if s != "Robert Patrick" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Robert Patrick", s)
	}

	ok, err = ps.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a record was not found")
	}
	v, err = ps.Read("character_id")
	if err != nil {
		t.Fatal(err)
	}
	i, ok = v.AsInt64()
	if !ok {
		t.Fatal("asInt64 must return `true`")
	}
	if i != 2 {
		t.Fatalf("unexpected value: want: %v, got: %v", 2, i)
	}
	v, err = ps.Read("character_name")
	if err != nil {
		t.Fatal(err)
	}
	s, ok = v.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if s != "Monica Reyes" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Monica Reyes", s)
	}
	v, err = ps.Read("actor_id")
	if err != nil {
		t.Fatal(err)
	}
	i, ok = v.AsInt64()
	if !ok {
		t.Fatal("asInt64 must return `true`")
	}
	if i != 102 {
		t.Fatalf("unexpected value: want: %v, got: %v", 102, i)
	}
	v, err = ps.Read("actor_name")
	if err != nil {
		t.Fatal(err)
	}
	s, ok = v.AsString()
	if !ok {
		t.Fatal("asString must return `true`")
	}
	if s != "Annabeth Gish" {
		t.Fatalf("unexpected value: want: %#v, got: %#v", "Annabeth Gish", s)
	}

	ok, err = ps.Next()
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("Next must return `false`")
	}
}

func makeTestLogFileAndDBFile(dir string) (string, string, error) {
	logFile, err := ioutil.TempFile(dir, "*.log")
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
	dbFile, err := ioutil.TempFile(dir, "*.tbl")
	if err != nil {
		return "", "", err
	}
	return logFile.Name(), dbFile.Name(), nil
}

func makeTestLogFile(dir string) (string, error) {
	logFile, err := ioutil.TempFile(dir, "*.log")
	if err != nil {
		return "", err
	}
	return logFile.Name(), nil
}

func makeTestMetaDataDBFiles(dir string) error {
	_, err := os.Create(filepath.Join(dir, "table_catalog.tbl"))
	if err != nil {
		return err
	}
	_, err = os.Create(filepath.Join(dir, "field_catalog.tbl"))
	if err != nil {
		return err
	}
	return nil
}

func makeTestDBFile(dir string) (string, error) {
	dbFile, err := ioutil.TempFile(dir, "*.tbl")
	if err != nil {
		return "", err
	}
	return dbFile.Name(), nil
}
