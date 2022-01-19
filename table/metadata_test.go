package table

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nihei9/simple-db/storage"
)

func TestMetadataManager_tableManager(t *testing.T) {
	testDir, err := storage.MakeTestDir()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	var logFileName string
	var tmpDBName string
	{
		logFilePath, dbFilePath, err := makeTestLogFileAndDBFile(testDir)
		if err != nil {
			t.Fatal(err)
		}
		logFileName = filepath.Base(logFilePath)
		tmpDBName = strings.TrimSuffix(filepath.Base(dbFilePath), ".tbl")
	}

	st, err := storage.InitStorage(context.Background(), &storage.StorageConfig{
		DirPath:     testDir,
		LogFileName: logFileName,
		BlkSize:     1000,
		BufSize:     10,
	})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := st.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	mm, err := NewMetadataManager(true, tx)
	if err != nil {
		t.Fatal(err)
	}

	sc := NewShcema()
	sc.Add("A", NewInt64Field())
	sc.Add("B", NewUint64Field())
	sc.Add("C", NewStringField(32))

	err = mm.CreateTable(tx, tmpDBName, sc)
	if err != nil {
		t.Fatal(err)
	}

	la, err := mm.FindLayout(tx, tmpDBName)
	if err != nil {
		t.Fatal(err)
	}
	if la == nil {
		t.Fatal("findLayout method must return a non-nil value")
	}
}

func TestMetadataManager_viewManager(t *testing.T) {
	testDir, err := storage.MakeTestDir()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	var logFileName string
	{
		logFilePath, _, err := makeTestLogFileAndDBFile(testDir)
		if err != nil {
			t.Fatal(err)
		}
		logFileName = filepath.Base(logFilePath)
	}

	st, err := storage.InitStorage(context.Background(), &storage.StorageConfig{
		DirPath:     testDir,
		LogFileName: logFileName,
		BlkSize:     1000,
		BufSize:     10,
	})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := st.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	mm, err := NewMetadataManager(true, tx)
	if err != nil {
		t.Fatal(err)
	}

	viewDef := "select a, b from foo"
	err = mm.CreateView(tx, "my_view", viewDef)
	if err != nil {
		t.Fatal(err)
	}

	d, err := mm.FindViewDef(tx, "my_view")
	if err != nil {
		t.Fatal(err)
	}
	if d != viewDef {
		t.Fatalf("unexpected view def: want: %v, got: %v", viewDef, d)
	}
}

func TestMetadataManager_statisticManager(t *testing.T) {
	testDir, err := storage.MakeTestDir()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	var logFileName string
	var tmpDBName string
	{
		logFilePath, dbFilePath, err := makeTestLogFileAndDBFile(testDir)
		if err != nil {
			t.Fatal(err)
		}
		logFileName = filepath.Base(logFilePath)
		tmpDBName = strings.TrimSuffix(filepath.Base(dbFilePath), ".tbl")
	}

	st, err := storage.InitStorage(context.Background(), &storage.StorageConfig{
		DirPath:     testDir,
		LogFileName: logFileName,
		BlkSize:     1000,
		BufSize:     10,
	})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := st.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	mm, err := NewMetadataManager(true, tx)
	if err != nil {
		t.Fatal(err)
	}

	sc := NewShcema()
	sc.Add("A", NewInt64Field())
	sc.Add("B", NewUint64Field())
	sc.Add("C", NewStringField(32))

	err = mm.CreateTable(tx, tmpDBName, sc)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx, err = st.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	stat, err := mm.TableStatistic(tx, tmpDBName)
	if err != nil {
		t.Fatal(err)
	}

	if stat.BlockCount != 0 {
		t.Fatalf("BlockCount must be 0")
	}
	if stat.RecordCount != 0 {
		t.Fatalf("RecordCount must be 0")
	}

	ts, err := NewTableScanner(tx, tmpDBName, NewLayout(sc))
	if err != nil {
		t.Fatal(err)
	}
	err = ts.BeforeFirst()
	if err != nil {
		t.Fatal(err)
	}
	err = ts.Insert()
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx, err = st.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	stat, err = mm.TableStatistic(tx, tmpDBName)
	if err != nil {
		t.Fatal(err)
	}

	if stat.BlockCount <= 0 {
		t.Fatalf("BlockCount must be >0")
	}
	if stat.RecordCount <= 0 {
		t.Fatalf("RecordCount must be >0")
	}
}
