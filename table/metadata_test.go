package table

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/nihei9/simple-db/storage"
)

func TestMetadataManager_tableManager(t *testing.T) {
	testDir, err := os.MkdirTemp("", "simple-db-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	var logFileName string
	var dbFileName string
	{
		logFilePath, dbFilePath, err := makeTestLogFileAndDBFile(testDir)
		if err != nil {
			t.Fatal(err)
		}
		logFileName = filepath.Base(logFilePath)
		dbFileName = filepath.Base(dbFilePath)
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

	err = mm.CreateTable(tx, dbFileName, sc)
	if err != nil {
		t.Fatal(err)
	}

	la, err := mm.FindLayout(tx, dbFileName)
	if err != nil {
		t.Fatal(err)
	}
	if la == nil {
		t.Fatal("findLayout method must return a non-nil value")
	}
}
