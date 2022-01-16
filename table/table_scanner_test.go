package table

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nihei9/simple-db/storage"
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

	sc := NewShcema()
	sc.Add("A", NewInt64Field())
	sc.Add("B", NewUint64Field())
	sc.Add("C", NewStringField(10))

	la := NewLayout(sc)

	tx, err := st.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	ts, err := NewTableScanner(tx, tmpTableName, la)
	if err != nil {
		t.Fatal(err)
	}

	if !ts.contain("A") {
		t.Fatal("a field was not found")
	}
	if !ts.contain("B") {
		t.Fatal("a field was not found")
	}
	if !ts.contain("C") {
		t.Fatal("a field was not found")
	}
	if ts.contain("a") {
		t.Fatal("invalid field")
	}

	type testData struct {
		a int64
		b uint64
		c string
	}

	data := make([]*testData, 100)
	for i := 0; i < len(data); i++ {
		data[i] = &testData{
			a: int64(i),
			b: uint64(i * -1),
			c: fmt.Sprintf("#%v", i),
		}
	}

	err = ts.BeforeFirst()
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range data {
		err := ts.Insert()
		if err != nil {
			t.Fatal(err)
		}

		err = ts.WriteInt64("A", d.a)
		if err != nil {
			t.Fatal(err)
		}
		err = ts.WriteUint64("B", d.b)
		if err != nil {
			t.Fatal(err)
		}
		err = ts.WriteString("C", d.c)
		if err != nil {
			t.Fatal(err)
		}
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
	err = ts.Delete()
	if err != nil {
		t.Fatal(err)
	}
	firstSlot := 1

	err = ts.BeforeFirst()
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range data[firstSlot:] {
		ok, err := ts.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("a record was not found")
		}

		rid, ok := ts.RecordID()
		if !ok {
			t.Fatal("RecordID must return `true`")
		}
		if rid.blkNum < 0 || rid.slotNum < 0 {
			t.Fatalf("invalid record id: %#v", rid)
		}

		a, err := ts.ReadInt64("A")
		if err != nil {
			t.Fatal(err)
		}
		if a != d.a {
			t.Fatalf("unexpected value was read: field: %v: want: %v, got: %v", "A", d.a, a)
		}
		b, err := ts.ReadUint64("B")
		if err != nil {
			t.Fatal(err)
		}
		if b != d.b {
			t.Fatalf("unexpected value was read: field: %v: want: %v, got: %v", "B", d.b, b)
		}
		c, err := ts.ReadString("C")
		if err != nil {
			t.Fatal(err)
		}
		if c != d.c {
			t.Fatalf("unexpected value was read: field: %v: want: %#v, got: %#v", "C", d.c, c)
		}
	}

	err = ts.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
}
