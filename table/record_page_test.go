package table

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/nihei9/simple-db/storage"
)

func TestRecordPage(t *testing.T) {
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
		BlkSize:     400,
		BufSize:     10,
	})
	if err != nil {
		t.Fatal(err)
	}

	sc := NewShcema()
	sc.Add("A", NewInt64Field())
	sc.Add("B", NewUint64Field())
	sc.Add("C", NewStringField(9))

	la := NewLayout(sc)

	tx, err := st.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	blk, err := tx.AllocBlock(dbFileName)
	if err != nil {
		t.Fatal(err)
	}

	rp, err := newRecordPage(tx, blk, la)
	if err != nil {
		t.Fatal(err)
	}

	err = rp.format()
	if err != nil {
		t.Fatal(err)
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

	savedCount := 0
	var slot slotNum = -1
	for _, d := range data {
		var err error
		slot, err = rp.insertAfter(slot)
		if err != nil {
			if !errors.Is(err, errRecPageSlotOutOfRange) {
				t.Fatal(err)
			}
			break
		}

		err = rp.writeInt64(slot, "A", d.a)
		if err != nil {
			t.Fatal(err)
		}
		err = rp.writeUint64(slot, "B", d.b)
		if err != nil {
			t.Fatal(err)
		}
		err = rp.writeString(slot, "C", d.c)
		if err != nil {
			t.Fatal(err)
		}

		savedCount++
	}

	err = rp.delete(0)
	if err != nil {
		t.Fatal(err)
	}
	var firstSlot slotNum = 1

	slot = firstSlot - 1
	for _, d := range data[firstSlot:savedCount] {
		var err error
		slot, err = rp.nextUsedSlotAfter(slot)
		if err != nil {
			if !errors.Is(err, errRecPageSlotOutOfRange) {
				t.Fatal(err)
			}
			break
		}

		a, err := rp.readInt64(slot, "A")
		if err != nil {
			t.Fatal(err)
		}
		if a != d.a {
			t.Fatalf("unexpected value was read: field: %v: want: %v, got: %v", "A", d.a, a)
		}
		b, err := rp.readUint64(slot, "B")
		if err != nil {
			t.Fatal(err)
		}
		if b != d.b {
			t.Fatalf("unexpected value was read: field: %v: want: %v, got: %v", "B", d.b, b)
		}
		c, err := rp.readString(slot, "C")
		if err != nil {
			t.Fatal(err)
		}
		if c != d.c {
			t.Fatalf("unexpected value was read: field: %v: want: %#v, got: %#v", "C", d.c, c)
		}
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
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
