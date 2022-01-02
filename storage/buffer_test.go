package storage

import (
	"bytes"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestBuffer(t *testing.T) {
	testDir, err := os.MkdirTemp("", "simple-db-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	t.Run("a buffer can modify a page and write it out to a disk", func(t *testing.T) {
		blkSize := 400
		var buf *buffer
		{
			fm, lm, err := newTestFileManagerAndLogManager(testDir, blkSize)
			if err != nil {
				t.Fatal(err)
			}
			buf, err = newBuffer(fm, lm)
			if err != nil {
				t.Fatal(err)
			}
		}

		var dbFilePath string
		{
			var err error
			dbFilePath, err = makeTestDBFile(testDir)
			if err != nil {
				t.Fatal(err)
			}
			dbFileName := filepath.Base(dbFilePath)

			blk, err := buf.fm.alloc(dbFileName)
			if err != nil {
				t.Fatal(err)
			}
			err = buf.assign(blk)
			if err != nil {
				t.Fatal(err)
			}
		}

		text1 := "Trust no one."
		_, err = buf.contents.writeString(0, text1)
		if err != nil {
			t.Fatal(err)
		}
		err = buf.modify(1, 1)
		if err != nil {
			t.Fatal(err)
		}
		s, _, err := buf.contents.readString(0)
		if err != nil {
			t.Fatal(err)
		}
		if s != text1 {
			t.Fatalf("an unexpected string is read: want: %#v, got: %#v", text1, s)
		}
		c, err := readString(dbFilePath, buf.blk.blkNum, blkSize)
		if err != nil {
			t.Fatal(err)
		}
		if c != "" {
			t.Fatalf("unexpected contents: want: %#v, got: %#v", "", c)
		}
		err = buf.flush()
		if err != nil {
			t.Fatal(err)
		}
		c, err = readString(dbFilePath, buf.blk.blkNum, blkSize)
		if err != nil {
			t.Fatal(err)
		}
		if c != text1 {
			t.Fatalf("unexpected contents: want: %#v, got: %#v", text1, c)
		}

		text2 := "I want to believe."
		_, err = buf.contents.writeString(0, text2)
		if err != nil {
			t.Fatal(err)
		}
		err = buf.modify(1, 2)
		if err != nil {
			t.Fatal(err)
		}
		s, _, err = buf.contents.readString(0)
		if err != nil {
			t.Fatal(err)
		}
		if s != text2 {
			t.Fatalf("an unexpected string is read: want: %#v, got: %#v", text2, s)
		}
		c, err = readString(dbFilePath, buf.blk.blkNum, blkSize)
		if err != nil {
			t.Fatal(err)
		}
		if c != text1 {
			t.Fatalf("unexpected contents: want: %#v, got: %#v", text1, c)
		}
		err = buf.flush()
		if err != nil {
			t.Fatal(err)
		}
		c, err = readString(dbFilePath, buf.blk.blkNum, blkSize)
		if err != nil {
			t.Fatal(err)
		}
		if c != text2 {
			t.Fatalf("unexpected contents: want: %#v, got: %#v", text2, c)
		}
	})

	t.Run("when a buffer is unassigned, pin and unpin function cannot call", func(t *testing.T) {
		var buf *buffer
		{
			fm, lm, err := newTestFileManagerAndLogManager(testDir, 400)
			if err != nil {
				t.Fatal(err)
			}
			buf, err = newBuffer(fm, lm)
			if err != nil {
				t.Fatal(err)
			}
		}

		err = buf.pin()
		if !errors.Is(err, errBufferUnassigned) {
			t.Fatalf("unexpected error: want: %v, got: %v", errBufferUnassigned, err)
		}

		err = buf.unpin()
		if !errors.Is(err, errBufferUnassigned) {
			t.Fatalf("unexpected error: want: %v, got: %v", errBufferUnassigned, err)
		}

		{
			dbFilePath, err := makeTestDBFile(testDir)
			if err != nil {
				t.Fatal(err)
			}
			dbFileName := filepath.Base(dbFilePath)

			blk, err := buf.fm.alloc(dbFileName)
			if err != nil {
				t.Fatal(err)
			}
			err = buf.assign(blk)
			if err != nil {
				t.Fatal(err)
			}
		}

		err = buf.pin()
		if err != nil {
			t.Fatal(err)
		}
		if !buf.pinned() {
			t.Fatal("buffer is not pinned")
		}
		err = buf.unpin()
		if err != nil {
			t.Fatal(err)
		}
		err = buf.unpin()
		if !errors.Is(err, errBufferNegativePinCounter) {
			t.Fatalf("unexpected error: want: %v, got: %v", errBufferNegativePinCounter, err)
		}
	})
}

func TestBufferManager(t *testing.T) {
	testDir, err := os.MkdirTemp("", "simple-db-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	t.Run("", func(t *testing.T) {
		fm, lm, err := newTestFileManagerAndLogManager(testDir, 400)
		if err != nil {
			t.Fatal(err)
		}

		dbFilePath, err := makeTestDBFile(testDir)
		if err != nil {
			t.Fatal(err)
		}
		dbFileName := filepath.Base(dbFilePath)

		bm, err := newBufferManager(fm, lm, 3)
		if err != nil {
			t.Fatal(err)
		}

		var buf1 *buffer
		var buf2 *buffer
		{
			blk1, err := fm.alloc(dbFileName)
			if err != nil {
				t.Fatal(err)
			}
			buf1, err = bm.pin(blk1)
			if err != nil {
				t.Fatal(err)
			}

			blk2, err := fm.alloc(dbFileName)
			if err != nil {
				t.Fatal(err)
			}
			buf2, err = bm.pin(blk2)
			if err != nil {
				t.Fatal(err)
			}
		}

		var txNum1 transactionNum = 1
		_, err = buf1.contents.writeString(0, "Hi")
		if err != nil {
			t.Fatal(err)
		}
		err = buf1.modify(txNum1, 1)
		if err != nil {
			t.Fatal(err)
		}

		var txNum2 transactionNum = 2
		_, err = buf2.contents.writeString(0, "Hello")
		if err != nil {
			t.Fatal(err)
		}
		err = buf2.modify(txNum2, 2)
		if err != nil {
			t.Fatal(err)
		}

		err = bm.flushAll(txNum2)
		if err != nil {
			t.Fatal(err)
		}
		// `buf1` must not be written out to a disk yet because the buffer manager doesn't flush the transaction
		// corresponding to the modification of `buf1` yet.
		{
			p, err := loadOntoPage(dbFilePath, buf1.blk.blkNum, fm.blkSize)
			if err != nil {
				t.Fatal(err)
			}
			v, _, err := p.readString(0)
			if err != nil {
				t.Fatal(err)
			}
			if v != "" {
				t.Fatalf("unexpected value is read: want: %#v, got: %#v", "", v)
			}
		}
		// `buf2` must be written out to a disk because the buffer manager has flushed the transaction
		// corresponding to the modification of `buf2`.
		{
			p, err := loadOntoPage(dbFilePath, buf2.blk.blkNum, fm.blkSize)
			if err != nil {
				t.Fatal(err)
			}
			v, _, err := p.readString(0)
			if err != nil {
				t.Fatal(err)
			}
			if v != "Hello" {
				t.Fatalf("unexpected value is read: want: %#v, got: %#v", "Hello", v)
			}
		}

		err = bm.flushAll(txNum1)
		if err != nil {
			t.Fatal(err)
		}
		{
			p, err := loadOntoPage(dbFilePath, buf1.blk.blkNum, fm.blkSize)
			if err != nil {
				t.Fatal(err)
			}
			v, _, err := p.readString(0)
			if err != nil {
				t.Fatal(err)
			}
			if v != "Hi" {
				t.Fatalf("unexpected value is read: want: %#v, got: %#v", "Hi", v)
			}
		}

		err = bm.unpin(buf1)
		if err != nil {
			t.Fatal(err)
		}
		err = bm.unpin(buf2)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func newTestFileManagerAndLogManager(dir string, blkSize int) (*fileManager, *logManager, error) {
	fm, err := newFileManager(dir, blkSize)
	if err != nil {
		return nil, nil, err
	}
	f, err := ioutil.TempFile(dir, "log_*")
	if err != nil {
		return nil, nil, err
	}
	lm, err := newLogManager(fm, filepath.Base(f.Name()))
	if err != nil {
		return nil, nil, err
	}

	return fm, lm, nil
}

func makeTestDBFile(dir string) (string, error) {
	f, err := ioutil.TempFile(dir, "db_*")
	if err != nil {
		return "", err
	}
	return f.Name(), nil
}

func loadOntoPage(filePath string, blkNum int, blkSize int) (*page, error) {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	p, err := newPage(blkSize)
	if err != nil {
		return nil, err
	}
	offset := blkNum * blkSize
	err = p.load(bytes.NewReader(b[offset:]))
	if err != nil {
		return nil, err
	}
	return p, nil
}

func readString(filePath string, blkNum int, blkSize int) (string, error) {
	p, err := loadOntoPage(filePath, blkNum, blkSize)
	if err != nil {
		return "", err
	}
	s, _, err := p.readString(0)
	if err != nil {
		return "", err
	}
	return s, nil
}
