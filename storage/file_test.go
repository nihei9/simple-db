package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestBlockID_equal(t *testing.T) {
	blk1 := newBlockID("foo", 0)
	blk2 := newBlockID("foo", 1)
	blk3 := newBlockID("bar", 0)
	blk4 := newBlockID("foo", 0)
	if blk1.equal(blk2) {
		t.Fatalf("blk2 must be !=blk1")
	}
	if blk1.equal(blk3) {
		t.Fatalf("blk3 must be !=1")
	}
	if !blk1.equal(blk4) {
		t.Fatalf("blk4 must be ==blk1")
	}
}

func TestPage_newPage(t *testing.T) {
	t.Run("1 byte block is valid", func(t *testing.T) {
		p, err := newPage(1)
		if err != nil {
			t.Fatal(err)
		}
		if p == nil {
			t.Fatal("new page must be non-nil")
		}
	})

	t.Run("block size must be >0", func(t *testing.T) {
		p, err := newPage(0)
		if !errors.Is(err, errPageBlockSizeOutOfRange) {
			t.Fatalf("expected error didn't occur: want: %v, got: %v", errPageBlockSizeOutOfRange, err)
		}
		if p != nil {
			t.Fatal("new page must be nil")
		}
	})

	t.Run("block size must be >0", func(t *testing.T) {
		p, err := newPage(-1)
		if !errors.Is(err, errPageBlockSizeOutOfRange) {
			t.Fatalf("expected error didn't occur: want: %v, got: %v", errPageBlockSizeOutOfRange, err)
		}
		if p != nil {
			t.Fatal("new page must be nil")
		}
	})
}

func TestPage_write(t *testing.T) {
	t.Run("0 offset is valid", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(0, []byte("foo"))
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run(">0 offset is valid", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(1, []byte("foo"))
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("0 byte data (empty slice) is valid", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(0, []byte{})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("0 byte data (nil) is valid", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(0, nil)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("cannot write when the data size to be written to a page is over the page size", func(t *testing.T) {
		blkSize := 100
		p, err := newPage(blkSize)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(0, make([]byte, blkSize))
		if !errors.Is(err, errPageTooBigData) {
			t.Fatalf("expected error didn't occur: want: %v, got: %v", errPageTooBigData, err)
		}
	})

	t.Run("cannot write with an offset that is negative integer", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(-1, []byte("foo"))
		if !errors.Is(err, errPageOffsetOutOfRange) {
			t.Fatalf("expected error didn't occur: want: %v, got: %v", errPageOffsetOutOfRange, err)
		}
	})

	t.Run("cannot write with an offset that is out of range", func(t *testing.T) {
		blkSize := 100
		p, err := newPage(blkSize)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(blkSize, []byte("foo"))
		if !errors.Is(err, errPageOffsetOutOfRange) {
			t.Fatalf("expected error didn't occur: want: %v, got: %v", errPageOffsetOutOfRange, err)
		}
	})
}

func TestPage_read(t *testing.T) {
	t.Run("page.read returns data written by page.write as it is", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(0, []byte("foo"))
		if err != nil {
			t.Fatal(err)
		}
		b, _, err := p.read(0)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "foo" {
			t.Fatalf("unexpected data: want: %v, got: %v", []byte("foo"), b)
		}
	})

	t.Run("0 offset is valid", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(0, []byte("foo"))
		if err != nil {
			t.Fatal(err)
		}
		b, _, err := p.read(0)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "foo" {
			t.Fatalf("unexpected data: want: %v, got: %v", []byte("foo"), b)
		}
	})

	t.Run(">0 offset is valid", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(1, []byte("foo"))
		if err != nil {
			t.Fatal(err)
		}
		b, _, err := p.read(1)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "foo" {
			t.Fatalf("unexpected data: want: %v, got: %v", []byte("foo"), b)
		}
	})

	t.Run("0 byte data (empty slice) is valid", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(0, []byte{})
		if err != nil {
			t.Fatal(err)
		}
		b, _, err := p.read(0)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) != 0 {
			t.Fatalf("unexpected data: want: 0 byte, got: %v byte", len(b))
		}
	})

	t.Run("0 byte data (nil) is valid", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(0, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, _, err := p.read(0)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) != 0 {
			t.Fatalf("unexpected data: want: 0 byte, got: %v byte", len(b))
		}
	})

	t.Run("cannot read with an offset that is negative integer", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = p.read(-1)
		if !errors.Is(err, errPageOffsetOutOfRange) {
			t.Fatalf("expected error didn't occur: want: %v, got: %v", errPageOffsetOutOfRange, err)
		}
	})

	t.Run("cannot read with an offset that is out of range", func(t *testing.T) {
		blkSize := 100
		p, err := newPage(blkSize)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = p.read(blkSize)
		if !errors.Is(err, errPageOffsetOutOfRange) {
			t.Fatalf("expected error didn't occur: want: %v, got: %v", errPageOffsetOutOfRange, err)
		}
	})

	// `blkSize - 1` is an invalid offset because it cannot contain a control part.
	// The control part needs at least binary.MaxVarintLen64 bytes of length.
	//
	// page.write method writes data in the following form:
	//
	// ┌──────┬────────────┐
	// │ Ctrl │    Data    │
	// └──────┴────────────┘
	// - Ctrl: This part contains the length of the Data part.
	// - Data: This part contains data passed as an argument of page.write method.
	t.Run("cannot read with an invalid offset", func(t *testing.T) {
		blkSize := 100
		p, err := newPage(blkSize)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = p.read(blkSize - 1)
		if !errors.Is(err, errPageInvalidOffset) {
			t.Fatalf("expected error didn't occur: want: %v, got: %v", errPageInvalidOffset, err)
		}
	})

	t.Run("a negative data size is invalid", func(t *testing.T) {
		blkSize := 100
		p, err := newPage(blkSize)
		if err != nil {
			t.Fatal(err)
		}
		b := make([]byte, blkSize)
		binary.PutVarint(b, -1)
		err = p.load(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = p.read(0)
		if !errors.Is(err, errPageNegativeDataSize) {
			t.Fatalf("expected error didn't occur: want: %v, got: %v", errPageNegativeDataSize, err)
		}
	})

	t.Run("when data that is out of range is requested, read returns an error", func(t *testing.T) {
		blkSize := 100
		p, err := newPage(blkSize)
		if err != nil {
			t.Fatal(err)
		}
		b := make([]byte, blkSize)
		binary.PutVarint(b, 1000000)
		err = p.load(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = p.read(0)
		if !errors.Is(err, errPageDataOutOfRange) {
			t.Fatalf("expected error didn't occur: want: %v, got: %v", errPageDataOutOfRange, err)
		}
	})
}

const (
	maxUint64 = ^uint64(0)
	minUint64 = uint64(0)
	maxInt64  = int64(maxUint64 >> 1)
	minInt64  = -maxInt64 - 1
)

func TestPage_int64(t *testing.T) {
	t.Run("readInt64/writeInt64 can read/write the minimum value of int64", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.writeInt64(0, minInt64)
		if err != nil {
			t.Fatal(err)
		}
		v, _, err := p.readInt64(0)
		if err != nil {
			t.Fatal(err)
		}
		if v != minInt64 {
			t.Fatalf("unexpected value: want: %v, got: %v", minInt64, v)
		}
	})

	t.Run("readInt64/writeInt64 can read/write the maximum value of int64", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.writeInt64(0, maxInt64)
		if err != nil {
			t.Fatal(err)
		}
		v, _, err := p.readInt64(0)
		if err != nil {
			t.Fatal(err)
		}
		if v != maxInt64 {
			t.Fatalf("unexpected value: want: %v, got: %v", maxInt64, v)
		}
	})

	t.Run("when data is empty, readInt64 returns an error", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(0, []byte{})
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = p.readInt64(0)
		if !errors.Is(err, errDecodeVarintEmptySource) {
			t.Fatal(err)
		}
	})
}

func TestPage_uint64(t *testing.T) {
	t.Run("readUint64/writeUint64 can read/write the minimum value of int64", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.writeUint64(0, minUint64)
		if err != nil {
			t.Fatal(err)
		}
		v, _, err := p.readUint64(0)
		if err != nil {
			t.Fatal(err)
		}
		if v != minUint64 {
			t.Fatalf("unexpected value: want: %v, got: %v", minInt64, v)
		}
	})

	t.Run("readUint64/writeUint64 can read/write the maximum value of int64", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.writeUint64(0, maxUint64)
		if err != nil {
			t.Fatal(err)
		}
		v, _, err := p.readUint64(0)
		if err != nil {
			t.Fatal(err)
		}
		if v != maxUint64 {
			t.Fatalf("unexpected value: want: %v, got: %v", maxInt64, v)
		}
	})

	t.Run("when data is empty, readUint64 returns an error", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.write(0, []byte{})
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = p.readUint64(0)
		if !errors.Is(err, errDecodeVarintEmptySource) {
			t.Fatal(err)
		}
	})
}

func TestPage_string(t *testing.T) {
	t.Run("readString can read a string written by writeString as it is", func(t *testing.T) {
		p, err := newPage(100)
		if err != nil {
			t.Fatal(err)
		}
		text := "I want to believe."
		_, err = p.writeString(0, text)
		if err != nil {
			t.Fatal(err)
		}
		v, _, err := p.readString(0)
		if err != nil {
			t.Fatal(err)
		}
		if v != text {
			t.Fatalf("unexpected value: want: %v, got: %v", maxInt64, v)
		}
	})
}

func TestFileManager(t *testing.T) {
	testDir, err := os.MkdirTemp("", "simple-db-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	fm, err := newFileManager(filepath.Join(testDir, "db"), 400)
	if err != nil {
		t.Fatal(err)
	}

	blk, err := fm.alloc("f1")
	if err != nil {
		t.Fatal(err)
	}

	p1, err := newPage(fm.blkSize)
	if err != nil {
		t.Fatal(err)
	}
	pos1 := 88
	n, err := p1.writeString(pos1, "The truth is out there.")
	if err != nil {
		t.Fatal(err)
	}
	pos2 := pos1 + n
	_, err = p1.writeInt64(pos2, 1993)
	if err != nil {
		t.Fatal(err)
	}
	err = fm.write(blk, p1)
	if err != nil {
		t.Fatal(err)
	}

	p2, err := newPage(fm.blkSize)
	if err != nil {
		t.Fatal(err)
	}
	err = fm.read(blk, p2)
	if err != nil {
		t.Fatal(err)
	}
	v1, _, err := p2.readString(pos1)
	if err != nil {
		t.Fatal(err)
	}
	if v1 != "The truth is out there." {
		t.Fatalf(`unexpected string value: want: "The truth is out there.", got: %+v`, v1)
	}
	v2, _, err := p2.readInt64(pos2)
	if err != nil {
		t.Fatal(err)
	}
	if v2 != 1993 {
		t.Fatalf("unexpected int value: want: 1993, got: %v", v2)
	}
}
