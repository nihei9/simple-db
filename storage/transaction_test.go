package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestTransaction_commit(t *testing.T) {
	testDir, err := MakeTestDir()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	fm, lm, err := newTestFileManagerAndLogManager(testDir, 400)
	if err != nil {
		t.Fatal(err)
	}

	var dbFileName string
	{
		dbFilePath, err := MakeTestTableFile(testDir, "")
		if err != nil {
			t.Fatal(err)
		}
		dbFileName = filepath.Base(dbFilePath)
	}

	bm, err := newBufferManager(fm, lm, 5)
	if err != nil {
		t.Fatal(err)
	}

	lockTab := newLockTable()

	ctx := context.Background()
	txNumC := runTransactionNumIssuer(ctx)
	var g *errgroup.Group
	g, ctx = errgroup.WithContext(ctx)
	for i := 0; i < 10; i++ {
		g.Go(func() (retErr error) {
			txNum := <-txNumC

			defer func() {
				if retErr != nil {
					retErr = fmt.Errorf("transaction #%v: %v", txNum, retErr)
				}
			}()

			tx, err := newTransaction(ctx, txNum, fm, lm, bm, lockTab)
			if err != nil {
				return err
			}

			blk, err := tx.AllocBlock(dbFileName)
			if err != nil {
				return err
			}

			err = tx.Pin(blk)
			if err != nil {
				return err
			}

			err = tx.WriteInt64(blk.Hash, 100, -1900, true)
			if err != nil {
				return err
			}

			err = tx.WriteUint64(blk.Hash, 150, 2022, true)
			if err != nil {
				return err
			}

			err = tx.WriteString(blk.Hash, 200, "Hello", true)
			if err != nil {
				return err
			}

			vInt64, err := tx.ReadInt64(blk.Hash, 100)
			if err != nil {
				return err
			}
			if vInt64 != -1900 {
				t.Fatalf("unexpected value was read: want: %v, got: %v", -1900, vInt64)
			}

			vUint64, err := tx.ReadUint64(blk.Hash, 150)
			if err != nil {
				return err
			}
			if vUint64 != 2022 {
				t.Fatalf("unexpected value was read: want: %v, got: %v", 2022, vUint64)
			}

			vString, err := tx.ReadString(blk.Hash, 200)
			if err != nil {
				return err
			}
			if vString != "Hello" {
				t.Fatalf("unexpected value was read: want: %v, got: %v", "Hello", vString)
			}

			err = tx.Commit()
			if err != nil {
				return err
			}

			return nil
		})
	}
	err = g.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

func TestTransaction_rollback(t *testing.T) {
	testDir, err := MakeTestDir()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	fm, lm, err := newTestFileManagerAndLogManager(testDir, 400)
	if err != nil {
		t.Fatal(err)
	}

	var dbFileName string
	{
		dbFilePath, err := MakeTestTableFile(testDir, "")
		if err != nil {
			t.Fatal(err)
		}
		dbFileName = filepath.Base(dbFilePath)
	}

	bm, err := newBufferManager(fm, lm, 5)
	if err != nil {
		t.Fatal(err)
	}

	lockTab := newLockTable()

	ctx := context.Background()
	txNumC := runTransactionNumIssuer(ctx)

	var blk *BlockID
	{
		txNum := <-txNumC
		tx, err := newTransaction(ctx, txNum, fm, lm, bm, lockTab)
		if err != nil {
			t.Fatal(err)
		}

		blk, err = tx.AllocBlock(dbFileName)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Pin(blk)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.WriteInt64(blk.Hash, 100, -1900, true)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.WriteUint64(blk.Hash, 150, 2022, true)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.WriteString(blk.Hash, 200, "Hello", true)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}

	{
		txNum := <-txNumC
		tx, err := newTransaction(ctx, txNum, fm, lm, bm, lockTab)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Pin(blk)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.WriteInt64(blk.Hash, 100, -1700, true)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.WriteUint64(blk.Hash, 150, 2099, true)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.WriteString(blk.Hash, 200, "Bye", true)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}
	}

	{
		txNum := <-txNumC
		tx, err := newTransaction(ctx, txNum, fm, lm, bm, lockTab)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Pin(blk)
		if err != nil {
			t.Fatal(err)
		}

		vInt64, err := tx.ReadInt64(blk.Hash, 100)
		if err != nil {
			t.Fatal(err)
		}
		if vInt64 != -1900 {
			t.Fatalf("unexpected value was read: want: %v, got: %v", -1900, vInt64)
		}

		vUint64, err := tx.ReadUint64(blk.Hash, 150)
		if err != nil {
			t.Fatal(err)
		}
		if vUint64 != 2022 {
			t.Fatalf("unexpected value was read: want: %v, got: %v", 2022, vUint64)
		}

		vString, err := tx.ReadString(blk.Hash, 200)
		if err != nil {
			t.Fatal(err)
		}
		if vString != "Hello" {
			t.Fatalf("unexpected value was read: want: %v, got: %v", "Hello", vString)
		}
	}
}

func TestTransaction_recover(t *testing.T) {
	testDir, err := MakeTestDir()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	fm, lm, err := newTestFileManagerAndLogManager(testDir, 400)
	if err != nil {
		t.Fatal(err)
	}

	var dbFileName string
	{
		dbFilePath, err := MakeTestTableFile(testDir, "")
		if err != nil {
			t.Fatal(err)
		}
		dbFileName = filepath.Base(dbFilePath)
	}

	bm, err := newBufferManager(fm, lm, 5)
	if err != nil {
		t.Fatal(err)
	}

	lockTab := newLockTable()

	ctx := context.Background()
	txNumC := runTransactionNumIssuer(ctx)

	var blk *BlockID
	{
		txNum := <-txNumC
		tx, err := newTransaction(ctx, txNum, fm, lm, bm, lockTab)
		if err != nil {
			t.Fatal(err)
		}

		blk, err = tx.AllocBlock(dbFileName)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Pin(blk)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.WriteInt64(blk.Hash, 100, -1900, true)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.WriteUint64(blk.Hash, 150, 2022, true)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.WriteString(blk.Hash, 200, "Hello", true)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}

	{
		txNum := <-txNumC
		tx, err := newTransaction(ctx, txNum, fm, lm, bm, lockTab)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Pin(blk)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.WriteInt64(blk.Hash, 100, -1700, true)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.WriteUint64(blk.Hash, 150, 2099, true)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.WriteString(blk.Hash, 200, "Bye", true)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Unpin(blk)
		if err != nil {
			t.Fatal(err)
		}
	}

	{
		lockTab := newLockTable()

		txNum := <-txNumC
		tx, err := newTransaction(ctx, txNum, fm, lm, bm, lockTab)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Recover()
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Pin(blk)
		if err != nil {
			t.Fatal(err)
		}

		vInt64, err := tx.ReadInt64(blk.Hash, 100)
		if err != nil {
			t.Fatal(err)
		}
		if vInt64 != -1900 {
			t.Fatalf("unexpected value was read: want: %v, got: %v", -1900, vInt64)
		}

		vUint64, err := tx.ReadUint64(blk.Hash, 150)
		if err != nil {
			t.Fatal(err)
		}
		if vUint64 != 2022 {
			t.Fatalf("unexpected value was read: want: %v, got: %v", 2022, vUint64)
		}

		vString, err := tx.ReadString(blk.Hash, 200)
		if err != nil {
			t.Fatal(err)
		}
		if vString != "Hello" {
			t.Fatalf("unexpected value was read: want: %v, got: %v", "Hello", vString)
		}
	}
}
