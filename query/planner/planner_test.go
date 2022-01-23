package planner

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nihei9/simple-db/storage"
	"github.com/nihei9/simple-db/table"
)

func TestPlanner(t *testing.T) {
	testDir, err := storage.MakeTestDir()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	var logFileName string
	{
		logFilePath, err := makeTestLogFile(testDir)
		if err != nil {
			t.Fatal(err)
		}
		logFileName = filepath.Base(logFilePath)
	}

	// Make table files
	{
		_, err := storage.MakeTestTableFile(testDir, "actors")
		if err != nil {
			t.Fatal(err)
		}
		_, err = storage.MakeTestTableFile(testDir, "characters")
		if err != nil {
			t.Fatal(err)
		}
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
	mm, err := table.NewMetadataManager(true, tx)
	if err != nil {
		t.Fatal(err)
	}

	qp := NewBasicQueryPlanner(mm)
	up := NewBasicUpdatePlanner(mm)
	p := NewPlanner(qp, up)

	// Test CREATE TABLE and INSERT
	{
		rows, err := p.ExecuteUpdate(tx, strings.NewReader(`create table actors(aid int, aname varchar(100))`))
		if err != nil {
			t.Fatal(err)
		}
		if rows != 0 {
			t.Fatalf("unexpected affected rows: want: %v, got: %v", 0, rows)
		}

		rows, err = p.ExecuteUpdate(tx, strings.NewReader(`insert into actors(aid, aname) values(100, 'Robert Patrick')`))
		if err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("unexpected affected rows: want: %v, got: %v", 1, rows)
		}
		rows, err = p.ExecuteUpdate(tx, strings.NewReader(`insert into actors(aid, aname) values(101, 'Annabeth Gish')`))
		if err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("unexpected affected rows: want: %v, got: %v", 1, rows)
		}

		rows, err = p.ExecuteUpdate(tx, strings.NewReader(`create table characters(cid int, cname varchar(100), caid int)`))
		if err != nil {
			t.Fatal(err)
		}
		if rows != 0 {
			t.Fatalf("unexpected affected rows: want: %v, got: %v", 0, rows)
		}

		rows, err = p.ExecuteUpdate(tx, strings.NewReader(`insert into characters(cid, cname, caid) values(10, 'John Doggett', 100)`))
		if err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("unexpected affected rows: want: %v, got: %v", 1, rows)
		}
		rows, err = p.ExecuteUpdate(tx, strings.NewReader(`insert into characters(cid, cname, caid) values(11, 'Monica Reyes', 101)`))
		if err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("unexpected affected rows: want: %v, got: %v", 1, rows)
		}
	}

	type resultRecord struct {
		aid   int64
		aname string
		cid   int64
		cname string
	}

	// Test SELECT
	{
		plan, err := p.CreateQueryPlan(tx, strings.NewReader(`select aid, aname, cid, cname from actors, characters where aid = caid`))
		if err != nil {
			t.Fatal(err)
		}

		result, err := plan.Open()
		if err != nil {
			t.Fatal(err)
		}
		err = result.BeforeFirst()
		if err != nil {
			t.Fatal(err)
		}

		var recs []*resultRecord
		for {
			ok, err := result.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}

			aid, err := result.ReadInt64("aid")
			if err != nil {
				t.Fatal(err)
			}
			aname, err := result.ReadString("aname")
			if err != nil {
				t.Fatal(err)
			}
			cid, err := result.ReadInt64("cid")
			if err != nil {
				t.Fatal(err)
			}
			cname, err := result.ReadString("cname")
			if err != nil {
				t.Fatal(err)
			}
			recs = append(recs, &resultRecord{
				aid:   aid,
				aname: aname,
				cid:   cid,
				cname: cname,
			})
		}
		if len(recs) != 2 {
			t.Fatalf("unexprec record count: want: %v, got: %v", 2, len(recs))
		}
		r0 := recs[0]
		if r0.aid != 100 || r0.aname != "Robert Patrick" || r0.cid != 10 || r0.cname != "John Doggett" {
			t.Fatalf("unexpected record: %#v", r0)
		}
		r1 := recs[1]
		if r1.aid != 101 || r1.aname != "Annabeth Gish" || r1.cid != 11 || r1.cname != "Monica Reyes" {
			t.Fatalf("unexpected record: %#v", r1)
		}
	}

	// Test UPDATE
	{
		rows, err := p.ExecuteUpdate(tx, strings.NewReader(`update characters set cname = 'John Jay Doggett' where cid = 10`))
		if err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("unexpected affected rows: want: %v, got: %v", 1, rows)
		}
		rows, err = p.ExecuteUpdate(tx, strings.NewReader(`update characters set cname = 'Monica Julieta Reyes' where cid = 11`))
		if err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("unexpected affected rows: want: %v, got: %v", 1, rows)
		}

		plan, err := p.CreateQueryPlan(tx, strings.NewReader(`select cid, cname from characters`))
		if err != nil {
			t.Fatal(err)
		}

		result, err := plan.Open()
		if err != nil {
			t.Fatal(err)
		}
		err = result.BeforeFirst()
		if err != nil {
			t.Fatal(err)
		}

		var recs []*resultRecord
		for {
			ok, err := result.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}

			cid, err := result.ReadInt64("cid")
			if err != nil {
				t.Fatal(err)
			}
			cname, err := result.ReadString("cname")
			if err != nil {
				t.Fatal(err)
			}
			recs = append(recs, &resultRecord{
				cid:   cid,
				cname: cname,
			})
		}
		if len(recs) != 2 {
			t.Fatalf("unexprec record count: want: %v, got: %v", 2, len(recs))
		}
		r0 := recs[0]
		if r0.cid != 10 || r0.cname != "John Jay Doggett" {
			t.Fatalf("unexpected record: %#v", r0)
		}
		r1 := recs[1]
		if r1.cid != 11 || r1.cname != "Monica Julieta Reyes" {
			t.Fatalf("unexpected record: %#v", r1)
		}
	}

	// Test DELETE
	{
		rows, err := p.ExecuteUpdate(tx, strings.NewReader(`delete from characters where cid = 11`))
		if err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("unexpected affected rows: want: %v, got: %v", 1, rows)
		}

		plan, err := p.CreateQueryPlan(tx, strings.NewReader(`select cid, cname from characters`))
		if err != nil {
			t.Fatal(err)
		}

		result, err := plan.Open()
		if err != nil {
			t.Fatal(err)
		}
		err = result.BeforeFirst()
		if err != nil {
			t.Fatal(err)
		}

		var recs []*resultRecord
		for {
			ok, err := result.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}

			cid, err := result.ReadInt64("cid")
			if err != nil {
				t.Fatal(err)
			}
			cname, err := result.ReadString("cname")
			if err != nil {
				t.Fatal(err)
			}
			recs = append(recs, &resultRecord{
				cid:   cid,
				cname: cname,
			})
		}
		if len(recs) != 1 {
			t.Fatalf("unexprec record count: want: %v, got: %v", 1, len(recs))
		}
		r0 := recs[0]
		if r0.cid != 10 || r0.cname != "John Jay Doggett" {
			t.Fatalf("unexpected record: %#v", r0)
		}
	}
}

func makeTestLogFile(dir string) (string, error) {
	logFile, err := storage.MakeTestLogFile(dir)
	if err != nil {
		return "", err
	}
	err = makeTestMetaDataDBFiles(dir)
	if err != nil {
		return "", err
	}
	return logFile, nil
}

func makeTestMetaDataDBFiles(dir string) error {
	_, err := storage.MakeTestTableFile(dir, "table_catalog")
	if err != nil {
		return err
	}
	_, err = storage.MakeTestTableFile(dir, "field_catalog")
	if err != nil {
		return err
	}
	_, err = storage.MakeTestTableFile(dir, "view_catalog")
	if err != nil {
		return err
	}
	return nil
}
