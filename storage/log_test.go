package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestLogManager(t *testing.T) {
	testDir, err := os.MkdirTemp("", "simple-db-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	fm, err := newFileManager(filepath.Join(testDir, "log"), 400)
	if err != nil {
		t.Fatal(err)
	}

	lm, err := newLogManager(fm, "log")
	if err != nil {
		t.Fatal(err)
	}

	logCount := 1000

	var logs []string
	for i := 0; i < logCount; i++ {
		logs = append(logs, fmt.Sprintf("log #%v", i))
	}

	for _, log := range logs {
		_, err := lm.appendLog([]byte(log))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = lm.flushAll()
	if err != nil {
		t.Fatal(err)
	}

	n := logCount
	err = lm.apply(func(rec []byte) error {
		n--
		if string(rec) != logs[n] {
			t.Fatalf("unexpected log record: want: %v, got: %v", logs[n], string(rec))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%v records remain", n)
	}
}
