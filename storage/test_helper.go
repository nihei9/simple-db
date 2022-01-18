package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

func MakeTestDir() (string, error) {
	path, err := os.MkdirTemp("", "simple-db-test-*")
	if err != nil {
		return "", err
	}
	return path, nil
}

func MakeTestLogFile(dirPath string) (string, error) {
	dir, err := ioutil.TempFile(dirPath, "*.log")
	if err != nil {
		return "", err
	}
	return dir.Name(), nil
}

func MakeTestTableFile(dirPath string, tableName string) (string, error) {
	if tableName == "" {
		f, err := ioutil.TempFile(dirPath, "*.tbl")
		if err != nil {
			return "", err
		}
		return f.Name(), nil
	}

	tabFilePath := filepath.Join(dirPath, fmt.Sprintf("%v.tbl", tableName))
	_, err := os.Create(tabFilePath)
	if err != nil {
		return "", err
	}
	return tabFilePath, nil
}
