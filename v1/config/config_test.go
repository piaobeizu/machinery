package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestConfig(t *testing.T) {
	cnf, err := NewFromYaml("/Users/steven/develop/code/github/machinery/v1/config/testconfig.yml" +
		"", false)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("config is %v\n", cnf.CycleQueue)
	print(getCurrentPath())
}
func getCurrentPath() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}
	return dir
}

