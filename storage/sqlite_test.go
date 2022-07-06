package storage

import (
	"os"
	"testing"
)

func withSqlite(t *testing.T, name string, fn func(*testing.T, Store)) {
	t.Parallel()
	os.RemoveAll("./db")
	store, err := NewSqliteStore("db")
	if err != nil {
		panic(err)
	}
	fn(t, store)
	defer os.RemoveAll("./db")
}
