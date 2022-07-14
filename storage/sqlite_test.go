package storage

import (
	"fmt"
	"os"
	"testing"
)

func withSqlite(t *testing.T, name string, fn func(*testing.T, Store)) {
	t.Parallel()
	os.RemoveAll(fmt.Sprintf("./%s", name))
	store, err := NewSqliteStore(name)
	if err != nil {
		panic(err)
	}
	fn(t, store)
}
