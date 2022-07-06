package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/stretchr/testify/assert"
)

func TestSqliteBasicQueueOps(t *testing.T) {
	os.Remove("lol")
	os.Remove("scheduled")
	os.Remove("retries")
	os.Remove("dead")
	os.Remove("working")
	os.Remove("default")
	store, _ := NewSqliteStore("lol")

	t.Run("Push", func(t *testing.T) {
		store.Flush()
		q, err := store.GetQueue("default")
		assert.NoError(t, err)

		assert.EqualValues(t, 0, q.Size())

		data, err := q.Pop()
		assert.NoError(t, err)
		assert.Nil(t, data)

		j := client.NewJob("hello")
		hello, _ := json.Marshal(j)
		err = q.Push(hello)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, q.Size())

		j = client.NewJob("world")
		world, _ := json.Marshal(j)
		err = q.Push(world)
		assert.NoError(t, err)
		assert.EqualValues(t, 2, q.Size())

		values := [][]byte{hello, world}
		err = q.Each(func(idx int, value []byte) error {
			assert.Equal(t, values[idx], value)
			return nil
		})
		assert.NoError(t, err)

		data, err = q.Pop()
		assert.NoError(t, err)
		assert.Equal(t, hello, data)
		assert.EqualValues(t, 1, q.Size())

		cnt, err := q.Clear()
		assert.NoError(t, err)
		assert.EqualValues(t, 0, cnt)
		assert.EqualValues(t, 0, q.Size())

		validQueues := func(names []string) {
			for _, v := range names {
				_, err = store.GetQueue(v)
				assert.NoError(t, err)
				os.Remove(v)
			}
		}
		invalidQueues := func(names []string) {
			for _, v := range names {
				_, err = store.GetQueue(v)
				assert.Error(t, err)
			}
		}
		validQueues([]string{"A-Za-z0-9_.-", "-", "A", "a"})
		invalidQueues([]string{"default?page=1", "user@example.com", "c&c", "priority|high", ""})
	})

	os.Remove("lol")
	os.Remove("scheduled")
	os.Remove("retries")
	os.Remove("dead")
	os.Remove("working")
	os.Remove("default")
	store, _ = NewSqliteStore("lol")

	t.Run("heavy", func(t *testing.T) {
		store.Flush()
		q, err := store.GetQueue("default")
		assert.NoError(t, err)

		assert.EqualValues(t, 0, q.Size())
		j := client.NewJob("first")
		first, _ := json.Marshal(j)
		err = q.Push(first)
		assert.NoError(t, err)
		n := 5000
		// Push N jobs to queue
		// Get Size() each time

		start := time.Now()
		for i := 0; i < n; i++ {
			j := client.NewJob("fake")
			world, _ := json.Marshal(j)
			err = q.Push(world)
			assert.NoError(t, err)
			assert.EqualValues(t, i+2, q.Size())
		}
		j = client.NewJob("last")
		last, _ := json.Marshal(j)
		err = q.Push(last)
		assert.NoError(t, err)
		assert.EqualValues(t, n+2, q.Size())
		fmt.Println(time.Since(start))
		q, err = store.GetQueue("default")
		assert.NoError(t, err)

		// Pop N jobs from queue
		// Get Size() each time
		start = time.Now()
		assert.EqualValues(t, n+2, q.Size())
		data, err := q.Pop()
		assert.NoError(t, err)
		assert.Equal(t, first, data)
		for i := 0; i < n; i++ {
			_, err := q.Pop()
			assert.NoError(t, err)
			assert.EqualValues(t, n-i, q.Size())
		}
		data, err = q.Pop()
		assert.NoError(t, err)
		assert.Equal(t, last, data)
		assert.EqualValues(t, 0, q.Size())

		data, err = q.Pop()
		assert.NoError(t, err)
		assert.Nil(t, data)
		fmt.Println(time.Since(start))
	})
}
