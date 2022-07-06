package storage

import (
	"os"
	"testing"

	"github.com/contribsys/faktory/client"
	"github.com/stretchr/testify/assert"
)

func TestSqliteBasicQueueOps(t *testing.T) {
	withSqlite(t, "testing-sqqueue", func(t *testing.T, store Store) {
		t.Run("Push", func(t *testing.T) {
			store.Flush()
			q, err := store.GetQueue("default")
			assert.NoError(t, err)

			assert.EqualValues(t, 0, q.Size())

			data, err := q.Pop()
			assert.NoError(t, err)
			assert.Nil(t, data)

			hello := client.NewJob("hello")
			err = q.Push(hello)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size())

			world := client.NewJob("world")
			err = q.Push(world)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, q.Size())

			values := []*client.Job{hello, world}
			err = q.Each(func(idx int, value *client.Job) error {
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

		t.Run("heavy", func(t *testing.T) {
			store.Flush()
			q, err := store.GetQueue("default")
			assert.NoError(t, err)

			assert.EqualValues(t, 0, q.Size())
			first := client.NewJob("first")
			err = q.Push(first)
			assert.NoError(t, err)
			n := 5000
			// Push N jobs to queue
			// Get Size() each time

			for i := 0; i < n; i++ {
				world := client.NewJob("fake")
				err = q.Push(world)
				assert.NoError(t, err)
				assert.EqualValues(t, i+2, q.Size())
			}
			last := client.NewJob("last")
			err = q.Push(last)
			assert.NoError(t, err)
			assert.EqualValues(t, n+2, q.Size())
			q, err = store.GetQueue("default")
			assert.NoError(t, err)

			// Pop N jobs from queue
			// Get Size() each time
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
		})
	})
}
