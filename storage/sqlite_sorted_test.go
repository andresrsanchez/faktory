package storage

import (
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestSqliteBasicSortedOps(t *testing.T) {
	withSqlite(t, "testing-sstore", func(t *testing.T, store Store) {
		t.Run("large set", func(t *testing.T) {
			sset := store.Retries()
			err := sset.Clear()
			assert.NoError(t, err)

			for i := 0; i < 550; i++ {
				job := client.NewJob("OtherType", 1, 2, 3)
				if i%100 == 0 {
					job = client.NewJob("SpecialType", 1, 2, 3)
				}
				job.At = util.Nows()
				err = sset.Add(job)
				assert.NoError(t, err)
			}
			assert.EqualValues(t, 550, sset.Size())

			count := 0
			err = sset.Each(func(idx int, entry SortedEntry) error {
				j, err := entry.Job()
				assert.NoError(t, err)
				assert.NotNil(t, j)
				count += 1
				return nil
			})
			assert.NoError(t, err)
			assert.EqualValues(t, 550, count)

			//spcount := 0
			// 	err = sset.Find("*SpecialType*", func(idx int, entry SortedEntry) error {
			// 		j, err := entry.Job()
			// 		assert.NoError(t, err)
			// 		assert.NotNil(t, j)
			// 		assert.Equal(t, "SpecialType", j.Type)
			// 		spcount += 1
			// 		return nil
			// 	})
			// 	assert.NoError(t, err)
			// 	assert.EqualValues(t, 6, spcount)
			err = sset.Clear()
			assert.NoError(t, err)
		})

		t.Run("junk data", func(t *testing.T) {
			sset := store.Retries()
			assert.EqualValues(t, 0, sset.Size())

			tim := util.Nows()
			first := client.NewJob("dummy")
			jid := first.Jid
			err := sset.AddElement(tim, first)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size())

			entry, err := sset.Get(jid)
			assert.NoError(t, err)
			assert.NotNil(t, entry)
			job, err := entry.Job()
			assert.NoError(t, err)
			assert.Equal(t, jid, job.Jid)

			// add a second job with exact same time to handle edge case of
			// sorted set entries with same score.
			second := client.NewJob("dummy")
			newjid := second.Jid
			err = sset.AddElement(tim, second)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, sset.Size())

			entry, err = sset.Get(newjid)
			j, _ := entry.Job()
			assert.NoError(t, err)
			assert.Equal(t, second, j)

			ok, err := sset.Remove(newjid)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size())
			assert.True(t, ok)

			ok, err = sset.RemoveElement(tim, jid)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, sset.Size())
			assert.True(t, ok)

			err = sset.AddElement(tim, second)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size())

			assert.Equal(t, sset.Name(), "retries")
			assert.NoError(t, sset.Clear())
			assert.EqualValues(t, 0, sset.Size())
		})

		t.Run("good data", func(t *testing.T) {
			sset := store.Scheduled()
			job := client.NewJob("SomeType", 1, 2, 3)

			assert.EqualValues(t, 0, sset.Size())
			err := sset.Add(job)
			assert.Error(t, err)

			job.At = util.Nows()
			err = sset.Add(job)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size())

			job = client.NewJob("OtherType", 1, 2, 3)
			job.At = util.Nows()
			err = sset.Add(job)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, sset.Size())

			expectedTypes := []string{"SomeType", "OtherType"}
			actualTypes := []string{}

			err = sset.Each(func(idx int, entry SortedEntry) error {
				j, err := entry.Job()
				assert.NoError(t, err)
				actualTypes = append(actualTypes, j.Type)
				return nil
			})
			assert.NoError(t, err)
			assert.Equal(t, expectedTypes, actualTypes)

			var jkey string
			err = sset.Each(func(idx int, entry SortedEntry) error {
				k, err := entry.Job()
				assert.NoError(t, err)
				jkey = k.Jid
				return nil
			})
			assert.NoError(t, err)

			q, err := store.GetQueue("default")
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size())
			assert.EqualValues(t, 2, sset.Size())

			err = store.EnqueueFrom(sset, jkey)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size())
			assert.EqualValues(t, 1, sset.Size())

			err = store.EnqueueAll(sset)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, q.Size())
			assert.EqualValues(t, 0, sset.Size())

			job = client.NewJob("CronType", 1, 2, 3)
			job.At = util.Nows()
			err = sset.Add(job)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size())

			err = sset.Each(func(idx int, entry SortedEntry) error {
				k, err := entry.Job()
				assert.NoError(t, err)
				jkey = k.Jid
				return nil
			})
			assert.NoError(t, err)

			entry, err := sset.Get(jkey)
			assert.NoError(t, err)

			expiry := time.Now().Add(180 * 24 * time.Hour)

			assert.EqualValues(t, 1, sset.Size())
			assert.EqualValues(t, 0, store.Dead().Size())
			err = sset.MoveTo(store.Dead(), entry, expiry)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, sset.Size())
			assert.EqualValues(t, 1, store.Dead().Size())
		})
	})
}
