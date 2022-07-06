package storage

import (
	"context"
	"database/sql"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/go-redis/redis"
)

type BackupInfo struct {
	Id        int64
	FileCount int32
	Size      int64
	Timestamp int64
}

type Store interface {
	Close() error
	Retries() SortedSet
	Scheduled() SortedSet
	Working() SortedSet
	Dead() SortedSet
	ExistingQueue(string) (q Queue, ok bool)
	GetQueue(string) (Queue, error)
	EachQueue(func(Queue))
	Stats() map[string]string
	EnqueueAll(SortedSet) error
	EnqueueFrom(SortedSet, string) error
	PausedQueues() ([]string, error)

	History(days int, fn func(day string, procCnt uint64, failCnt uint64)) error
	Success() error
	Failure() error
	TotalProcessed() uint64
	TotalFailures() uint64

	// Clear the database of all job data.
	// Equivalent to Redis's FLUSHDB
	Flush() error

	Raw() KV
	Redis
	Sqlite() (string, *sql.DB)
}

type KV interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
}

type Redis interface {
	Redis() *redis.Client
}

//crear un programa aparte con 3 worker insertando en bd con un maxconn
//crear un programa aparte con 3 worker insertando en bd con un busytimeout
//cgo lsm for history and sset?
type Queue interface {
	Name() string
	Size() uint64

	Pause() error
	Resume() error
	IsPaused() bool

	Add(job *client.Job) error
	Push(data *client.Job) error

	Pop() (*client.Job, error)
	BPop(context.Context) (*client.Job, error)
	Clear() (uint64, error)

	Each(func(index int, data *client.Job) error) error
	Page(start int64, count int64, fn func(index int, data *client.Job) error) error

	Delete(keys []string) error
}

type SortedEntry interface {
	Job() (*client.Job, error)
}

type SortedSet interface {
	Name() string
	Size() uint64
	Clear() error

	Add(job *client.Job) error
	AddElement(timestamp string, job *client.Job) error

	Get(key string) (SortedEntry, error)
	Page(start int, count int, fn func(index int, e SortedEntry) error) (int, error)
	Each(fn func(idx int, e SortedEntry) error) error

	Find(match string, fn func(idx int, e SortedEntry) error) error

	// bool is whether or not the element was actually removed from the sset.
	// the scheduler and other things can be operating on the sset concurrently
	// so we need to be careful about the data changing under us.
	Remove(key string) (bool, error)
	RemoveElement(timestamp string, jid string) (bool, error)
	RemoveBefore(timestamp string, maxCount int64, fn func(job *client.Job) error) (int64, error)
	RemoveEntry(ent SortedEntry) error

	// Move the given key from this SortedSet to the given
	// SortedSet atomically.  The given func may mutate the payload and
	// return a new tstamp.
	MoveTo(sset SortedSet, entry SortedEntry, newtime time.Time) error
}
