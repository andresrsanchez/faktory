package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/go-redis/redis"
	_ "modernc.org/sqlite"
)

type sqliteSorted struct {
	name  string
	store *sqliteStore
	db    *sql.DB
}
type sqliteStore struct {
	Name      string
	mu        sync.Mutex
	queueSet  map[string]*sqliteQueue
	scheduled *sqliteSorted
	retries   *sqliteSorted
	dead      *sqliteSorted
	working   *sqliteSorted
	db        *sql.DB
}

type dummyEntry struct {
	value []byte
	job   *client.Job
	key   []byte
}

func NewDummyEntry(j *client.Job) *dummyEntry {
	b, _ := json.Marshal(j)
	return &dummyEntry{
		job:   j,
		value: b,
		key:   []byte(j.Jid),
	}
}

func (e *dummyEntry) Value() []byte {
	return e.value
}

func (e *dummyEntry) Key() ([]byte, error) {
	if e.key != nil {
		return e.key, nil
	}
	j, err := e.Job()
	if err != nil {
		return nil, err
	}
	return []byte(j.Jid), nil
}

func (e *dummyEntry) Job() (*client.Job, error) {
	if e.job != nil {
		return e.job, nil
	}

	var job client.Job
	err := json.Unmarshal(e.value, &job)
	if err != nil {
		return nil, err
	}

	e.job = &job
	return e.job, nil
}

func NewSqliteStore(name string) (Store, error) {
	db, err := sql.Open("sqlite", name)
	if err != nil {
		return nil, err
	}
	ss := &sqliteStore{
		Name:     name,
		queueSet: make(map[string]*sqliteQueue),
		db:       db,
	}
	err = ss.initSorted()
	if err != nil {
		return nil, err
	}
	q := `
	create table if not exists queues (
		id integer not null primary key,
		name text not null,
		status int default 1 not null)`
	_, err = db.Exec(q)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query("select name from queues")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		q, err := ss.NewQueue(name)
		if err != nil {
			continue
		}
		ss.queueSet[name] = q
	}
	return ss, nil
}

func (ss *sqliteStore) initSorted() error {
	q := `
	create table if not exists jobs (
		id integer not null primary key, 
		jid text not null unique,
		queue text not null,
		jobtype text not null,
		args text not null,
		created_at datetime not null,
		at datetime not null,
		retry integer
	)`
	initEntryDB := func(name string) (*sqliteSorted, error) {
		db, err := sql.Open("sqlite", name)
		if err != nil {
			return nil, err
		}
		db.Exec("PRAGMA journal_mode = WAL")
		db.Exec("PRAGMA synchronous = NORMAL")
		_, err = db.Exec(q)
		if err != nil {
			return nil, err
		}
		s := &sqliteSorted{name: "scheduled", store: ss, db: db}
		return s, nil
	}
	var err error
	ss.scheduled, err = initEntryDB("scheduled")
	if err != nil {
		return err
	}
	ss.retries, err = initEntryDB("retries")
	if err != nil {
		return err
	}
	ss.dead, err = initEntryDB("dead")
	if err != nil {
		return err
	}
	ss.working, err = initEntryDB("working")
	return err
}

func (store *sqliteStore) Stats() map[string]string {
	return map[string]string{}
}

func (store *sqliteStore) PausedQueues() ([]string, error) {
	rows, err := store.db.Query("select name from queues where status=?", 0)
	if err != nil {
		return nil, err
	}
	var r []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			continue
		}
		r = append(r, name)
	}
	return r, nil
}

// queues are iterated in sorted, lexigraphical order
func (store *sqliteStore) EachQueue(x func(Queue)) {
	for _, k := range store.queueSet {
		x(k)
	}
}

func (store *sqliteStore) Flush() error {
	_, err := store.db.Exec("delete * from queues")
	if err != nil {
		return err
	}
	return os.RemoveAll("./dbs")
}

func (store *sqliteStore) ExistingQueue(name string) (Queue, bool) {
	q, ok := store.queueSet[name]
	return q, ok
}

//remember attachdb
// creates the queue if it doesn't already exist
func (store *sqliteStore) GetQueue(name string) (Queue, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	q, ok := store.queueSet[name]
	if ok {
		return q, nil
	}
	if !ValidQueueName.MatchString(name) {
		return nil, fmt.Errorf("queue names must match %v", ValidQueueName)
	}
	return store.NewQueue(name)
}

func (store *sqliteStore) Close() error {
	util.Debug("Stopping nothing")
	return nil
}

func (store *sqliteStore) Redis() *redis.Client { //Review
	return nil
}

func (store *sqliteStore) Retries() SortedSet {
	return store.retries
}

func (store *sqliteStore) Scheduled() SortedSet {
	return store.scheduled
}

func (store *sqliteStore) Working() SortedSet {
	return store.working
}

func (store *sqliteStore) Dead() SortedSet {
	return store.dead
}

func (store *sqliteStore) EnqueueAll(sset SortedSet) error { //Review process batches?
	query := `select jid, queue, jobtype, args, created_at, at, retry from jobs limit ? offset ?`
	batches := func(offset int, limit int) ([]client.Job, error) {
		db, err := sql.Open("sqlite", sset.Name())
		if err != nil {
			return nil, err
		}
		rows, err := db.Query(query, limit, offset)
		if err != nil {
			return nil, err
		}
		var r []client.Job
		for rows.Next() {
			var args string
			var j client.Job
			err = rows.Scan(&j.Jid, &j.Queue, &j.Type, &args, &j.CreatedAt, &j.At, &j.Retry)
			if err != nil {
				return nil, err
			}
			err = json.Unmarshal([]byte(args), &j.Args)
			if err != nil {
				return nil, err //or continue¿?
			}
			r = append(r, j)
		}
		return r, nil
	}
	shouldContinue := true
	offset, limit := 0, 50
	for shouldContinue {
		jobs, err := batches(offset, limit)
		if err != nil {
			return err
		}
		offset += limit
		shouldContinue = len(jobs) > 0
		for _, j := range jobs {
			q, err := store.GetQueue(j.Queue)
			if err != nil {
				return err
			}
			_, err = sset.Remove([]byte(j.Jid)) //danger ignore param _
			if err != nil {
				return err
			}
			err = q.Add(&j)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (store *sqliteStore) EnqueueFrom(sset SortedSet, key []byte) error {
	entry, err := sset.Get(key)
	if err != nil {
		return err
	}
	if entry == nil {
		// race condition, element was removed already
		return nil
	}

	job, err := entry.Job()
	if err != nil {
		return err
	}

	q, err := store.GetQueue(job.Queue)
	if err != nil {
		return err
	}

	ok, err := sset.Remove(key)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	err = q.Add(job)
	if err != nil {
		return err
	}
	return nil
}

func (store *sqliteStore) Success() error {
	return nil
}
func (store *sqliteStore) TotalProcessed() uint64 {
	return 0
}
func (store *sqliteStore) TotalFailures() uint64 {
	return 0
}
func (store *sqliteStore) Failure() error {
	return nil
}
func (store *sqliteStore) History(days int, fn func(day string, procCnt uint64, failCnt uint64)) error {
	return nil
}
func (s *sqliteStore) Raw() KV {
	return nil
}
