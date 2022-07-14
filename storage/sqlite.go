package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/go-redis/redis"
	_ "github.com/mattn/go-sqlite3"
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
	os.MkdirAll(fmt.Sprintf("./%s", name), os.ModePerm)
	db, err := getConn(name, name)
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
		id integer primary key,
		name text not null,
		status int default 1 not null);
	create table if not exists history (
		id integer primary key,
		name text not null,
		date date default (date('now')),
		count int not null default 1
	);`
	_, err = db.Exec(q)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS name_date ON history(name, date);")
	if err != nil {
		return nil, err
	}

	rows, err := db.Query("select name from queues")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (ss *sqliteStore) Sqlite() (string, *sql.DB) {
	return ss.Name, ss.db
}

func (ss *sqliteStore) initSorted() error {
	q := `
	create table if not exists jobs (
		id integer not null primary key, 
		jid text not null unique,
		queue text not null,
		jobtype text not null,
		args text not null,
		created_at datetime,
		enqueued_at datetime,
		at datetime not null,
		retry integer,
		reserved_at datetime,
		expires_at datetime,
		backtrace int,
		wid text,
		reserve_for int,
		retry_count int,
		remaining int,
		failed_at datetime,
		next_at datetime,
		err_msg text,
		err_type text,
		fbacktrace text
	)`
	initEntryDB := func(name string) (*sqliteSorted, error) {
		db, err := getConn(ss.Name, name)
		if err != nil {
			return nil, err
		}
		_, err = db.Exec(q)
		if err != nil {
			return nil, err
		}
		s := &sqliteSorted{name: name, store: ss, db: db}
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

func ScanJob(rows *sql.Rows) (*client.Job, error) {
	j := &client.Job{Failure: &client.Failure{}}
	var args string
	var created, enqueue, at, failed, next, msg, etype, fbacktrace sql.NullString
	var reserve, retry, rcount, remaining, trace sql.NullInt32
	err := rows.Scan(&j.Jid, &j.Queue, &j.Type, &args, &created, &at, &enqueue, &retry, &reserve, &trace, &rcount, &remaining, &failed, &next, &msg, &etype, &fbacktrace)
	if err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(args), &j.Args)
	j.CreatedAt = created.String
	j.EnqueuedAt = enqueue.String
	j.At = at.String
	//*j.Retry = int(retry.Int32)
	j.ReserveFor = int(reserve.Int32)
	j.Backtrace = int(trace.Int32)
	j.Failure.RetryCount = int(rcount.Int32)
	j.Failure.RetryRemaining = int(remaining.Int32)
	j.Failure.FailedAt = failed.String
	j.Failure.NextAt = next.String
	j.Failure.ErrorMessage = msg.String
	j.Failure.ErrorType = etype.String
	//j.Failure.Backtrace = fbacktrace
	return j, nil
}

func scanThing(rows *sql.Rows) (interface{}, error) {
	j := struct {
		Jid              string        `json:"jid"`
		Queue            string        `json:"queue"`
		Type             string        `json:"jobtype"`
		Args             []interface{} `json:"args"`
		CreatedAt        string        `json:"created_at,omitempty"`
		EnqueuedAt       string        `json:"enqueued_at,omitempty"`
		At               string        `json:"at,omitempty"`
		ReserveFor       int           `json:"reserve_for,omitempty"`
		Retry            *int          `json:"retry"`
		Backtrace        int           `json:"backtrace,omitempty"`
		RetryCount       int           `json:"retry_count"`
		RetryRemaining   int           `json:"remaining"`
		FailedAt         string        `json:"failed_at"`
		NextAt           string        `json:"next_at,omitempty"`
		ErrorMessage     string        `json:"message,omitempty"`
		ErrorType        string        `json:"errtype,omitempty"`
		FailureBacktrace []string      `json:"fbacktrace,omitempty"`
		Since            string        `json:"reserved_at"`
		Expiry           string        `json:"expires_at"`
		Wid              string        `json:"wid"`
	}{}
	var args string
	var created, enqueue, at, failed, next, msg, etype, fbacktrace, since, expiry, wid sql.NullString
	var reserve, retry, rcount, remaining, trace sql.NullInt32
	err := rows.Scan(&j.Jid, &j.Queue, &j.Type, &args, &created, &at, &enqueue, &retry, &reserve, &trace, &rcount, &remaining, &failed, &next, &msg, &etype, &fbacktrace, &since, &expiry, &wid)
	if err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(args), &j.Args)
	j.CreatedAt = created.String
	j.EnqueuedAt = enqueue.String
	j.At = at.String
	//*j.Retry = int(retry.Int32)
	j.ReserveFor = int(reserve.Int32)
	j.Backtrace = int(trace.Int32)
	j.RetryCount = int(rcount.Int32)
	j.RetryRemaining = int(remaining.Int32)
	j.FailedAt = failed.String
	j.NextAt = next.String
	j.ErrorMessage = msg.String
	j.ErrorType = etype.String
	//j.Failure.Backtrace = fbacktrace
	j.Since = since.String
	j.Expiry = expiry.String
	j.Wid = wid.String

	return j, nil
}

func (store *sqliteStore) Stats() map[string]string {
	return map[string]string{}
}

func (store *sqliteStore) PausedQueues() ([]string, error) {
	rows, err := store.db.Query("select name from queues where status=?", 0)
	if err != nil {
		return nil, err
	}
	r := []string{}
	defer rows.Close()
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
	store.mu.Lock()
	defer store.mu.Unlock()
	pragmas := "&_journal_mode=WAL&_synchronous=NORMAL&cache=shared"
	for k := range store.queueSet {
		db, _ := sql.Open("sqlite3", filepath.Join(store.Name, fmt.Sprintf("%s?%s", k, pragmas)))
		_, err := db.Exec("delete from jobs")
		if err != nil {
			return err
		}
		db.Close()
	}
	store.queueSet = map[string]*sqliteQueue{}
	_, err := store.db.Exec("delete from queues")
	if err != nil {
		return err
	}
	_, err = store.db.Exec("delete from history")
	if err != nil {
		return err
	}
	flush := func(db *sql.DB) error {
		_, err := db.Exec("delete from jobs")
		return err
	}
	err = flush(store.dead.db)
	if err != nil {
		return err
	}
	err = flush(store.retries.db)
	if err != nil {
		return err
	}
	err = flush(store.scheduled.db)
	if err != nil {
		return err
	}
	err = flush(store.working.db)
	return err
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

func getConn(folder string, name string) (*sql.DB, error) {
	pragmas := "&_journal_mode=WAL&_synchronous=NORMAL&cache=shared"
	return sql.Open("sqlite3", filepath.Join(folder, fmt.Sprintf("%s?%s", name, pragmas)))
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
		db, err := getConn(store.Name, sset.Name())
		if err != nil {
			return nil, err
		}
		rows, err := db.Query(query, limit, offset)
		if err != nil {
			return nil, err
		}
		var r []client.Job
		defer rows.Close()
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
	_, err := store.db.Exec("insert into history(name) values (?) on conflict(name,date) do update set count=count+1", "processed")
	// _, err := store.db.Exec("insert into history(name) values (?)", "processed")
	return err
}
func (store *sqliteStore) TotalProcessed() (r uint64) {
	store.db.QueryRow("select count from history where name=? and date=date('now')", "processed").Scan(&r)
	return
}
func (store *sqliteStore) TotalFailures() (r uint64) {
	store.db.QueryRow("select count from history where name=? and date=date('now')", "failures").Scan(&r)
	return
}
func (store *sqliteStore) Failure() error {
	// _, err := store.db.Exec("insert into history(name) values (?)", "failures")
	_, err := store.db.Exec("insert into history(name) values (?),(?) on conflict(name,date) do update set count=count+1", "failures", "processed")
	return err
}

func (store *sqliteStore) History(days int, fn func(day string, procCnt uint64, failCnt uint64)) error {
	before := time.Now().AddDate(0, 0, -(days - 1))
	daystrs := make([]string, days)
	fails := make([]int, days)
	procds := make([]int, days)
	rows, err := store.db.Query("select name, count, date from history where date >= ?", before.Format("2006-01-02"))
	if err != nil {
		return err
	}
	var p = make(map[string]int)
	var f = make(map[string]int)
	defer rows.Close()
	for rows.Next() {
		var count int
		var name string
		var date time.Time
		err := rows.Scan(&name, &count, &date)
		if err != nil {
			continue
		}
		if name == "processed" {
			p[date.Format("2006-01-02")] = count
		} else {
			f[date.Format("2006-01-02")] = count
		}
	}
	for i := 0; i < days; i++ {
		daystr := before.Format("2006-01-02")
		daystrs[i] = daystr
		if count, ok := p[daystr]; ok {
			procds[i] = count
		}
		if count, ok := f[daystr]; ok {
			fails[i] = count
		}
		before = before.Add(24 * time.Hour)
	}
	for idx := 0; idx < days; idx++ {
		fn(daystrs[idx], uint64(procds[idx]), uint64(fails[idx]))
	}
	return nil
}
func (s *sqliteStore) Raw() KV {
	return nil
}

func openSqlite(lel string, lol int) (Store, error) {
	return NewSqliteStore("db")
}

func bootSqlite(string, string) (func(), error) {
	return func() {}, nil
}

func stopSqlite(string) error {
	return nil
}
