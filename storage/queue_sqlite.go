package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	_ "modernc.org/sqlite"
)

type sqliteQueue struct {
	db         *sql.DB
	name       string
	store      *sqliteStore
	done       bool
	stmtDelete *sql.Stmt
}

func (q *sqliteQueue) Pause() error {
	_, err := q.store.db.Exec("insert into queues(name, status) values (?,?)", q.name, 0)
	return err
}

func (q *sqliteQueue) Resume() error {
	_, err := q.store.db.Exec("update queues set status=1 where name=?", q.name)
	return err
}

func (q *sqliteQueue) IsPaused() (r bool) {
	q.store.db.QueryRow("select status from queues where name=?", q.name).Scan(&r)
	return r
}

func (q *sqliteQueue) Close() {
	q.done = true //review
}

func (q *sqliteQueue) Name() string {
	return q.name
}

//faltar parametros al job
func (q *sqliteQueue) Page(start int64, count int64, fn func(index int, job *client.Job) error) error {
	query := `select jid, queue, jobtype, args, created_at, at, retry from jobs limit ? offset ?`
	rows, err := q.db.Query(query, count, start)
	if err != nil {
		return err
	}
	var idx int
	defer rows.Close()
	for rows.Next() {
		var j client.Job
		err = rows.Scan(&j.Jid, &j.Queue, &j.Type, &j.ArgsRaw, &j.CreatedAt, &j.At, &j.Retry)
		if err != nil {
			return err
		}
		err = fn(idx, &j)
		if err != nil {
			return err
		}
		idx++
	}
	return nil
}

func (q *sqliteQueue) Each(fn func(index int, job *client.Job) error) error {
	return q.Page(0, -1, fn)
}

func (q *sqliteQueue) Clear() (uint64, error) {
	q.store.mu.Lock()
	defer q.store.mu.Unlock() //is necessary?
	_, err := q.store.db.Exec("delete from queues where name=?", q.name)
	if err != nil {
		return 0, err
	}
	_, err = q.db.Exec("delete from jobs")
	if err != nil {
		return 0, err
	}
	delete(q.store.queueSet, q.name)
	return 0, nil
}

func (q *sqliteQueue) Size() uint64 {
	var r uint64
	q.db.QueryRow("select count(1) from jobs").Scan(&r)
	return r
}

func (q *sqliteQueue) Add(job *client.Job) error {
	job.EnqueuedAt = util.Nows()
	return q.insertIntoQueue(job)
}

func (q *sqliteQueue) insertIntoQueue(job *client.Job) error {
	var fail string
	if job.Failure != nil {
		data, _ := json.Marshal(job.Failure)
		fail = string(data)
	}
	query := `insert into jobs(jid, queue, jobtype, args, created_at, at, retry, 
		enqueued_at, backtrace, failure) values (?,?,?,?,?,?,?,?,?,?)`
	_, err := q.db.Exec(query, job.Jid, job.Queue, job.Type, job.ArgsRaw, job.CreatedAt, job.At, job.Retry, job.EnqueuedAt, job.Backtrace, fail)
	return err
}

func (q *sqliteQueue) Push(j *client.Job) error {
	return q.insertIntoQueue(j)
}

type Raw struct {
	rBuffer []sql.RawBytes
	args    []interface{}
}

var pool = sync.Pool{
	New: func() interface{} {
		rawBuffer := make([]sql.RawBytes, 8)
		scanCallArgs := make([]interface{}, len(rawBuffer))
		for i := range rawBuffer {
			scanCallArgs[i] = &rawBuffer[i]
		}
		return &Raw{
			rBuffer: rawBuffer,
			args:    scanCallArgs,
		}
	},
}

func (q *sqliteQueue) Pop() (*client.Job, error) {
	if q.done {
		return nil, nil
	}
	rows, err := q.stmtDelete.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	raw := pool.Get().(*Raw)
	err = rows.Scan(raw.args...)
	if err != nil {
		return nil, err
	}
	re := &client.Job{
		Jid:     string(raw.rBuffer[0]),
		Queue:   string(raw.rBuffer[1]),
		Type:    string(raw.rBuffer[2]),
		ArgsRaw: string(raw.rBuffer[3]),
		// Args:        rawBuffer[3],
		CreatedAt:  string(raw.rBuffer[4]),
		EnqueuedAt: string(raw.rBuffer[7]),
		At:         string(raw.rBuffer[5]),
		// Retry:      raw.rBuffer[6].(int),
		// Failure:    &client.Failure{}, //review
	}
	retry := raw.rBuffer[6]
	if retry != nil {
		r, err := strconv.Atoi(string(retry))
		if err != nil {
			return nil, err
		}
		re.Retry = &r
	}

	pool.Put(raw)
	return re, nil
}

//review nullstrings
func (q *sqliteQueue) BPop(ctx context.Context) (*client.Job, error) {
	var enq, at sql.NullString
	query := "delete from jobs where id = (select MIN(id) from jobs) returning jid, queue, jobtype, args, created_at, at, retry, enqueued_at"
	var j client.Job
	if err := q.db.QueryRowContext(ctx, query).Scan(&j.Jid, &j.Queue, &j.Type, &j.ArgsRaw, &j.CreatedAt, &at, &j.Retry, &enq); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if enq.Valid {
		j.EnqueuedAt = enq.String
	}
	if at.Valid {
		j.At = at.String
	}
	return &j, nil
}

func (q *sqliteQueue) Delete(vals []string) error {
	if len(vals) == 0 {
		return nil
	}
	query := "delete from jobs where jid in (?" + strings.Repeat(",?", len(vals)-1) + ")"
	_, err := q.db.Exec(query, vals) //no deberia fucionar
	return err
}

func (store *sqliteStore) NewQueue(name string) (*sqliteQueue, error) {
	db, err := getConn(store.Name, name)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("PRAGMA journal_mode = WAL")
	if err != nil {
		fmt.Println("pragmatic1")
		fmt.Println(err)
	}
	_, err = db.Exec("PRAGMA synchronous = NORMAL")
	if err != nil {
		fmt.Println("pragmatic2")
		fmt.Println(err)
	}
	q := `
	create table if not exists jobs (
		id integer not null primary key, 
		jid text not null unique,
		queue text not null,
		jobtype text not null,
		args text not null,
		created_at text,
		enqueued_at text,
		at text not null,
		reserve_for integer,
		retry integer,
		reserved_at text,
		expires_at text,
		backtrace int,
		failure text
	)`
	_, err = db.Exec(q)
	if err != nil {
		return nil, err
	}
	_, err = store.db.Exec("insert into queues(name) values(?)", name)
	if err != nil {
		return nil, err
	}
	sq := &sqliteQueue{
		name:  name,
		done:  false,
		store: store,
		db:    db,
	}
	sq.stmtDelete, _ = db.Prepare("delete from jobs where id = (select MIN(id) from jobs) returning jid, queue, jobtype, args, created_at, at, retry, enqueued_at")
	store.queueSet[name] = sq //unsafe
	return sq, nil
}
