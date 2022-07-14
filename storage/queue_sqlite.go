package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	_ "modernc.org/sqlite"
)

type sqliteQueue struct {
	db    *sql.DB
	name  string
	store *sqliteStore
	done  bool
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

func (q *sqliteQueue) Page(start int64, count int64, fn func(index int, data []byte) error) error {
	query := `select jid, queue, jobtype, args, created_at, at, retry from jobs limit ? offset ?`
	rows, err := q.db.Query(query, count, start)
	if err != nil {
		return err
	}
	var idx int
	defer rows.Close()
	for rows.Next() {
		var args string
		var j client.Job
		err = rows.Scan(&j.Jid, &j.Queue, &j.Type, &args, &j.CreatedAt, &j.At, &j.Retry)
		if err != nil {
			return err
		}
		err = json.Unmarshal([]byte(args), &j.Args)
		if err != nil {
			return err //or continue¿?
		}
		p, err := json.Marshal(j)
		if err != nil {
			return err
		}
		err = fn(idx, p)
		if err != nil {
			return err
		}
		idx++
	}
	return nil
}

func (q *sqliteQueue) Each(fn func(index int, data []byte) error) error {
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
	b, err := json.Marshal(job.Args)
	if err != nil {
		return err
	}
	query := `insert into jobs(jid, queue, jobtype, args, created_at, at, retry, enqueued_at) values (?,?,?,?,?,?,?,?)`
	_, err = q.db.Exec(query, job.Jid, job.Queue, job.Type, string(b), job.CreatedAt, job.At, job.Retry, job.EnqueuedAt)
	return err
}

func (q *sqliteQueue) Push(payload []byte) error {
	var j client.Job
	err := json.Unmarshal(payload, &j)
	if err != nil {
		return err
	}
	return q.insertIntoQueue(&j)
}

func (q *sqliteQueue) Pop() ([]byte, error) {
	if q.done {
		return nil, nil
	}
	var args string
	var enq, at sql.NullString
	query := "delete from jobs where id = (select MIN(id) from jobs) returning jid, queue, jobtype, args, created_at, at, retry, enqueued_at"
	var j client.Job
	if err := q.db.QueryRow(query).Scan(&j.Jid, &j.Queue, &j.Type, &args, &j.CreatedAt, &at, &j.Retry, &enq); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	j.EnqueuedAt = enq.String
	j.At = at.String
	err := json.Unmarshal([]byte(args), &j.Args)
	if err != nil {
		return nil, err
	}
	return json.Marshal(j)
}

func (q *sqliteQueue) BPop(ctx context.Context) ([]byte, error) {
	var args string
	var enq, at sql.NullString
	query := "delete from jobs where id = (select MIN(id) from jobs) returning jid, queue, jobtype, args, created_at, at, retry, enqueued_at"
	var j client.Job
	if err := q.db.QueryRowContext(ctx, query).Scan(&j.Jid, &j.Queue, &j.Type, &args, &j.CreatedAt, &at, &j.Retry, &enq); err != nil {
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
	err := json.Unmarshal([]byte(args), &j.Args)
	if err != nil {
		return nil, err
	}
	return json.Marshal(j)
}

func (q *sqliteQueue) Delete(vals [][]byte) error {
	if len(vals) == 0 {
		return nil
	}
	query := "delete from jobs where jid in (?" + strings.Repeat(",?", len(vals)-1) + ")"
	var jids []string
	for _, v := range vals {
		jids = append(jids, string(v))
	}
	_, err := q.db.Exec(query, jids)
	return err
}

func (store *sqliteStore) NewQueue(name string) (*sqliteQueue, error) {
	db, err := getConn(store.Name, name)
	db.SetMaxOpenConns(1)
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
	// _, err = db.Exec("PRAGMA busy_timeout = 5000")
	// if err != nil {
	// 	fmt.Println(err)
	// }
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
	store.queueSet[name] = sq //unsafe
	return sq, nil
}
