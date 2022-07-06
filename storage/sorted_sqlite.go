package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	_ "modernc.org/sqlite"
)

func (ss *sqliteSorted) Name() string {
	return ss.name
}

func (ss *sqliteSorted) Size() (r uint64) {
	ss.db.QueryRow("select count(1) from jobs").Scan(&r)
	return
}

func (ss *sqliteSorted) Clear() error {
	_, err := ss.db.Exec("delete from jobs")
	return err
}

func (ss *sqliteSorted) Add(job *client.Job) error {
	if job.At == "" {
		return errors.New("Job does not have an At timestamp")
	}
	return ss.insertJob(job)
}

func (ss *sqliteSorted) insertJob(job *client.Job) error {
	var fail string
	if job.Failure != nil {
		data, _ := json.Marshal(job.Failure)
		fail = string(data)
	}
	q := `insert into jobs(jid, queue, jobtype, args, created_at, enqueued_at, at, retry, backtrace, failure, reservation) values (?,?,?,?,?,?,?,?,?,?,?)`
	_, err := ss.db.Exec(q, job.Jid, job.Queue, job.Type, job.ArgsRaw, job.CreatedAt, job.EnqueuedAt, job.At, job.Retry, job.Backtrace, fail, job.Reservation)
	return err
}

func (ss *sqliteSorted) Get(key string) (SortedEntry, error) {
	var failure string
	var j client.Job
	q := `select jid, queue, jobtype, args, created_at, enqueued_at, at, retry, backtrace, failure, reservation from jobs where jid=?`
	if err := ss.db.QueryRow(q, key).Scan(&j.Jid, &j.Queue, &j.Type, &j.ArgsRaw, &j.CreatedAt, &j.EnqueuedAt, &j.At, &j.Retry, &j.Backtrace, &failure, &j.Reservation); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if failure != "" {
		err := json.Unmarshal([]byte(failure), &j.Failure)
		if err != nil {
			return nil, err
		}
	}
	return NewDummyEntry(&j), nil
}

func (ss *sqliteSorted) Find(match string, fn func(index int, e SortedEntry) error) error {
	if match == "-" {
		return fmt.Errorf("not supported filter: %s", match)
	}
	weird := struct {
		Jids    []interface{} `json:"jids,omitempty"`
		Jobtype string        `json:"jobtype,omitempty"`
	}{}
	var builder strings.Builder
	builder.WriteString("select jid, queue, jobtype, args, created_at, enqueued_at, at, retry, backtrace, failure, reservation from jobs where 1 ")
	if match != "*" {
		err := json.Unmarshal([]byte(match), &weird)
		if err != nil {
			return err
		}
		if len(weird.Jids) > 0 {
			builder.WriteString("and jid in (?")
			builder.WriteString(strings.Repeat(",?", len(weird.Jids)-1))
			builder.WriteString(")")
		}
		if weird.Jobtype != "" {
			builder.WriteString("and jobtype=?")
			weird.Jids = append(weird.Jids, weird.Jobtype)
		}
	}
	rows, err := ss.db.Query(builder.String(), weird.Jids...)
	if err != nil {
		return err
	}
	var idx int
	defer rows.Close()
	for rows.Next() {
		var failure string
		var j client.Job
		err = rows.Scan(&j.Jid, &j.Queue, &j.Type, &j.ArgsRaw, &j.CreatedAt, &j.EnqueuedAt, &j.At, &j.Retry, &j.Backtrace, &failure, &j.Reservation)
		if err != nil {
			continue
		}
		if failure != "" {
			err = json.Unmarshal([]byte(failure), &j.Failure)
			if err != nil {
				continue
			}
		}
		err = fn(idx, NewDummyEntry(&j))
		if err != nil {
			continue
		}
	}
	return err
}

func (ss *sqliteSorted) Page(start int, count int, fn func(index int, e SortedEntry) error) (int, error) {
	q := `select jid, queue, jobtype, args, created_at, enqueued_at, at, retry, backtrace, failure, reservation from jobs limit ? offset ?`
	rows, err := ss.db.Query(q, count, start)
	if err != nil {
		return 0, err
	}
	var idx int
	defer rows.Close()
	for rows.Next() {
		var failure string
		var j client.Job
		err = rows.Scan(&j.Jid, &j.Queue, &j.Type, &j.ArgsRaw, &j.CreatedAt, &j.EnqueuedAt, &j.At, &j.Retry, &j.Backtrace, &failure, &j.Reservation)
		if err != nil {
			continue
		}
		if failure != "" {
			err = json.Unmarshal([]byte(failure), &j.Failure)
			if err != nil {
				continue
			}
		}
		err = fn(idx, NewDummyEntry(&j))
		if err != nil {
			return idx, err
		}
		idx++
	}
	return idx, nil
}

func (ss *sqliteSorted) Each(fn func(idx int, e SortedEntry) error) error {
	count := 50
	current := 0

	for {
		elms, err := ss.Page(current, count, fn)
		if err != nil {
			return err
		}

		if elms < count {
			// last page, done iterating
			return nil
		}
		current += count
	}
}

func (ss *sqliteSorted) Remove(key string) (bool, error) {
	err := ss.delete(key)
	return err == nil, err
}

func (ss *sqliteSorted) RemoveElement(timestamp string, jid string) (bool, error) {
	err := ss.delete(jid)
	return err == nil, err
}

func (ss *sqliteSorted) delete(jid string) error {
	_, err := ss.db.Exec("delete from jobs where jid=?", jid)
	return err
}

func (ss *sqliteSorted) RemoveBefore(timestamp string, maxCount int64, fn func(job *client.Job) error) (int64, error) {
	_, err := util.ParseTime(timestamp)
	if err != nil {
		return 0, err
	}
	q := `delete from jobs where id in(select id from jobs where at <= ? limit ?) returning
	jid, queue, jobtype, args, created_at, enqueued_at, at, retry, backtrace, failure, reservation`
	rows, err := ss.db.Query(q, timestamp, maxCount)
	if err != nil {
		return 0, err
	}
	var jobs []*client.Job
	for rows.Next() {
		var failure string
		var j client.Job
		err = rows.Scan(&j.Jid, &j.Queue, &j.Type, &j.ArgsRaw, &j.CreatedAt, &j.EnqueuedAt, &j.At, &j.Retry, &j.Backtrace, &failure, &j.Reservation)
		if err != nil {
			continue
		}
		if failure != "" {
			err = json.Unmarshal([]byte(failure), j.Failure)
			if err != nil {
				continue
			}
		}
		jobs = append(jobs, &j)
	}
	rows.Close()
	var count int64
	for _, job := range jobs {
		err = fn(job)
		if err != nil {
			util.Warnf("Unable to process timed job: %v", err)
			continue
		}
		count++
	}
	return count, nil
}
func (ss *sqliteSorted) MoveTo(sset SortedSet, entry SortedEntry, newtime time.Time) error {
	j, err := entry.Job()
	if err != nil {
		return err
	}
	err = ss.delete(j.Jid)
	if err != nil {
		return err
	}
	return sset.AddElement(util.Thens(newtime), j)
}

func (ss *sqliteSorted) RemoveEntry(entry SortedEntry) error {
	j, err := entry.Job()
	if err != nil {
		return err
	}
	return ss.delete(j.Jid)
}

func (ss *sqliteSorted) AddElement(timestamp string, t *client.Job) error {
	_, err := util.ParseTime(timestamp)
	if err != nil {
		return err
	}
	var fail string
	if t.Failure != nil {
		data, _ := json.Marshal(t.Failure)
		fail = string(data)
	}
	_, err = ss.stmtInsert.Exec(t.Jid, t.Queue, t.Type, t.ArgsRaw, t.CreatedAt, t.EnqueuedAt, t.At, t.ReserveFor, t.Retry, t.Backtrace, fail, t.Reservation)
	return err
}
