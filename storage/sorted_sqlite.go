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
	b, err := json.Marshal(job.Args)
	if err != nil {
		return err
	}
	q := `insert into jobs(jid, queue, jobtype, args, created_at, at, retry) values (?,?,?,?,?,?,?)`
	_, err = ss.db.Exec(q, job.Jid, job.Queue, job.Type, string(b), job.CreatedAt, job.At, job.Retry)
	return err
}

func (ss *sqliteSorted) Get(key []byte) (SortedEntry, error) {
	var args string
	var j client.Job
	q := `select jid, queue, jobtype, args, created_at, at, retry from jobs where jid=?`
	if err := ss.db.QueryRow(q, string(key)).Scan(&j.Jid, &j.Queue, &j.Type, &args, &j.CreatedAt, &j.At, &j.Retry); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	err := json.Unmarshal([]byte(args), &j.Args)
	if err != nil {
		return nil, err
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
	builder.WriteString("select jid, queue, jobtype, args, created_at, at, retry from jobs where 1 ")
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
	for rows.Next() {
		var j client.Job
		var args string
		err = rows.Scan(&j.Jid, &j.Queue, &j.Type, &args, &j.CreatedAt, &j.At, &j.Retry)
		if err != nil {
			continue
		}
		err = fn(idx, NewDummyEntry(&j))
		if err != nil {
			continue
		}
	}
	return err
}

func (ss *sqliteSorted) Page(start int, count int, fn func(index int, e SortedEntry) error) (int, error) {
	q := `select jid, queue, jobtype, args, created_at, at, retry from jobs limit ? offset ?`
	rows, err := ss.db.Query(q, count, start)
	if err != nil {
		return 0, err
	}
	var idx int
	for rows.Next() {
		var args string
		var j client.Job
		err = rows.Scan(&j.Jid, &j.Queue, &j.Type, &args, &j.CreatedAt, &j.At, &j.Retry)
		if err != nil {
			continue
		}
		err = json.Unmarshal([]byte(args), &j.Args)
		if err != nil {
			continue
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

func (ss *sqliteSorted) Remove(key []byte) (bool, error) {
	err := ss.delete(string(key))
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

func (ss *sqliteSorted) RemoveBefore(timestamp string, maxCount int64, fn func(data []byte) error) (int64, error) {
	tim, err := util.ParseTime(timestamp)
	if err != nil {
		return 0, err
	}
	q := "delete from jobs where id in(select id from jobs where at <= ? limit ?)"
	lol, err := ss.db.Exec(q, tim, maxCount)
	if err != nil {
		return 0, err
	}
	return lol.RowsAffected()
}
func (ss *sqliteSorted) MoveTo(sset SortedSet, entry SortedEntry, newtime time.Time) error {
	jid, err := entry.Key()
	if err != nil {
		return err
	}
	err = ss.delete(string(jid))
	if err != nil {
		return err
	}
	return sset.AddElement(util.Thens(newtime), string(jid), entry.Value())
}

func (ss *sqliteSorted) RemoveEntry(ent SortedEntry) error {
	jid, err := ent.Key()
	if err != nil {
		return err
	}
	return ss.delete(string(jid))
}

func (ss *sqliteSorted) AddElement(timestamp string, jid string, payload []byte) error {
	_, err := util.ParseTime(timestamp) //use timestamp
	if err != nil {
		return err
	}
	var j client.Job
	err = json.Unmarshal(payload, &j)
	if err != nil {
		return err
	}
	return ss.insertJob(&j)
}
