package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	_ "modernc.org/sqlite"
)

func (ss *sqliteSorted) Name() string {
	return ss.name
}

func (ss *sqliteSorted) Size() uint64 {
	db, err := sql.Open("sqlite", ss.name)
	if err != nil {
		return 0
	}
	var r uint64
	err = db.QueryRow("select count(1) from jobs").Scan(&r)
	if err != nil {
		return 0
	}
	return r
}

func (ss *sqliteSorted) Clear() error {
	db, err := sql.Open("sqlite", ss.name)
	if err != nil {
		return err
	}
	_, err = db.Exec("delete from jobs")
	return err
}

func (ss *sqliteSorted) Add(job *client.Job) error {
	if job.At == "" {
		return errors.New("Job does not have an At timestamp")
	}
	return ss.insertJob(job)
}

func (ss *sqliteSorted) insertJob(job *client.Job) error {
	db, err := sql.Open("sqlite", ss.name)
	if err != nil {
		return err
	}
	b, err := json.Marshal(job.Args)
	if err != nil {
		return err
	}
	q := `insert into jobs(jid, queue, jobtype, args, created_at, at, retry) values (?,?,?,?,?,?,?)`
	_, err = db.Exec(q, job.Jid, job.Queue, job.Type, string(b), job.CreatedAt, job.At, job.Retry)
	return err
}

func (ss *sqliteSorted) Get(key []byte) (SortedEntry, error) {
	db, err := sql.Open("sqlite", ss.name)
	if err != nil {
		return nil, err
	}
	var args string
	var j client.Job
	q := `select jid, queue, jobtype, args, created_at, at, retry from jobs where jid=?`
	if err = db.QueryRow(q, string(key)).Scan(&j.Jid, &j.Queue, &j.Type, &args, &j.CreatedAt, &j.At, &j.Retry); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	err = json.Unmarshal([]byte(args), &j.Args)
	if err != nil {
		return nil, err
	}
	return NewDummyEntry(&j), nil
}

func (ss *sqliteSorted) Find(match string, fn func(index int, e SortedEntry) error) error {
	// db, err := sql.Open("sqlite", ss.name)
	// if err != nil {
	// 	return err
	// }
	// rows, err := db.Query("select * from jobs where name regexp ?", match)
	// if err != nil {
	// 	return err
	// }
	// for rows.Next() {
	// 	rows.Scan()
	// }
	return nil
}

func (ss *sqliteSorted) Page(start int, count int, fn func(index int, e SortedEntry) error) (int, error) {
	db, err := sql.Open("sqlite", ss.name)
	if err != nil {
		return 0, err
	}
	q := `select jid, queue, jobtype, args, created_at, at, retry from jobs limit ? offset ?`
	rows, err := db.Query(q, count, start)
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
	db, err := sql.Open("sqlite", ss.name)
	if err != nil {
		return err
	}
	_, err = db.Exec("delete from jobs where jid=?", jid)
	return err
}

func (ss *sqliteSorted) RemoveBefore(timestamp string, maxCount int64, fn func(data []byte) error) (int64, error) {
	tim, err := util.ParseTime(timestamp)
	if err != nil {
		return 0, err
	}
	db, err := sql.Open("sqlite", ss.name)
	if err != nil {
		return 0, err
	}
	q := `DELETE FROM jobs
	WHERE id in
	(
	  SELECT id from jobs where name =? and time <= ? LIMIT ?
	)`
	lol, err := db.Exec(q, ss.name, tim, maxCount)
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
