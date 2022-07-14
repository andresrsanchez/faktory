package manager

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"

	_ "modernc.org/sqlite"
)

var (
	// We can't pass a nil across the Fetcher interface boundary so we'll
	// use this sentinel value to mean nil.
	Nothing Lease = &simpleLease{}
)

func (m *manager) RemoveQueue(qName string) error {
	q, ok := m.store.ExistingQueue(qName)
	if ok {
		_, err := q.Clear()
		if err != nil {
			return fmt.Errorf("cannot remove queue: %w", err)
		}
	}
	m.paused = filter([]string{qName}, m.paused)
	return nil
}

func (m *manager) PauseQueue(qName string) error {
	q, ok := m.store.ExistingQueue(qName)
	if ok {
		err := q.Pause()
		if err != nil {
			return fmt.Errorf("cannot pause queue: %w", err)
		}
		m.paused = append(filter([]string{qName}, m.paused), qName)
	}
	return nil
}

func (m *manager) ResumeQueue(qName string) error {
	q, ok := m.store.ExistingQueue(qName)
	if ok {
		err := q.Resume()
		if err != nil {
			return fmt.Errorf("cannot resume queue: %w", err)
		}

		m.paused = filter([]string{qName}, m.paused)
	}
	return nil
}

// returns the subset of "queues" which are not in "paused"
func filter(paused []string, queues []string) []string {
	if len(paused) == 0 {
		return queues
	}

	qs := make([]string, len(queues))
	count := 0

	for qidx := 0; qidx < len(queues); qidx++ {
		if !contains(queues[qidx], paused) {
			qs[count] = queues[qidx]
			count++
		}
	}
	return qs[:count]
}

func contains(a string, slc []string) bool {
	for x := range slc {
		if a == slc[x] {
			return true
		}
	}
	return false
}

func (m *manager) Fetch(ctx context.Context, wid string, queues ...string) (*client.Job, error) {
	if len(queues) == 0 {
		return nil, fmt.Errorf("must call fetch with at least one queue")
	}
restart:
	activeQueues := filter(m.paused, queues)
	if len(activeQueues) == 0 {
		// if we pause all queues, there is nothing to fetch
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
		return nil, nil
	}
	lease, err := m.fetcher.Fetch(ctx, wid, activeQueues...)
	if err != nil {
		return nil, err
	}
	if lease != Nothing {
		job, err := lease.Job()
		if err != nil {
			return nil, err
		}
		err = callMiddleware(m.fetchChain, Ctx{ctx, job, m, nil}, func() error {
			return m.reserve(wid, lease)
		})
		if h, ok := err.(KnownError); ok {
			util.Infof("JID %s: %s", job.Jid, h.Error())
			if h.Code() == "DISCARD" {
				goto restart
			}
			return nil, err
		}
		if err != nil {
			return nil, err
		}
		return job, nil
	}
	return nil, nil
}

type Fetcher interface {
	Fetch(ctx context.Context, wid string, queues ...string) (Lease, error)
}

type BasicFetch struct {
	s storage.Store
}

type simpleLease struct {
	payload  []byte
	job      *client.Job
	released bool
}

func (el *simpleLease) Release() error {
	el.released = true
	return nil
}

func (el *simpleLease) Payload() []byte {
	return el.payload
}

func (el *simpleLease) Job() (*client.Job, error) {
	if el.job != nil {
		return el.job, nil
	}
	if el.payload == nil {
		return nil, nil
	}
	if el.job == nil {
		var job client.Job
		err := json.Unmarshal(el.payload, &job)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal job payload: %w", err)
		}
		el.job = &job
	}

	return el.job, nil
}

func BasicFetcher(s storage.Store) Fetcher {
	return &BasicFetch{s: s}
}

func (f *BasicFetch) Fetch(ctx context.Context, wid string, queues ...string) (Lease, error) {
	var weird []interface{}
	for _, v := range queues {
		weird = append(weird, v)
	}
	data, err := brpop(f.s, weird)
	if err != nil {
		return nil, err
	}
	if data != nil {
		return &simpleLease{payload: data}, nil
	}
	return Nothing, nil
}

func brpop(store storage.Store, queues []interface{}) ([]byte, error) { //empty queues?
	if len(queues) == 0 {
		return nil, nil
	}

	_, db := store.Sqlite()
	query := "select name from queues"
	rows, err := db.Query(query, queues...)
	if err != nil {
		return nil, err
	}
	rqueues := make(map[string]bool)
	defer rows.Close()
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			continue
		}
		rqueues[name] = true
	}
	if len(rqueues) == 0 {
		return nil, nil
	}

	query = `delete from jobs where id = (select id from jobs limit 1) returning jid, queue, jobtype, args, created_at, at, enqueued_at, 
	retry, reserve_for, backtrace, retry_count, remaining, failed_at, next_at, err_msg, err_type, fbacktrace`
	timeout := time.After(2 * time.Second)
	var i int
	var r []byte
	for {
		select {
		case <-timeout:
			return r, nil
		default:
			if i == len(rqueues) {
				i = 0
			}
			name := queues[i].(string)
			if _, ok := rqueues[name]; !ok {
				i++
				continue
			}
			queue, err := store.GetQueue(name)
			if err != nil {
				continue
			}
			return queue.Pop()
		}
	}
}

func ScanJob(row *sql.Row) (*client.Job, error) {
	j := &client.Job{Failure: &client.Failure{}}
	var args string
	var created, enqueue, at, failed, next, msg, etype, fbacktrace sql.NullString
	var reserve, retry, rcount, remaining, trace sql.NullInt32
	err := row.Scan(&j.Jid, &j.Queue, &j.Type, &args, &created, &at, &enqueue, &retry, &reserve, &trace, &rcount, &remaining, &failed, &next, &msg, &etype, &fbacktrace)
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
