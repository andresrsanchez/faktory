package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/storage"
)

var (
	AlwaysMatch = func(value string) bool {
		return true
	}
)

func mutateKill(store storage.Store, op client.Operation) error {
	ss := setForTarget(store, string(op.Target))
	if ss == nil {
		return fmt.Errorf("invalid target for mutation command")
	}
	match, _ := matchForFilter(op.Filter)
	return ss.Find(match, func(idx int, ent storage.SortedEntry) error {
		// if matchfn(string(ent.Value())) {
		return ss.MoveTo(store.Dead(), ent, time.Now().Add(manager.DeadTTL))
		// }
		// return nil
	})
}

func mutateRequeue(store storage.Store, op client.Operation) error {
	ss := setForTarget(store, string(op.Target))
	if ss == nil {
		return fmt.Errorf("invalid target for mutation command")
	}
	match, _ := matchForFilter(op.Filter)
	return ss.Find(match, func(idx int, ent storage.SortedEntry) error {
		j, err := ent.Job()
		if err != nil {
			return err
		}
		q, err := store.GetQueue(j.Queue)
		if err != nil {
			return err
		}
		err = q.Push(j)
		if err != nil {
			return err
		}
		return ss.RemoveEntry(ent)
	})
}

func mutateDiscard(store storage.Store, op client.Operation) error {
	ss := setForTarget(store, string(op.Target))
	if ss == nil {
		return fmt.Errorf("invalid target for mutation command")
	}
	if op.Filter == nil {
		return ss.Clear()
	}
	match, _ := matchForFilter(op.Filter)
	return ss.Find(match, func(idx int, ent storage.SortedEntry) error {
		// if matchfn(string(ent.Value())) {
		return ss.RemoveEntry(ent)
		//}
		//return nil
	})
}

func matchForFilter(filter *client.JobFilter) (string, func(value string) bool) {
	if filter == nil {
		return "*", AlwaysMatch
	}
	if filter.Regexp != "" {
		return "-", AlwaysMatch
	}
	data, err := json.Marshal(filter)
	if err != nil {
		return "-", AlwaysMatch
	}
	return string(data), AlwaysMatch
}

func mutate(c *Connection, s *Server, cmd string) {
	parts := strings.Split(cmd, " ")
	if len(parts) != 2 {
		_ = c.Error(cmd, fmt.Errorf("invalid format"))
		return
	}

	var err error
	var op client.Operation
	err = json.Unmarshal([]byte(parts[1]), &op)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}

	switch op.Cmd {
	case "clear":
		err = mutateClear(s.Store(), string(op.Target))
	case "kill":
		err = mutateKill(s.Store(), op)
	case "discard":
		err = mutateDiscard(s.Store(), op)
	case "requeue":
		err = mutateRequeue(s.Store(), op)
	default:
		err = fmt.Errorf("unknown mutate operation")
	}

	if err != nil {
		_ = c.Error(cmd, err)
		return
	}

	_ = c.Ok()
}

func mutateClear(store storage.Store, target string) error {
	ss := setForTarget(store, target)
	if ss == nil {
		return fmt.Errorf("invalid target for mutation command")
	}
	return ss.Clear()
}

func setForTarget(store storage.Store, name string) storage.SortedSet {
	switch name {
	case "retries":
		return store.Retries()
	case "dead":
		return store.Dead()
	case "scheduled":
		return store.Scheduled()
	default:
		return nil
	}
}
