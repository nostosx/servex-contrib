// Package pgcron manages durable SQL schedules in pg_cron. It does not execute
// Go callbacks, own PostgreSQL's scheduler, or install the extension on startup.
package pgcron

import (
	"context"
	"database/sql"
	"strings"

	"github.com/nostosx/servex-contrib/database/postgres"
)

type Scheduler struct {
	db     *sql.DB
	prefix string
}
type Job struct {
	ID                                          int64
	Name, Schedule, Command, Database, Username string
	Active                                      bool
}

// New confines job management to prefix. Prefix is trusted configuration, not authorization.
// Database privileges and pg_cron's own RLS remain the security boundary.
func New(db *sql.DB, prefix string) (*Scheduler, error) {
	if db == nil {
		return nil, postgres.Invalid("nil database")
	}
	if _, err := postgres.Ident(prefix); err != nil {
		return nil, err
	}
	// PostgreSQL's name type is limited to 63 bytes, including our separator.
	if len(prefix) > 61 {
		return nil, postgres.Invalid("prefix must leave room for a job name within 63 bytes")
	}
	return &Scheduler{db, prefix + ":"}, nil
}
func (s *Scheduler) Check(ctx context.Context) error { return postgres.Require(ctx, s.db, "pg_cron") }

// Schedule creates or updates the caller's named schedule. Command is trusted
// administrative SQL; never construct it from request text. Expressions/timezones
// are interpreted by the server's pg_cron configuration.
func (s *Scheduler) Schedule(ctx context.Context, name, schedule, command string) (int64, error) {
	if err := s.validateName(name); err != nil {
		return 0, err
	}
	if strings.TrimSpace(schedule) == "" || strings.TrimSpace(command) == "" {
		return 0, postgres.Invalid("schedule and SQL command are required")
	}
	var id int64
	err := s.db.QueryRowContext(ctx, `SELECT cron.schedule($1,$2,$3)`, s.prefix+name, schedule, command).Scan(&id)
	return id, postgres.Error(err)
}
func (s *Scheduler) Unschedule(ctx context.Context, name string) (bool, error) {
	if err := s.validateName(name); err != nil {
		return false, err
	}
	var removed bool
	// Select by name and caller before unscheduling; an absent job is idempotent.
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE((SELECT cron.unschedule(jobid) FROM cron.job WHERE jobname=$1 AND username=current_user),false)`, s.prefix+name).Scan(&removed)
	return removed, postgres.Error(err)
}
func (s *Scheduler) validateName(name string) error {
	if _, err := postgres.Ident(name); err != nil {
		return err
	}
	if len(s.prefix)+len(name) > 63 {
		return postgres.Invalid("qualified job name exceeds 63 bytes")
	}
	return nil
}
func (s *Scheduler) Jobs(ctx context.Context) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT jobid,jobname,schedule,command,database,username,active FROM cron.job WHERE starts_with(jobname,$1) AND username=current_user ORDER BY jobid`, s.prefix)
	if err != nil {
		return nil, postgres.Error(err)
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Name, &j.Schedule, &j.Command, &j.Database, &j.Username, &j.Active); err != nil {
			return nil, postgres.Error(err)
		}
		j.Name = strings.TrimPrefix(j.Name, s.prefix)
		jobs = append(jobs, j)
	}
	return jobs, postgres.Error(rows.Err())
}
