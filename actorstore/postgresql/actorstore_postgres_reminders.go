/*
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgresql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dapr/components-contrib/actorstore"
	"github.com/jackc/pgx/v5"
)

func (p *PostgreSQL) GetReminder(ctx context.Context, req actorstore.ReminderRef) (res actorstore.GetReminderResponse, err error) {
	if !req.IsValid() {
		return res, actorstore.ErrInvalidRequestMissingParameters
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()

	q := fmt.Sprintf(`SELECT EXTRACT(EPOCH FROM reminder_execution_time - CURRENT_TIMESTAMP)::int, reminder_period, reminder_ttl, reminder_data
		FROM %s WHERE actor_type = $1 AND actor_id = $2 AND reminder_name = $3`, p.metadata.TableName(pgTableReminders))
	var delay int
	err = p.db.
		QueryRow(queryCtx, q, req.ActorType, req.ActorID, req.Name).
		Scan(&delay, &res.Period, &res.TTL, &res.Data)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return res, actorstore.ErrReminderNotFound
		}
		return res, fmt.Errorf("failed to retrieve reminder: %w", err)
	}

	// The query doesn't return an exact time, but rather the number of seconds from present, to make sure we always use the clock of the DB server and avoid clock skews
	res.ExecutionTime = time.Now().Add(time.Duration(delay) * time.Second)

	return res, nil
}

func (p *PostgreSQL) CreateReminder(ctx context.Context, req actorstore.CreateReminderRequest) error {
	if !req.IsValid() {
		return actorstore.ErrInvalidRequestMissingParameters
	}

	// Do not store the exact time, but rather the delay from now, to use the DB server's clock
	var executionTime time.Duration
	if !req.ExecutionTime.IsZero() {
		executionTime = time.Until(req.ExecutionTime)
	} else {
		// Note that delay could be zero
		executionTime = req.Delay
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()
	q := fmt.Sprintf(`INSERT INTO %s
			(actor_type, actor_id, reminder_name, reminder_execution_time, reminder_period, reminder_ttl, reminder_data)
		VALUES ($1, $2, $3, CURRENT_TIMESTAMP + $4::interval, $5, $6, $7)
		ON CONFLICT (actor_type, actor_id, reminder_name) DO UPDATE SET
			reminder_execution_time = EXCLUDED.reminder_execution_time,
			reminder_period = EXCLUDED.reminder_period,
			reminder_ttl = EXCLUDED.reminder_ttl,
			reminder_data = EXCLUDED.reminder_data,
			reminder_lease_time = NULL,
			reminder_lease_pid = NULL`, p.metadata.TableName(pgTableReminders))
	_, err := p.db.Exec(queryCtx, q, req.ActorType, req.ActorID, req.Name, executionTime, req.Period, req.TTL, req.Data)
	if err != nil {
		return fmt.Errorf("failed to create reminder: %w", err)
	}
	return nil
}

func (p *PostgreSQL) CreateLeasedReminder(ctx context.Context, req actorstore.CreateLeasedReminderRequest) (*actorstore.FetchedReminder, error) {
	if !req.Reminder.IsValid() {
		return nil, actorstore.ErrInvalidRequestMissingParameters
	}

	// Do not store the exact time, but rather the delay from now, to use the DB server's clock
	var executionTime time.Duration
	if !req.Reminder.ExecutionTime.IsZero() {
		executionTime = time.Until(req.Reminder.ExecutionTime)
	} else {
		// Note that delay could be zero
		executionTime = req.Reminder.Delay
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()
	q := fmt.Sprintf(createReminderWithLeaseQuery, p.metadata.TableName(pgTableReminders), p.metadata.TableName(pgTableActors))
	row := p.db.QueryRow(queryCtx, q,
		req.Reminder.ActorType, req.Reminder.ActorID, req.Reminder.Name, executionTime,
		req.Reminder.Period, req.Reminder.TTL, req.Reminder.Data, p.metadata.PID,
		req.ActorTypes, req.Hosts,
	)
	res, err := p.scanFetchedReminderRow(row, time.Now())
	if err != nil {
		return nil, fmt.Errorf("failed to create reminder: %w", err)
	}
	if res == nil {
		// Row was inserted, but we couldn't get a lease
		return nil, nil
	}

	return res, nil
}

func (p *PostgreSQL) DeleteReminder(ctx context.Context, req actorstore.ReminderRef) error {
	if !req.IsValid() {
		return actorstore.ErrInvalidRequestMissingParameters
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()
	_, err := p.db.Exec(queryCtx,
		fmt.Sprintf(`DELETE FROM %s WHERE actor_type = $1 AND actor_id = $2 AND reminder_name = $3`, p.metadata.TableName(pgTableReminders)),
		req.ActorType, req.ActorID, req.Name,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return actorstore.ErrReminderNotFound
		}
		return fmt.Errorf("failed to delete reminder: %w", err)
	}
	return nil
}

func (p *PostgreSQL) FetchNextReminders(ctx context.Context, req actorstore.FetchNextRemindersRequest) ([]actorstore.FetchedReminder, error) {
	cfg := p.metadata.Config

	// If there's no host or supported actor types, that means there's nothing to return
	if len(req.Hosts) == 0 && len(req.ActorTypes) == 0 {
		return nil, nil
	}

	// Allocate with enough capacity for the max batch size
	res := make([]actorstore.FetchedReminder, 0, cfg.RemindersFetchAheadBatchSize)

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()

	rows, _ := p.db.Query(queryCtx,
		fmt.Sprintf(remindersFetchQuery, p.metadata.TableName(pgTableReminders), p.metadata.TableName(pgTableActors)),
		cfg.RemindersFetchAheadInterval, cfg.RemindersLeaseDuration,
		req.ActorTypes, req.Hosts,
		cfg.RemindersFetchAheadBatchSize, p.metadata.PID,
	)
	defer rows.Close()

	now := time.Now()
	for rows.Next() {
		r, err := p.scanFetchedReminderRow(rows, now)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch upcoming reminders: %w", err)
		}
		if r == nil {
			// Should never happen
			continue
		}
		res = append(res, *r)
	}

	err := rows.Err()
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, actorstore.ErrReminderNotFound
		}

		return nil, fmt.Errorf("failed to fetch upcoming reminders: %w", err)
	}

	return res, nil
}

func (p *PostgreSQL) scanFetchedReminderRow(row pgx.Row, now time.Time) (*actorstore.FetchedReminder, error) {
	var (
		actorType, actorID, name string
		delay                    int
		lease                    leaseData
	)
	err := row.Scan(&lease.reminderID, &actorType, &actorID, &name, &delay, &lease.leaseTime)
	if err != nil {
		return nil, err
	}

	// If we couldn't get a lease, return nil
	if lease.leaseTime == nil {
		return nil, nil
	}

	// The query doesn't return an exact time, but rather the number of seconds from present, to make sure we always use the clock of the DB server and avoid clock skews
	r := actorstore.NewFetchedReminder(
		actorType+"||"+actorID+"||"+name,
		now.Add(time.Duration(delay)*time.Second),
		lease,
	)
	return &r, nil
}

type leaseData struct {
	reminderID string
	leaseTime  *time.Time
}
