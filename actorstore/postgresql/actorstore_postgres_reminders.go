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

	"github.com/jackc/pgx/v5"

	"github.com/dapr/components-contrib/actorstore"
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
	rows, err := p.db.Exec(queryCtx,
		fmt.Sprintf(`DELETE FROM %s WHERE actor_type = $1 AND actor_id = $2 AND reminder_name = $3`, p.metadata.TableName(pgTableReminders)),
		req.ActorType, req.ActorID, req.Name,
	)
	if err != nil {
		return fmt.Errorf("failed to delete reminder: %w", err)
	}
	if rows.RowsAffected() == 0 {
		return actorstore.ErrReminderNotFound
	}
	return nil
}

func (p *PostgreSQL) FetchNextReminders(ctx context.Context, req actorstore.FetchNextRemindersRequest) ([]*actorstore.FetchedReminder, error) {
	cfg := p.metadata.Config

	// If there's no host or supported actor types, that means there's nothing to return
	if len(req.Hosts) == 0 && len(req.ActorTypes) == 0 {
		return nil, nil
	}

	// Allocate with enough capacity for the max batch size
	res := make([]*actorstore.FetchedReminder, 0, cfg.RemindersFetchAheadBatchSize)

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
		res = append(res, r)
	}

	err := rows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch upcoming reminders: %w", err)
	}

	return res, nil
}

func (p *PostgreSQL) GetReminderWithLease(ctx context.Context, fr *actorstore.FetchedReminder) (res actorstore.Reminder, err error) {
	lease, ok := fr.Lease().(leaseData)
	if !ok || !lease.IsValid() {
		return res, errors.New("invalid reminder lease object")
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()

	q := fmt.Sprintf(`SELECT
			actor_type, actor_id, reminder_name,
			EXTRACT(EPOCH FROM reminder_execution_time - CURRENT_TIMESTAMP)::int,
			reminder_period, reminder_ttl, reminder_data
		FROM %s
		WHERE
			reminder_id = $1
			AND reminder_lease_id = $2
			AND reminder_lease_pid = $3`,
		p.metadata.TableName(pgTableReminders),
	)
	var delay int
	err = p.db.
		QueryRow(queryCtx, q, lease.reminderID, *lease.leaseID, p.metadata.PID).
		Scan(
			&res.ActorType, &res.ActorID, &res.Name,
			&delay, &res.Period, &res.TTL, &res.Data,
		)
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

func (p *PostgreSQL) UpdateReminderWithLease(ctx context.Context, fr *actorstore.FetchedReminder, req actorstore.UpdateReminderWithLeaseRequest) error {
	lease, ok := fr.Lease().(leaseData)
	if !ok || !lease.IsValid() {
		return errors.New("invalid reminder lease object")
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()

	// Unless KeepLease is true, we also release the lease on the reminder
	var leaseQuery string
	if !req.KeepLease {
		leaseQuery = "reminder_lease_id = NULL, reminder_lease_time = NULL, reminder_lease_pid = NULL,"
	} else {
		// Refresh the lease without releasing it
		leaseQuery = "reminder_lease_time = CURRENT_TIMESTAMP,"
	}

	res, err := p.db.Exec(queryCtx,
		fmt.Sprintf(`UPDATE %s SET
				%s
				reminder_execution_time = $4,
				reminder_period = $5,
				reminder_ttl = $6
			WHERE
				reminder_id = $1
				AND reminder_lease_id = $2
				AND reminder_lease_pid = $3`,
			p.metadata.TableName(pgTableReminders), leaseQuery,
		),
		lease.reminderID, *lease.leaseID, p.metadata.PID,
		req.ExecutionTime, req.Period, req.TTL,
	)
	if err != nil {
		return fmt.Errorf("failed to update reminder: %w", err)
	}
	if res.RowsAffected() == 0 {
		return actorstore.ErrReminderNotFound
	}
	return nil
}

func (p *PostgreSQL) DeleteReminderWithLease(ctx context.Context, fr *actorstore.FetchedReminder) error {
	lease, ok := fr.Lease().(leaseData)
	if !ok || !lease.IsValid() {
		return errors.New("invalid reminder lease object")
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()
	res, err := p.db.Exec(queryCtx,
		fmt.Sprintf(`DELETE FROM %s WHERE reminder_id = $1 AND reminder_lease_id = $2 AND reminder_lease_pid = $3`, p.metadata.TableName(pgTableReminders)),
		lease.reminderID, *lease.leaseID, p.metadata.PID,
	)
	if err != nil {
		return fmt.Errorf("failed to delete reminder: %w", err)
	}
	if res.RowsAffected() == 0 {
		return actorstore.ErrReminderNotFound
	}
	return nil
}

func (p *PostgreSQL) RenewReminderLeases(ctx context.Context, req actorstore.RenewReminderLeasesRequest) (int64, error) {
	// If there's no connected host or no supported actor type, do not renew any lease
	if len(req.Hosts) == 0 && len(req.ActorTypes) == 0 {
		return 0, nil
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()

	res, err := p.db.Exec(queryCtx,
		fmt.Sprintf(`UPDATE %[1]s
			SET reminder_lease_time = CURRENT_TIMESTAMP
			WHERE reminder_id IN (
				SELECT reminder_id
				FROM %[1]s
				LEFT JOIN %[2]s
					ON %[2]s.actor_type = %[1]s.actor_type AND %[2]s.actor_id = %[1]s.actor_id
				WHERE 
					%[1]s.reminder_lease_pid = $1
					AND %[1]s.reminder_lease_time IS NOT NULL
					AND %[1]s.reminder_lease_time >= CURRENT_TIMESTAMP - $2::interval
					AND %[1]s.reminder_lease_id IS NOT NULL
					AND (
						(
							%[2]s.host_id IS NULL
							AND %[1]s.actor_type = ANY($3)
						)
						OR %[2]s.host_id = ANY($4)
					)
			)`,
			p.metadata.TableName(pgTableReminders),
			p.metadata.TableName(pgTableActors),
		),
		p.metadata.PID, p.metadata.Config.RemindersLeaseDuration,
		req.ActorTypes, req.Hosts,
	)
	if err != nil {
		return 0, fmt.Errorf("database error: %w", err)
	}

	return res.RowsAffected(), nil
}

func (p *PostgreSQL) RelinquishReminderLease(ctx context.Context, fr *actorstore.FetchedReminder) error {
	lease, ok := fr.Lease().(leaseData)
	if !ok || !lease.IsValid() {
		return errors.New("invalid reminder lease object")
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()
	_, err := p.db.Exec(queryCtx,
		fmt.Sprintf(`UPDATE %s
			SET
				reminder_lease_id = NULL,
				reminder_lease_time = NULL,
				reminder_lease_pid = NULL
			WHERE
				reminder_id = $1
				AND reminder_lease_id = $2
				AND reminder_lease_pid = $3`,
			p.metadata.TableName(pgTableReminders),
		),
		lease.reminderID, *lease.leaseID, p.metadata.PID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return actorstore.ErrReminderNotFound
		}
		return fmt.Errorf("failed to relinquish lease for reminder: %w", err)
	}
	return nil
}

func (p *PostgreSQL) scanFetchedReminderRow(row pgx.Row, now time.Time) (*actorstore.FetchedReminder, error) {
	var (
		actorType, actorID, name string
		delay                    int
		lease                    leaseData
	)
	err := row.Scan(&lease.reminderID, &actorType, &actorID, &name, &delay, &lease.leaseID)
	if err != nil {
		return nil, err
	}

	// If we couldn't get a lease, return nil
	if lease.leaseID == nil || *lease.leaseID == "" {
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
	leaseID    *string
}

func (ld leaseData) IsValid() bool {
	return ld.reminderID != "" && ld.leaseID != nil && *ld.leaseID != ""
}
