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

	"github.com/dapr/components-contrib/actorstore"
	"github.com/jackc/pgx/v5"
)

func (p *PostgreSQL) GetReminder(ctx context.Context, req actorstore.ReminderRef) (res actorstore.GetReminderResponse, err error) {
	if !req.IsValid() {
		return res, actorstore.ErrInvalidRequestMissingParameters
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()
	err = p.db.
		QueryRow(queryCtx, fmt.Sprintf(`SELECT reminder_execution_time, reminder_period, reminder_ttl, reminder_data
		FROM %s WHERE reminder_reference = $1`, p.metadata.TableName(pgTableReminders)), req.GetReference()).
		Scan(&res.ExecutionTime, &res.Period, &res.TTL, &res.Data)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return res, actorstore.ErrReminderNotFound
		}
		return res, fmt.Errorf("failed to retrieve reminder: %w", err)
	}
	return res, nil
}

func (p *PostgreSQL) CreateReminder(ctx context.Context, req actorstore.CreateReminderRequest) error {
	if !req.IsValid() {
		return actorstore.ErrInvalidRequestMissingParameters
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()
	q := fmt.Sprintf(`INSERT INTO %s
			(reminder_reference, reminder_execution_time, reminder_period, reminder_ttl, reminder_data)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (reminder_reference) DO UPDATE SET
			reminder_execution_time = EXCLUDED.reminder_execution_time,
			reminder_period = EXCLUDED.reminder_period,
			reminder_ttl = EXCLUDED.reminder_ttl,
			reminder_data = EXCLUDED.reminder_data`, p.metadata.TableName(pgTableReminders))
	_, err := p.db.Exec(queryCtx, q, req.GetReference(), req.ExecutionTime, req.Period, req.TTL, req.Data)
	if err != nil {
		return fmt.Errorf("failed to create reminder: %w", err)
	}
	return nil
}

func (p *PostgreSQL) DeleteReminder(ctx context.Context, req actorstore.ReminderRef) error {
	if !req.IsValid() {
		return actorstore.ErrInvalidRequestMissingParameters
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()
	_, err := p.db.Exec(queryCtx, fmt.Sprintf(`DELETE FROM %s WHERE reminder_reference = $1`, p.metadata.TableName(pgTableReminders)), req.GetReference())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return actorstore.ErrReminderNotFound
		}
		return fmt.Errorf("failed to delete reminder: %w", err)
	}
	return nil
}
