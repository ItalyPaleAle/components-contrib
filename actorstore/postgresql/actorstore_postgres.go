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
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/components-contrib/actorstore"
	pginterfaces "github.com/dapr/components-contrib/internal/component/postgresql/interfaces"
	sqlinternal "github.com/dapr/components-contrib/internal/component/sql"
	pgmigrations "github.com/dapr/components-contrib/internal/component/sql/migrations/postgres"
	"github.com/dapr/kit/logger"
)

// NewPostgreSQLActorStore creates a new instance of an actor store backed by PostgreSQL
func NewPostgreSQLActorStore(logger logger.Logger) actorstore.Store {
	return &PostgreSQL{
		logger: logger,
	}
}

type PostgreSQL struct {
	logger   logger.Logger
	metadata pgMetadata
	db       *pgxpool.Pool
	running  atomic.Bool
}

func (p *PostgreSQL) Init(ctx context.Context, md actorstore.Metadata) error {
	if !p.running.CompareAndSwap(false, true) {
		return errors.New("already running")
	}

	// Parse metadata
	err := p.metadata.InitWithMetadata(md)
	if err != nil {
		p.logger.Errorf("Failed to parse metadata: %v", err)
		return err
	}

	// Connect to the database
	config, err := p.metadata.GetPgxPoolConfig()
	if err != nil {
		p.logger.Error(err)
		return err
	}

	connCtx, connCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	p.db, err = pgxpool.NewWithConfig(connCtx, config)
	connCancel()
	if err != nil {
		err = fmt.Errorf("failed to connect to the database: %w", err)
		p.logger.Error(err)
		return err
	}

	err = p.Ping(ctx)
	if err != nil {
		err = fmt.Errorf("failed to ping the database: %w", err)
		p.logger.Error(err)
		return err
	}

	// Migrate schema
	err = p.performMigrations(ctx)
	if err != nil {
		p.logger.Error(err)
		return err
	}

	p.logger.Info("Established connection to PostgreSQL")

	return nil
}

func (p *PostgreSQL) performMigrations(ctx context.Context) error {
	m := pgmigrations.Migrations{
		DB:                p.db,
		Logger:            p.logger,
		MetadataTableName: p.metadata.MetadataTableName,
		MetadataKey:       "migrations-actorstore",
	}

	var (
		hostsTable           = p.metadata.TableName(pgTableHosts)
		hostsActorTypesTable = p.metadata.TableName(pgTableHostsActorTypes)
		actorsTable          = p.metadata.TableName(pgTableActors)
	)

	return m.Perform(ctx, []sqlinternal.MigrationFn{
		// Migration 1: create the tables
		func(ctx context.Context) error {
			p.logger.Infof("Creating tables for actors state. Hosts table: '%s'. Hosts actor types table: '%s'. Actors table: '%s'", hostsTable, hostsActorTypesTable, actorsTable)
			_, err := p.db.Exec(ctx,
				fmt.Sprintf(migration1Query, hostsTable, hostsActorTypesTable, actorsTable),
			)
			if err != nil {
				return fmt.Errorf("failed to create state table: %w", err)
			}
			return nil
		},
	})
}

func (p *PostgreSQL) Ping(ctx context.Context) error {
	if !p.running.Load() {
		return errors.New("not running")
	}

	ctx, cancel := context.WithTimeout(ctx, p.metadata.Timeout)
	err := p.db.Ping(ctx)
	cancel()
	return err
}

func (p *PostgreSQL) Close() (err error) {
	if !p.running.Load() {
		return nil
	}

	p.logger.Debug("Closing connection")
	if p.db != nil {
		p.db.Close()
	}
	return err
}

func (p *PostgreSQL) AddActorHost(ctx context.Context, properties actorstore.AddActorHostRequest) (string, error) {
	if properties.AppID == "" || properties.Address == "" || properties.APILevel <= 0 {
		return "", actorstore.ErrInvalidRequestMissingParameters
	}

	// Because we need to update 2 tables, we need a transaction
	return executeInTransaction(ctx, p.logger, p.db, p.metadata.Timeout, func(ctx context.Context, tx pgx.Tx) (hostID string, err error) {
		var (
			hostsTable           = p.metadata.TableName(pgTableHosts)
			hostsActorTypesTable = p.metadata.TableName(pgTableHostsActorTypes)
		)

		// The hosts table has a unique constraint on host_address
		// To start, we need to delete from the hosts table records which have the same host_address if one of:
		// - The last health check is before the configured amount (i.e. the host is down)
		// - The host_app_id is also the same, which indicates that the app has just been restarted
		// We need to actually delete the row if it exists and matches the criteria above, and can't just perform a "REPLACE" query (i.e. "ON CONFLICT DO UPDATE") because that doesn't cause records in other tables that reference this one (most notably, "actors") to be deleted automatically. We also want to make sure the host_id changes.
		queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
		defer queryCancel()
		_, err = tx.Exec(queryCtx,
			fmt.Sprintf(
				`DELETE FROM %s WHERE
					(host_address = $1 AND host_app_id = $2)
        			OR host_last_healthcheck < CURRENT_TIMESTAMP - $3::interval`,
				hostsTable,
			),
			properties.Address, properties.AppID, p.metadata.Config.FailedInterval(),
		)
		if err != nil {
			return "", fmt.Errorf("failed to remove conflicting hosts: %w", err)
		}

		// We're ready to add the actor host
		// This is also when we detect if we have a conflict on the host_address column's unique index
		queryCtx, queryCancel = context.WithTimeout(ctx, p.metadata.Timeout)
		defer queryCancel()
		err = tx.QueryRow(queryCtx,
			fmt.Sprintf(
				`INSERT INTO %s
					(host_address, host_app_id, host_actors_api_level, host_last_healthcheck)
				VALUES
					($1, $2, $3, CURRENT_TIMESTAMP)
				RETURNING host_id`,
				hostsTable,
			),
			properties.Address, properties.AppID, properties.APILevel,
		).Scan(&hostID)
		if err != nil {
			if isUniqueViolationError(err) {
				return "", actorstore.ErrActorHostConflict
			}
			return "", fmt.Errorf("failed to insert actor host in hosts table: %w", err)
		}

		// Register each supported actor type
		queryCtx, queryCancel = context.WithTimeout(ctx, p.metadata.Timeout)
		defer queryCancel()
		err = insertHostActorTypes(queryCtx, tx, hostID, properties.ActorTypes, hostsActorTypesTable, p.metadata.Timeout)
		if err != nil {
			return "", err
		}

		return hostID, nil
	})
}

// Inserts the list of supported actor types for a host.
// Note that the context must have a timeout already applied if needed.
func insertHostActorTypes(ctx context.Context, tx pgx.Tx, actorHostID string, actorTypes []actorstore.ActorHostType, hostsActorTypesTable string, timeout time.Duration) error {
	if len(actorTypes) == 0 {
		// Nothing to do here
		return nil
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, timeout)
	defer queryCancel()

	// Use "CopyFrom" to insert multiple records more efficiently
	rows := make([][]any, len(actorTypes))
	for i, t := range actorTypes {
		rows[i] = []any{
			actorHostID,
			t.ActorType,
			t.IdleTimeout,
		}
	}
	n, err := tx.CopyFrom(
		queryCtx,
		pgx.Identifier{hostsActorTypesTable},
		[]string{"host_id", "actor_type", "actor_idle_timeout"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("failed to insert supported actor types in hosts actor types table: %w", err)
	}
	if n != int64(len(actorTypes)) {
		return fmt.Errorf("failed to insert supported actor types in hosts actor types table: inserted %d rows, but expected %d", n, len(actorTypes))
	}

	return nil
}

func (p *PostgreSQL) UpdateActorHost(ctx context.Context, actorHostID string, properties actorstore.UpdateActorHostRequest) (err error) {
	// We need at least _something_ to update
	// Note that:
	// ActorTypes==nil -> Do not update actor types
	// ActorTypes==slice with 0 elements -> Remove all actor types
	if actorHostID == "" || (!properties.UpdateLastHealthCheck && properties.ActorTypes == nil) {
		return actorstore.ErrInvalidRequestMissingParameters
	}

	var (
		hostsTable           = p.metadata.TableName(pgTableHosts)
		hostsActorTypesTable = p.metadata.TableName(pgTableHostsActorTypes)
	)

	// Let's avoid creating a transaction if we are not updating actor types (which involve updating 2 tables)
	// This saves at least 2 round-trips to the database and improves locking
	if properties.ActorTypes == nil {
		err = updateHostsTable(ctx, p.db, actorHostID, properties, hostsTable, p.metadata.Config.FailedInterval(), p.metadata.Timeout)
	} else {
		// Because we need to update 2 tables, we need a transaction
		_, err = executeInTransaction(ctx, p.logger, p.db, p.metadata.Timeout, func(ctx context.Context, tx pgx.Tx) (z struct{}, zErr error) {
			// Update all hosts properties, besides the list of supported actor types
			zErr = updateHostsTable(ctx, tx, actorHostID, properties, hostsTable, p.metadata.Config.FailedInterval(), p.metadata.Timeout)
			if zErr != nil {
				return z, zErr
			}

			// Next, delete all existing actor
			// This query could affect 0 rows, and that's fine
			queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
			defer queryCancel()
			_, zErr = p.db.Exec(queryCtx,
				fmt.Sprintf("DELETE FROM %s WHERE host_id = $1", hostsActorTypesTable),
				actorHostID,
			)
			if zErr != nil {
				return z, fmt.Errorf("failed to delete old host actor types: %w", zErr)
			}

			// Register the new supported actor types (if any)
			zErr = insertHostActorTypes(ctx, tx, actorHostID, properties.ActorTypes, hostsActorTypesTable, p.metadata.Timeout)
			if zErr != nil {
				return z, zErr
			}

			return z, nil
		})
	}

	if err != nil {
		return err
	}
	return nil
}

// Updates the hosts table with the given properties.
// Does not update ActorTypes which impacts a separate table.
func updateHostsTable(ctx context.Context, db pginterfaces.DBQuerier, actorHostID string, properties actorstore.UpdateActorHostRequest, hostsTable string, failedInterval time.Duration, timeout time.Duration) error {
	// For now, host_last_healthcheck is the only property that can be updated in the hosts table
	if properties.UpdateLastHealthCheck {
		return nil
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, timeout)
	defer queryCancel()
	res, err := db.Exec(queryCtx,
		fmt.Sprintf(`UPDATE %s
			SET
				host_last_healthcheck = CURRENT_TIMESTAMP
			WHERE
				host_id = $1 AND
				host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
			`, hostsTable),
		actorHostID, failedInterval,
	)
	if err != nil {
		return fmt.Errorf("failed to update actor host: %w", err)
	}
	if res.RowsAffected() == 0 {
		return actorstore.ErrActorHostNotFound
	}
	return nil
}

func (p *PostgreSQL) RemoveActorHost(ctx context.Context, actorHostID string) error {
	if actorHostID == "" {
		return actorstore.ErrInvalidRequestMissingParameters
	}

	// We need to delete from the hosts table only
	// Other table references rows from the hosts table through foreign keys, so records are deleted from there automatically (and atomically)
	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()
	q := fmt.Sprintf("DELETE FROM %s WHERE host_id = $1", p.metadata.TableName(pgTableHosts))
	res, err := p.db.Exec(queryCtx, q, actorHostID)
	if err != nil {
		return fmt.Errorf("failed to remove actor host: %w", err)
	}
	if res.RowsAffected() == 0 {
		return actorstore.ErrActorHostNotFound
	}

	return nil
}

func (p *PostgreSQL) LookupActor(ctx context.Context, ref actorstore.ActorRef) (res actorstore.LookupActorResponse, err error) {
	if ref.ActorType == "" || ref.ActorID == "" {
		return res, actorstore.ErrInvalidRequestMissingParameters
	}

	var (
		hostsTable           = p.metadata.TableName(pgTableHosts)
		hostsActorTypesTable = p.metadata.TableName(pgTableHostsActorTypes)
		actorsTable          = p.metadata.TableName(pgTableActors)
	)

	// This query could fail if there's a race condition where the same actor is being invoked multiple times and it doesn't exist already
	// So, let's implement a retry in case of conflicts
	for i := 0; i < 3; i++ {
		queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
		defer queryCancel()

		err = p.db.QueryRow(queryCtx,
			fmt.Sprintf(lookupActorQuery, hostsTable, hostsActorTypesTable, actorsTable),
			ref.ActorType, ref.ActorID, p.metadata.Config.FailedInterval(),
		).Scan(&res.AppID, &res.Address, &res.IdleTimeout)

		if err == nil {
			break
		} else {
			// If we got no rows, it means that we don't have a host that supports actors of the given type
			if errors.Is(err, pgx.ErrNoRows) {
				return res, actorstore.ErrNoActorHost
			}

			// Retry if the error is the violation of a unique constraint
			if isUniqueViolationError(err) {
				select {
				case <-time.After(50 * time.Millisecond):
					// nop
				case <-ctx.Done():
					return res, ctx.Err()
				}
				continue
			}

			// Return in case of other errors
			return res, fmt.Errorf("database error: %w", err)
		}
	}

	return res, nil
}

func (p *PostgreSQL) RemoveActor(ctx context.Context, ref actorstore.ActorRef) error {
	if ref.ActorType == "" || ref.ActorID == "" {
		return actorstore.ErrInvalidRequestMissingParameters
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer queryCancel()
	q := fmt.Sprintf("DELETE FROM %s WHERE actor_type = $1 AND actor_id = $2", p.metadata.TableName(pgTableActors))
	res, err := p.db.Exec(queryCtx, q, ref.ActorType, ref.ActorID)
	if err != nil {
		return fmt.Errorf("failed to remove actor: %w", err)
	}
	if res.RowsAffected() == 0 {
		return actorstore.ErrActorNotFound
	}

	return nil
}

// Returns true if the error is a unique constraint violation error, such as a duplicate unique index or primary key.
func isUniqueViolationError(err error) bool {
	if err == nil {
		return false
	}

	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}

func executeInTransaction[T any](ctx context.Context, log logger.Logger, db *pgxpool.Pool, timeout time.Duration, fn func(ctx context.Context, tx pgx.Tx) (T, error)) (res T, err error) {
	// Start the transaction
	queryCtx, queryCancel := context.WithTimeout(ctx, timeout)
	defer queryCancel()
	tx, err := db.Begin(queryCtx)
	if err != nil {
		return res, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Rollback in case of failure
	var success bool
	defer func() {
		if success {
			return
		}
		rollbackCtx, rollbackCancel := context.WithTimeout(ctx, timeout)
		defer rollbackCancel()
		rollbackErr := tx.Rollback(rollbackCtx)
		if rollbackErr != nil {
			// Log errors only
			log.Errorf("Error while attempting to roll back transaction: %v", rollbackErr)
		}
	}()

	// Execute the callback
	res, err = fn(ctx, tx)
	if err != nil {
		return res, err
	}

	// Commit the transaction
	queryCtx, queryCancel = context.WithTimeout(ctx, timeout)
	defer queryCancel()
	err = tx.Commit(queryCtx)
	if err != nil {
		return res, fmt.Errorf("failed to commit transaction: %w", err)
	}
	success = true

	return res, nil
}
