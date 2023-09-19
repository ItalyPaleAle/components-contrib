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
			p.logger.Info("Creating tables for actors state. Hosts table: '%s'. Hosts actor types table: '%s'. Actors table: '%s'", hostsTable, hostsActorTypesTable, actorsTable)
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

	if p.db != nil {
		err = p.Close()
	}
	return err
}

func (p *PostgreSQL) AddActorHost(ctx context.Context, properties actorstore.AddActorHostRequest) (string, error) {
	if properties.AppID == "" || properties.Address == "" || properties.ApiLevel <= 0 {
		return "", actorstore.ErrInvalidRequestMissingParameters
	}

	// Because we need to update 2 tables, we need a transaction
	return executeInTransaction(ctx, p.logger, p.db, p.metadata.Timeout, func(ctx context.Context, tx pgx.Tx) (hostID string, err error) {
		var (
			hostsTable           = p.metadata.TableName(pgTableHosts)
			hostsActorTypesTable = p.metadata.TableName(pgTableHostsActorTypes)
		)

		// First, add the actor host
		queryCtx, queryCancel := context.WithTimeout(ctx, p.metadata.Timeout)
		defer queryCancel()
		query := fmt.Sprintf(
			`INSERT INTO %s
				(host_address, host_app_id, host_actors_api_level, host_last_healthcheck)
			VALUES
				($1, $2, $3, CURRENT_TIMESTAMP)
			RETURNING host_id`,
			hostsTable,
		)
		err = tx.
			QueryRow(queryCtx, query, properties.Address, properties.AppID, properties.ApiLevel).
			Scan(&hostID)
		if err != nil {
			if isUniqueViolationError(err) {
				return "", actorstore.ErrActorHostConflict
			}
			return "", fmt.Errorf("failed to insert actor host in hosts table: %w", err)
		}

		// Register each supported actor type
		queryCtx, queryCancel = context.WithTimeout(ctx, p.metadata.Timeout)
		defer queryCancel()
		rows := make([][]any, len(properties.ActorTypes))
		for i, t := range properties.ActorTypes {
			rows[i] = []any{
				hostID,
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
			return "", fmt.Errorf("failed to insert supported actor types in hosts actor types table: %w", err)
		}
		if n != int64(len(properties.ActorTypes)) {
			return "", fmt.Errorf("failed to insert supported actor types in hosts actor types table: inserted %d rows, but expected %d", n, len(properties.ActorTypes))
		}

		return hostID, nil
	})
}

func (p *PostgreSQL) UpdateActorHost(ctx context.Context, actorHostID string, properties actorstore.UpdateActorHostRequest) error

func (p *PostgreSQL) RemoveActorHost(ctx context.Context, actorHostID string) error

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
			ref.ActorType, ref.ActorID,
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

func (p *PostgreSQL) RemoveActor(ctx context.Context, ref actorstore.ActorRef) error

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
