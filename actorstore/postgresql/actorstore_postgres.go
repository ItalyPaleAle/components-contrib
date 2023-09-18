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
	}

	var (
		hostsTable           = p.metadata.TablePrefix + "hosts"
		hostsActorTypesTable = p.metadata.TablePrefix + "hosts_actor_types"
		actorsTable          = p.metadata.TablePrefix + "actors"
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

func (p *PostgreSQL) AddActorHost(ctx context.Context, properties actorstore.AddActorHostRequest) (string, error)

func (p *PostgreSQL) UpdateActorHost(ctx context.Context, actorHostID string, properties actorstore.UpdateActorHostRequest) error

func (p *PostgreSQL) RemoveActorHost(ctx context.Context, actorHostID string) error

func (p *PostgreSQL) LookupActor(ctx context.Context, ref actorstore.ActorRef) (actorstore.LookupActorResponse, error) {

}

func (p *PostgreSQL) RemoveActor(ctx context.Context, ref actorstore.ActorRef) error
