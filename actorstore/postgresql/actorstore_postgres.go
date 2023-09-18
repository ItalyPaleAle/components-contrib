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
	"fmt"

	"github.com/dapr/components-contrib/actorstore"
	"github.com/dapr/kit/logger"
	"github.com/jackc/pgx/v5/pgxpool"
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
}

func (p *PostgreSQL) Init(ctx context.Context, md actorstore.Metadata) error {
	err := p.metadata.InitWithMetadata(md)
	if err != nil {
		p.logger.Errorf("Failed to parse metadata: %v", err)
		return err
	}

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

	pingCtx, pingCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	err = p.db.Ping(pingCtx)
	pingCancel()
	if err != nil {
		err = fmt.Errorf("failed to ping the database: %w", err)
		p.logger.Error(err)
		return err
	}

	err = p.migrateFn(ctx, p.db, MigrateOptions{
		Logger:            p.logger,
		StateTableName:    p.metadata.TableName,
		MetadataTableName: p.metadata.MetadataTableName,
	})
	if err != nil {
		return err
	}
}

func (p *PostgreSQL) Ping(ctx context.Context) error

func (p *PostgreSQL) Close() error

func (p *PostgreSQL) AddActorHost(ctx context.Context, properties actorstore.AddActorHostRequest) (string, error)

func (p *PostgreSQL) UpdateActorHost(ctx context.Context, actorHostID string, properties actorstore.UpdateActorHostRequest) error

func (p *PostgreSQL) RemoveActorHost(ctx context.Context, actorHostID string) error

func (p *PostgreSQL) LookupActor(ctx context.Context, ref actorstore.ActorRef) (actorstore.LookupActorResponse, error)

func (p *PostgreSQL) RemoveActor(ctx context.Context, ref actorstore.ActorRef) error
