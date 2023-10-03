//go:build conftests

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
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/components-contrib/actorstore/tests"
)

/*
This file contains additional methods that are only used for testing.
It is compiled only when the "conftests" tag is enabled
*/

// GetConn returns the database connection.
func (p *PostgreSQL) GetConn() *pgxpool.Pool {
	return p.db
}

// GetAllHosts returns the entire list of hosts in the database.
func (p *PostgreSQL) GetAllHosts() (map[string]tests.TestDataHost, error) {
	// Use a transaction for consistency
	return executeInTransaction(context.Background(), p.logger, p.db, time.Minute, func(ctx context.Context, tx pgx.Tx) (map[string]tests.TestDataHost, error) {
		res := map[string]tests.TestDataHost{}

		// First, load all hosts
		rows, err := tx.Query(ctx, "SELECT host_id, host_address, host_app_id, host_actors_api_level, host_last_healthcheck FROM "+p.metadata.TableName(pgTableHosts))
		if err != nil {
			return nil, fmt.Errorf("failed to load data from the hosts table: %w", err)
		}

		for rows.Next() {
			var hostID string
			r := tests.TestDataHost{
				ActorTypes: map[string]tests.TestDataActorType{},
			}
			err = rows.Scan(&hostID, &r.Address, &r.AppID, &r.APILevel, &r.LastHealthCheck)
			if err != nil {
				return nil, fmt.Errorf("failed to load data from the hosts table: %w", err)
			}
			res[hostID] = r
		}

		// Load all actor types
		rows, err = tx.Query(ctx, "SELECT host_id, actor_type, actor_idle_timeout FROM "+p.metadata.TableName(pgTableHostsActorTypes))
		if err != nil {
			return nil, fmt.Errorf("failed to load data from the hosts actor types table: %w", err)
		}

		for rows.Next() {
			var (
				hostID      string
				actorType   string
				idleTimeout int
			)
			err = rows.Scan(&hostID, &actorType, &idleTimeout)
			if err != nil {
				return nil, fmt.Errorf("failed to load data from the hosts actor types table: %w", err)
			}

			host, ok := res[hostID]
			if !ok {
				// Should never happen, given that host_id has a foreign key reference to the hosts table…
				return nil, fmt.Errorf("hosts actor types table contains data for inexisted host ID: %s", hostID)
			}
			host.ActorTypes[actorType] = tests.TestDataActorType{
				IdleTimeout: time.Duration(idleTimeout) * time.Second,
				ActorIDs:    make([]string, 0),
			}
		}

		// Lastly, load all actor IDs
		rows, err = tx.Query(ctx, "SELECT actor_type, actor_id, host_id FROM "+p.metadata.TableName(pgTableActors))
		if err != nil {
			return nil, fmt.Errorf("failed to load data from the actors table: %w", err)
		}

		for rows.Next() {
			var (
				actorType string
				actorID   string
				hostID    string
			)
			err = rows.Scan(&actorType, &actorID, &hostID)
			if err != nil {
				return nil, fmt.Errorf("failed to load data from the actors table: %w", err)
			}
		}

		return res, nil
	})
}
