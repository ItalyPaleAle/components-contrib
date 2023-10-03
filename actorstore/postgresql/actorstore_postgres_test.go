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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/actorstore"
	"github.com/dapr/components-contrib/actorstore/tests"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func generateTablePrefix(t *testing.T) string {
	b := make([]byte, 4)
	_, err := io.ReadFull(rand.Reader, b)
	require.NoError(t, err)

	return "conftests_" + hex.EncodeToString(b) + "_"
}

func TestComponent(t *testing.T) {
	connString := os.Getenv("POSTGRES_CONNSTRING")
	if connString == "" {
		t.Skip("Test skipped because the 'POSTGRES_CONNSTRING' is missing")
	}

	log := logger.NewLogger("conftests")
	store := NewPostgreSQLActorStore(log).(*PostgreSQL)
	tablePrefix := generateTablePrefix(t)

	log.Infof("Table prefix: %s", tablePrefix)

	t.Run("Init", func(t *testing.T) {
		err := store.Init(context.Background(), actorstore.Metadata{
			PID: tests.GetTestPID(),
			Configuration: actorstore.ActorsConfiguration{
				HostHealthCheckInterval:      time.Minute,
				RemindersFetchAheadInterval:  5 * time.Second,
				RemindersLeaseDuration:       10 * time.Second,
				RemindersFetchAheadBatchSize: 5,
			},
			Base: metadata.Base{
				Properties: map[string]string{
					"connectionString":  connString,
					"metadataTableName": tablePrefix + "metadata",
					"tablePrefix":       tablePrefix,
				},
			},
		})
		require.NoError(t, err)
	})

	require.False(t, t.Failed(), "Cannot continue if 'Init' test has failed")

	t.Cleanup(func() {
		log.Info("Removing tables")
		for _, table := range []pgTable{pgTableActors, pgTableHostsActorTypes, pgTableHosts, pgTableReminders, "metadata"} {
			_, err := store.GetConn().Exec(context.Background(), fmt.Sprintf("DROP TABLE %s", store.metadata.TableName(table)))
			if err != nil {
				log.Errorf("Failed to remove table %s: %v", table, err)
			}
		}
	})

	t.Run("Load test data", func(t *testing.T) {
		testData := tests.GetTestData()

		hosts := [][]any{}
		hostsActorTypes := [][]any{}
		actors := [][]any{}
		reminders := [][]any{}

		for hostID, host := range testData.Hosts {
			hosts = append(hosts, []any{hostID, host.Address, host.AppID, 10, host.LastHealthCheck})

			for _, at := range host.ActorTypes {
				hostsActorTypes = append(hostsActorTypes, []any{hostID, at.ActorType, int(at.IdleTimeout.Seconds())})

				for _, actorID := range at.ActorIDs {
					actors = append(actors, []any{at.ActorType, actorID, hostID, int(at.IdleTimeout.Seconds())})
				}
			}
		}

		for reminderID, reminder := range testData.Reminders {
			reminders = append(reminders, []any{
				reminderID, reminder.ActorType, reminder.ActorID, reminder.Name,
				reminder.ExecutionTime, reminder.LeaseID, reminder.LeaseTime, reminder.LeasePID,
			})
		}

		_, err := store.GetConn().CopyFrom(
			context.Background(),
			pgx.Identifier{store.metadata.TableName(pgTableHosts)},
			[]string{"host_id", "host_address", "host_app_id", "host_actors_api_level", "host_last_healthcheck"},
			pgx.CopyFromRows(hosts),
		)
		require.NoError(t, err, "Failed to load test data for hosts table")

		_, err = store.GetConn().CopyFrom(
			context.Background(),
			pgx.Identifier{store.metadata.TableName(pgTableHostsActorTypes)},
			[]string{"host_id", "actor_type", "actor_idle_timeout"},
			pgx.CopyFromRows(hostsActorTypes),
		)
		require.NoError(t, err, "Failed to load test data for hosts actor types table")

		_, err = store.GetConn().CopyFrom(
			context.Background(),
			pgx.Identifier{store.metadata.TableName(pgTableActors)},
			[]string{"actor_type", "actor_id", "host_id", "actor_idle_timeout"},
			pgx.CopyFromRows(actors),
		)
		require.NoError(t, err, "Failed to load test data for actors table")

		_, err = store.GetConn().CopyFrom(
			context.Background(),
			pgx.Identifier{store.metadata.TableName(pgTableReminders)},
			[]string{"reminder_id", "actor_type", "actor_id", "reminder_name", "reminder_execution_time", "reminder_lease_id", "reminder_lease_time", "reminder_lease_pid"},
			pgx.CopyFromRows(reminders),
		)
		require.NoError(t, err, "Failed to load test data for reminders table")
	})

	require.False(t, t.Failed(), "Cannot continue if 'Load test data' test has failed")
}
