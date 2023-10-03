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
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

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

	// Define cleanupFn and make sure it runs even if the tests fail
	cleanupDone := false
	cleanupFn := func() {
		if cleanupDone {
			return
		}

		log.Info("Removing tables")
		for _, table := range []pgTable{pgTableActors, pgTableHostsActorTypes, pgTableHosts, pgTableReminders, "metadata"} {
			log.Info("Removing table %s", store.metadata.TableName(table))
			_, err := store.GetConn().Exec(context.Background(), fmt.Sprintf("DROP TABLE %s", store.metadata.TableName(table)))
			if err != nil {
				log.Errorf("Failed to remove table %s: %v", table, err)
			}
		}
		cleanupDone = true
	}
	t.Cleanup(cleanupFn)

	t.Run("Load test data", func(t *testing.T) {
		testData := tests.GetTestData()

		hosts := [][]any{}
		hostsActorTypes := [][]any{}
		actors := [][]any{}
		reminders := [][]any{}

		for hostID, host := range testData.Hosts {
			hosts = append(hosts, []any{hostID, host.Address, host.AppID, host.APILevel, host.LastHealthCheck})

			for actorType, at := range host.ActorTypes {
				hostsActorTypes = append(hostsActorTypes, []any{hostID, actorType, int(at.IdleTimeout.Seconds())})

				for _, actorID := range at.ActorIDs {
					actors = append(actors, []any{actorType, actorID, hostID, int(at.IdleTimeout.Seconds())})
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

	t.Run("Actor state", actorStateTests(store))

	// Perform cleanup before Close test
	cleanupFn()

	t.Run("Close", func(t *testing.T) {
		err := store.Close()
		require.NoError(t, err)
	})
}

func actorStateTests(store *PostgreSQL) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("Add new host", func(t *testing.T) {
			before, err := store.GetAllHosts()
			require.NoError(t, err)

			// Add
			hostID, err := store.AddActorHost(context.Background(), actorstore.AddActorHostRequest{
				AppID:   "newapp1",
				Address: "10.10.10.10",
				ActorTypes: []actorstore.ActorHostType{
					{ActorType: "newtype1", IdleTimeout: 20},
				},
				APILevel: 10,
			})
			require.NoError(t, err)
			require.NotEmpty(t, hostID)

			// Verify
			after, err := store.GetAllHosts()
			require.NoError(t, err)

			// 50d7623f-b165-4f9e-9f05-3b7a1280b222 should have been deleted because its last healthcheck was before the interval
			// The newly-added item should be in its place
			expectHosts := maps.Keys(before)
			for i, v := range expectHosts {
				if v == "50d7623f-b165-4f9e-9f05-3b7a1280b222" {
					expectHosts[i] = hostID
				}
			}

			afterHosts := maps.Keys(after)
			slices.Sort(expectHosts)
			slices.Sort(afterHosts)
			require.Equal(t, expectHosts, afterHosts)
		})
	}
}
