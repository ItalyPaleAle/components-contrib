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
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
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
			PID:           tests.GetTestPID(),
			Configuration: tests.GetActorsConfiguration(),
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
		var addedHostID string

		t.Run("Add new host", func(t *testing.T) {
			t.Run("Adding new hosts should purge expired ones", func(t *testing.T) {
				before, err := store.GetAllHosts()
				require.NoError(t, err)

				// Add
				addedHostID, err = store.AddActorHost(context.Background(), actorstore.AddActorHostRequest{
					AppID:   "newapp1",
					Address: "10.10.10.10",
					ActorTypes: []actorstore.ActorHostType{
						{ActorType: "newtype1", IdleTimeout: 20},
					},
					APILevel: 10,
				})
				require.NoError(t, err)
				require.NotEmpty(t, addedHostID)

				// Verify
				after, err := store.GetAllHosts()
				require.NoError(t, err)

				require.Len(t, after[addedHostID].ActorTypes, 1)
				require.Equal(t, 20*time.Second, after[addedHostID].ActorTypes["newtype1"].IdleTimeout)

				// 50d7623f-b165-4f9e-9f05-3b7a1280b222 should have been deleted because its last healthcheck was before the interval
				// The newly-added item should be in its place
				// Also note that deleting the actor host should have removed all actors that were hosted by this host
				// If that weren't the case, `GetAllHosts` should have returned an error
				expectHosts := maps.Keys(before)
				for i, v := range expectHosts {
					if v == "50d7623f-b165-4f9e-9f05-3b7a1280b222" {
						expectHosts[i] = addedHostID
					}
				}

				afterHosts := maps.Keys(after)
				slices.Sort(expectHosts)
				slices.Sort(afterHosts)
				require.Equal(t, expectHosts, afterHosts)
			})

			t.Run("Cannot register host with same address", func(t *testing.T) {
				before, err := store.GetAllHosts()
				require.NoError(t, err)

				// Add a host with the same address, just different appID
				_, err = store.AddActorHost(context.Background(), actorstore.AddActorHostRequest{
					AppID:   "newapp2",
					Address: "10.10.10.10",
					ActorTypes: []actorstore.ActorHostType{
						{ActorType: "newtype2", IdleTimeout: 10},
					},
					APILevel: 10,
				})
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrActorHostConflict)

				// Verify - nothing should have changed
				after, err := store.GetAllHosts()
				require.NoError(t, err)
				require.Equal(t, before, after)
			})
		})

		t.Run("Update existing host", func(t *testing.T) {
			t.Run("Update actor types", func(t *testing.T) {
				before, err := store.GetAllHosts()
				require.NoError(t, err)

				err = store.UpdateActorHost(context.Background(), addedHostID, actorstore.UpdateActorHostRequest{
					// Do not update last health check
					UpdateLastHealthCheck: false,
					ActorTypes: []actorstore.ActorHostType{
						{ActorType: "newtype", IdleTimeout: 10},
					},
				})
				require.NoError(t, err)

				// Verify
				after, err := store.GetAllHosts()
				require.NoError(t, err)

				expect := before[addedHostID]
				expect.ActorTypes = map[string]tests.TestDataActorType{
					"newtype": {
						IdleTimeout: 10 * time.Second,
						ActorIDs:    []string{},
					},
				}

				require.Equal(t, expect, after[addedHostID])
			})

			t.Run("Update host last healthcheck", func(t *testing.T) {
				before, err := store.GetAllHosts()
				require.NoError(t, err)

				err = store.UpdateActorHost(context.Background(), addedHostID, actorstore.UpdateActorHostRequest{
					UpdateLastHealthCheck: true,
				})
				require.NoError(t, err)

				// Verify
				after, err := store.GetAllHosts()
				require.NoError(t, err)

				require.Equal(t, before[addedHostID].AppID, after[addedHostID].AppID)
				require.Equal(t, before[addedHostID].Address, after[addedHostID].Address)
				require.Equal(t, before[addedHostID].ActorTypes, after[addedHostID].ActorTypes)
				require.True(t, before[addedHostID].LastHealthCheck.Before(after[addedHostID].LastHealthCheck))
			})

			t.Run("Error when host ID is empty", func(t *testing.T) {
				err := store.UpdateActorHost(context.Background(), "", actorstore.UpdateActorHostRequest{
					UpdateLastHealthCheck: true,
				})
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrInvalidRequestMissingParameters)
			})

			t.Run("Error when nothing to update", func(t *testing.T) {
				err := store.UpdateActorHost(context.Background(), "d0d8b4c1-0b34-4e8e-9163-a8ac72c4a0d6", actorstore.UpdateActorHostRequest{})
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrInvalidRequestMissingParameters)
			})

			t.Run("Error when host doesn't exist", func(t *testing.T) {
				err := store.UpdateActorHost(context.Background(), "d0d8b4c1-0b34-4e8e-9163-a8ac72c4a0d6", actorstore.UpdateActorHostRequest{
					UpdateLastHealthCheck: true,
				})
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrActorHostNotFound)
			})
		})

		t.Run("Remove actor host", func(t *testing.T) {
			t.Run("Remove existing host", func(t *testing.T) {
				before, err := store.GetAllHosts()
				require.NoError(t, err)

				err = store.RemoveActorHost(context.Background(), addedHostID)
				require.NoError(t, err)

				// Verify
				after, err := store.GetAllHosts()
				require.NoError(t, err)

				require.Len(t, after, len(before)-1)
				require.Empty(t, after[addedHostID])
			})

			t.Run("Error when host ID is empty", func(t *testing.T) {
				err := store.RemoveActorHost(context.Background(), "")
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrInvalidRequestMissingParameters)
			})

			t.Run("Error when host doesn't exist", func(t *testing.T) {
				err := store.RemoveActorHost(context.Background(), "d0d8b4c1-0b34-4e8e-9163-a8ac72c4a0d6")
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrActorHostNotFound)
			})
		})

		t.Run("Lookup actor", func(t *testing.T) {
			testData := tests.GetTestData()
			hosts, err := store.GetAllHosts()
			require.NoError(t, err)

			t.Run("Active actor, no options", func(t *testing.T) {
				// Test vectors: key is "actor-type/actor-id" and value is expected host ID
				tt := map[string]string{
					"type-B/type-B.211": "ded1e507-ed4a-4322-a3a4-b5e8719a9333",
					"type-C/type-C.12":  "ded1e507-ed4a-4322-a3a4-b5e8719a9333",
					"type-B/type-B.222": "f4c7d514-3468-48dd-9103-297bf7fe91fd",
					"type-A/type-A.11":  "7de434ce-e285-444f-9857-4d30cade3111",
					"type-A/type-A.12":  "7de434ce-e285-444f-9857-4d30cade3111",
					"type-B/type-B.112": "7de434ce-e285-444f-9857-4d30cade3111",
				}

				for k, v := range tt {
					t.Run(k, func(t *testing.T) {
						ref := actorstore.ActorRef{}
						ref.ActorType, ref.ActorID, _ = strings.Cut(k, "/")
						res, err := store.LookupActor(context.Background(), ref, actorstore.LookupActorOpts{})
						require.NoError(t, err)

						require.Equal(t, v, res.HostID)
						require.Equal(t, hosts[v].AppID, res.AppID)
						require.Equal(t, hosts[v].Address, res.Address)
						require.EqualValues(t, hosts[v].ActorTypes[ref.ActorType].IdleTimeout.Seconds(), res.IdleTimeout)
					})
				}
			})

			t.Run("Inactive actor, no options", func(t *testing.T) {
				const iterationsPerActorType = 50
				for at, atHosts := range testData.HostsByActorType() {
					t.Run(at, func(t *testing.T) {
						counts := map[string]int{}
						for _, host := range atHosts {
							counts[host] = 0
						}

						for i := 0; i < iterationsPerActorType; i++ {
							res, err := store.LookupActor(context.Background(), actorstore.ActorRef{
								ActorType: at,
								ActorID:   fmt.Sprintf("inactive-%d", i),
							}, actorstore.LookupActorOpts{})
							require.NoErrorf(t, err, "Failed on iteration %s/%d", at, i)

							require.Containsf(t, atHosts, res.HostID, "Failed on iteration %s/%d", at, i)
							require.Equalf(t, hosts[res.HostID].AppID, res.AppID, "Failed on iteration %s/%d", at, i)
							require.Equalf(t, hosts[res.HostID].Address, res.Address, "Failed on iteration %s/%d", at, i)
							require.EqualValuesf(t, hosts[res.HostID].ActorTypes[at].IdleTimeout.Seconds(), res.IdleTimeout, "Failed on iteration %s/%d", at, i)

							counts[res.HostID]++
						}

						// Ideally we'd have a perfectly uniform distribution of actors across all hosts
						// But this isn't always the case, so we will only assert that at least 1/10th of iterationsPerActorType is assigned to each host
						for _, host := range atHosts {
							assert.GreaterOrEqualf(t, counts[host], iterationsPerActorType/10, "Failed on host %s", host)
						}
					})
				}
			})
		})
	}
}
