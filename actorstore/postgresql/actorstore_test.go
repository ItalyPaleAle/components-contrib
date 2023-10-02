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
	"os"
	"testing"
	"time"

	"github.com/dapr/components-contrib/actorstore"
	"github.com/dapr/components-contrib/actorstore/tests"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/require"
)

func TestComponent(t *testing.T) {
	connString := os.Getenv("POSTGRES_CONNSTRING")
	if connString == "" {
		t.Skip("Test skipped because the 'POSTGRES_CONNSTRING' is missing")
	}

	store := NewPostgreSQLActorStore(logger.NewLogger("conftests"))

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
					"metadataTableName": "conftests_metadata",
					"tablePrefix":       "conftests_",
				},
			},
		})
		require.NoError(t, err)
	})

	require.False(t, t.Failed(), "Cannot continue if 'Init' test has failed")

	t.Run("Load test data", func(t *testing.T) {

	})

	require.False(t, t.Failed(), "Cannot continue if 'Load test data' test has failed")
}
