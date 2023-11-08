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

package actorstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/actorstore"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/kit/config"
)

type TestConfig struct {
	utils.CommonConfig
}

func NewTestConfig(component string, operations []string, configMap map[string]interface{}) (TestConfig, error) {
	testConfig := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "actorstore",
			ComponentName: component,
			Operations:    utils.NewStringSet(operations...),
		},
	}

	err := config.Decode(configMap, &testConfig)
	if err != nil {
		return testConfig, err
	}

	return testConfig, nil
}

// ConformanceTests runs conf tests for actor stores.
func ConformanceTests(t *testing.T, props map[string]string, store actorstore.Store, config TestConfig) {
	t.Run("Init", func(t *testing.T) {
		err := store.Init(context.Background(), actorstore.Metadata{
			PID:           GetTestPID(),
			Configuration: GetActorsConfiguration(),
			Base: metadata.Base{
				Properties: props,
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

		cleanupErr := store.Cleanup()
		assert.NoError(t, cleanupErr)
		cleanupDone = true
	}
	t.Cleanup(cleanupFn)

	t.Run("Actor state", actorStateTests(store))

	t.Run("Reminders", remindersTest(store))

	// Perform cleanup before Close test
	cleanupFn()

	t.Run("Close", func(t *testing.T) {
		err := store.Close()
		require.NoError(t, err)
	})
}

func loadActorStateTestData(store actorstore.Store) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()
		require.NoError(t, store.LoadActorStateTestData(GetTestData()), "Failed to load actor state test data")
	}
}
