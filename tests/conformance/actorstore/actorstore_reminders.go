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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/actorstore"
	"github.com/dapr/kit/ptr"
)

func remindersTest(store actorstore.Store) func(t *testing.T) {
	return func(t *testing.T) {
		testData := GetTestData()

		t.Run("Get a reminder", func(t *testing.T) {
			t.Run("Retrieve an existing reminder", func(t *testing.T) {
				r := testData.Reminders["f647315e-ffeb-4727-8a7a-539bb0d3e3cc"]
				res, err := store.GetReminder(context.Background(), actorstore.ReminderRef{
					ActorType: r.ActorType,
					ActorID:   r.ActorID,
					Name:      r.Name,
				})

				require.NoError(t, err)
				assert.InDelta(t, r.ExecutionTime.UnixNano(), res.ExecutionTime.UnixNano(), float64(time.Second/2))
			})

			t.Run("Error when reminder doesn't exist", func(t *testing.T) {
				_, err := store.GetReminder(context.Background(), actorstore.ReminderRef{
					ActorType: "notfound",
					ActorID:   "notfound",
					Name:      "notfound",
				})
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrReminderNotFound)
			})

			t.Run("Error when missing ActorType", func(t *testing.T) {
				_, err := store.GetReminder(context.Background(), actorstore.ReminderRef{
					ActorType: "",
					ActorID:   "myactorid",
					Name:      "myreminder",
				})
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrInvalidRequestMissingParameters)
			})

			t.Run("Error when missing ActorID", func(t *testing.T) {
				_, err := store.GetReminder(context.Background(), actorstore.ReminderRef{
					ActorType: "myactortype",
					ActorID:   "",
					Name:      "myreminder",
				})
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrInvalidRequestMissingParameters)
			})

			t.Run("Error when missing Name", func(t *testing.T) {
				_, err := store.GetReminder(context.Background(), actorstore.ReminderRef{
					ActorType: "myactortype",
					ActorID:   "myactorid",
					Name:      "",
				})
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrInvalidRequestMissingParameters)
			})
		})

		t.Run("Create a reminder", func(t *testing.T) {
			t.Run("Create a new reminder with fixed execution time, period, and data", func(t *testing.T) {
				ref := actorstore.ReminderRef{
					ActorType: "type-D",
					ActorID:   "type-D.1",
					Name:      "type-D.1.1",
				}
				opts := actorstore.ReminderOptions{
					ExecutionTime: time.Now().Add(time.Minute),
					Period:        ptr.Of("1m"),
					Data:          []byte("almeno tu nell'universo"),
				}

				err := store.CreateReminder(context.Background(), actorstore.Reminder{
					ReminderRef:     ref,
					ReminderOptions: opts,
				})
				require.NoError(t, err)

				res, err := store.GetReminder(context.Background(), ref)
				require.NoError(t, err)
				if assert.NotNil(t, res.Period) {
					assert.Equal(t, *opts.Period, *res.Period)
				}
				assert.Nil(t, res.TTL)
				assert.Equal(t, opts.Data, res.Data)
				assert.InDelta(t, opts.ExecutionTime.UnixNano(), res.ExecutionTime.UnixNano(), float64(time.Second/2))
			})

			t.Run("Create a new reminder with a delay and TTL", func(t *testing.T) {
				now := time.Now()
				ttl := now.Add(time.Hour)
				ref := actorstore.ReminderRef{
					ActorType: "type-D",
					ActorID:   "type-D.1",
					Name:      "type-D.1.2",
				}
				opts := actorstore.ReminderOptions{
					Delay: time.Minute,
					TTL:   &ttl,
				}

				err := store.CreateReminder(context.Background(), actorstore.Reminder{
					ReminderRef:     ref,
					ReminderOptions: opts,
				})
				require.NoError(t, err)

				res, err := store.GetReminder(context.Background(), ref)
				require.NoError(t, err)
				if assert.NotNil(t, res.TTL) {
					assert.InDelta(t, ttl.UnixNano(), res.TTL.UnixNano(), float64(time.Second/2))
				}
				assert.Nil(t, res.Period)
				assert.Empty(t, res.Data)
				assert.InDelta(t, now.Add(opts.Delay).UnixNano(), res.ExecutionTime.UnixNano(), float64(time.Second/2))
			})

			t.Run("Create a reminder with 0s delay", func(t *testing.T) {
				now := time.Now()
				ref := actorstore.ReminderRef{
					ActorType: "type-D",
					ActorID:   "type-D.1",
					Name:      "type-D.1.3",
				}

				err := store.CreateReminder(context.Background(), actorstore.Reminder{
					ReminderRef:     ref,
					ReminderOptions: actorstore.ReminderOptions{
						// Empty: no ExecutionTime and no Delay
						// Equals to 0 delay
					},
				})
				require.NoError(t, err)

				res, err := store.GetReminder(context.Background(), ref)
				require.NoError(t, err)
				assert.Nil(t, res.TTL)
				assert.Nil(t, res.Period)
				assert.Empty(t, res.Data)
				assert.InDelta(t, now.UnixNano(), res.ExecutionTime.UnixNano(), float64(time.Second/2))
			})

			t.Run("Replace an existing reminder", func(t *testing.T) {
				now := time.Now()
				ref := actorstore.ReminderRef{
					ActorType: "type-D",
					ActorID:   "type-D.1",
					Name:      "type-D.1.1",
				}
				opts := actorstore.ReminderOptions{
					// Should delete period and data
					Delay: 2 * time.Minute,
				}

				err := store.CreateReminder(context.Background(), actorstore.Reminder{
					ReminderRef:     ref,
					ReminderOptions: opts,
				})
				require.NoError(t, err)

				res, err := store.GetReminder(context.Background(), ref)
				require.NoError(t, err)
				assert.Nil(t, res.TTL)
				assert.Nil(t, res.Period)
				assert.Empty(t, res.Data)
				assert.InDelta(t, now.Add(opts.Delay).UnixNano(), res.ExecutionTime.UnixNano(), float64(time.Second/2))
			})

			t.Run("Error with invalid ReminderRef", func(t *testing.T) {
				err := store.CreateReminder(context.Background(), actorstore.CreateReminderRequest{
					ReminderRef: actorstore.ReminderRef{
						ActorType: "",
						ActorID:   "myactorid",
						Name:      "myreminder",
					},
					ReminderOptions: actorstore.ReminderOptions{
						Delay: 2 * time.Minute,
					},
				})
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrInvalidRequestMissingParameters)
			})
		})

		t.Run("Delete a reminder", func(t *testing.T) {
			t.Run("Delete an existing reminder", func(t *testing.T) {
				ref := actorstore.ReminderRef{
					ActorType: "type-D",
					ActorID:   "type-D.1",
					Name:      "type-D.1.1",
				}
				err := store.DeleteReminder(context.Background(), ref)
				require.NoError(t, err)

				_, err = store.GetReminder(context.Background(), ref)
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrReminderNotFound)
			})

			t.Run("Error when reminder doesn't exist", func(t *testing.T) {
				err := store.DeleteReminder(context.Background(), actorstore.ReminderRef{
					ActorType: "notfound",
					ActorID:   "notfound",
					Name:      "notfound",
				})
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrReminderNotFound)
			})

			t.Run("Error with invalid ReminderRef", func(t *testing.T) {
				_, err := store.GetReminder(context.Background(), actorstore.ReminderRef{
					ActorType: "",
					ActorID:   "myactorid",
					Name:      "myreminder",
				})
				require.Error(t, err)
				require.ErrorIs(t, err, actorstore.ErrInvalidRequestMissingParameters)
			})
		})
	}
}
