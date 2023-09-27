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
	"errors"
	"time"
)

// ErrReminderNotFound is returned by GetReminder and DeleteReminder when the reminder doesn't exist.
var ErrReminderNotFound = errors.New("reminder not found")

// StoreReminders is the part of the Store interface for managing reminders.
type StoreReminders interface {
	// GetReminder returns a reminder.
	// It erturns ErrReminderNotFound if it doesn't exist.
	GetReminder(ctx context.Context, req ReminderRef) (GetReminderResponse, error)

	// CreateReminder creates a new reminder.
	CreateReminder(ctx context.Context, req CreateReminderRequest) error

	// DeleteReminder deletes an existing reminder before it fires.
	// It erturns ErrReminderNotFound if it doesn't exist.
	DeleteReminder(ctx context.Context, req ReminderRef) error

	// FetchNextReminders retrieves the list of upcoming reminders, acquiring a lock on them.
	FetchNextReminders(ctx context.Context, req FetchNextRemindersRequest) ([]FetchedReminder, error)
}

// ReminderRef is the reference to a reminder (reminder name, actor type and ID).
type ReminderRef struct {
	// Actor type for the reminder.
	ActorType string
	// Actor ID for the reminder.
	ActorID string
	// Name of the reminder
	Name string
}

// IsValid returns true if all required fields are present.
func (r ReminderRef) IsValid() bool {
	return r.ActorType != "" && r.ActorID != "" && r.Name != ""
}

// ReminderOptions contains the options for a reminder.
type ReminderOptions struct {
	// Scheduled execution time.
	ExecutionTime time.Time
	// Reminder repetition period.
	Period *string
	// Deadline for repeating reminders (can be nil).
	TTL *time.Time
	// Data for the reminder (can be nil).
	Data []byte
}

// IsValid returns true if all required fields are present.
func (r ReminderOptions) IsValid() bool {
	return !r.ExecutionTime.IsZero()
}

// GetReminderResponse is the response from GetReminder.
type GetReminderResponse struct {
	ReminderOptions
}

// CreateReminderRequest is the request for CreateReminder.
type CreateReminderRequest struct {
	ReminderRef
	ReminderOptions
}

// IsValid returns true if all required fields are present.
func (r CreateReminderRequest) IsValid() bool {
	return r.ReminderRef.IsValid() && r.ReminderOptions.IsValid()
}

// FetchNextRemindersRequest is the request for FetchNextReminders.
type FetchNextRemindersRequest struct {
	// List of hosts with active connections to this actor service instance.
	Hosts []string
}

// FetchedReminder is the type for the reminders returned by FetchNextReminders.
type FetchedReminder struct {
	ReminderRef
	ReminderOptions

	Lease any
}
