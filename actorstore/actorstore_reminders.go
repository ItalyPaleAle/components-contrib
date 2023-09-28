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
	// It returns ErrReminderNotFound if it doesn't exist.
	GetReminder(ctx context.Context, req ReminderRef) (GetReminderResponse, error)

	// CreateReminder creates a new reminder.
	CreateReminder(ctx context.Context, req CreateReminderRequest) error

	// CreateLeasedReminder is like CreateReminder, but acquires the lease for the newly-created reminder right away.
	// It returns the created reminder's data.
	// If the returned data is nil, it means that the row was inserted, but we couldn't get a lease.
	CreateLeasedReminder(ctx context.Context, req CreateLeasedReminderRequest) (res *FetchedReminder, err error)

	// DeleteReminder deletes an existing reminder before it fires.
	// It erturns ErrReminderNotFound if it doesn't exist.
	DeleteReminder(ctx context.Context, req ReminderRef) error

	// FetchNextReminders retrieves the list of upcoming reminders, acquiring a lock on them.
	FetchNextReminders(ctx context.Context, req FetchNextRemindersRequest) ([]*FetchedReminder, error)

	// GetReminderWithLease retrieves a reminder from a FetchedReminder object that contains a lease too.
	// It returns ErrReminderNotFound if it doesn't exist or the lease is invalid.
	GetReminderWithLease(ctx context.Context, req *FetchedReminder) (res Reminder, err error)
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
	// Delay from current time.
	Delay time.Duration
	// Reminder repetition period.
	Period *string
	// Deadline for repeating reminders (can be nil).
	TTL *time.Time
	// Data for the reminder (can be nil).
	Data []byte
}

// IsValid returns true if all required fields are present.
func (r ReminderOptions) IsValid() bool {
	// Nothing to validate at this time
	// If ExecutionTime is zero, we'll take whatever the delay value is, even if zero.
	return true
}

// GetReminderResponse is the response from GetReminder.
type GetReminderResponse struct {
	ReminderOptions
}

// CreateReminderRequest is the request for CreateReminder.
type CreateReminderRequest = Reminder

// Reminder includes a full reminder, with all its properties.
type Reminder struct {
	ReminderRef
	ReminderOptions
}

// IsValid returns true if all required fields are present.
func (r Reminder) IsValid() bool {
	return r.ReminderRef.IsValid() && r.ReminderOptions.IsValid()
}

// CreateLeasedReminderRequest is the request for CreateLeasedReminder.
type CreateLeasedReminderRequest struct {
	// Reminder data
	Reminder Reminder

	// List of hosts with active connections to this actor service instance.
	Hosts []string
	// List of actor types supported by hosts with active connections to this instance of the actor service.
	ActorTypes []string
}

// FetchNextRemindersRequest is the request for FetchNextReminders.
type FetchNextRemindersRequest struct {
	// List of hosts with active connections to this actor service instance.
	Hosts []string
	// List of actor types supported by hosts with active connections to this instance of the actor service.
	ActorTypes []string
}

// FetchedReminder is the type for the reminders returned by FetchNextReminders.
type FetchedReminder struct {
	key           string
	executionTime time.Time
	lease         any
}

// NewFetchedReminder returns a new FetchedReminder object.
func NewFetchedReminder(key string, executionTime time.Time, lease any) FetchedReminder {
	return FetchedReminder{
		key:           key,
		executionTime: executionTime,
		lease:         lease,
	}
}

// Key implements the queuable interface.
func (r FetchedReminder) Key() string {
	return r.key
}

// ScheduledTime implements the queuable interface.
func (r FetchedReminder) ScheduledTime() time.Time {
	return r.executionTime
}

// Lease returns the value of the lease property.
func (r FetchedReminder) Lease() any {
	return r.lease
}
