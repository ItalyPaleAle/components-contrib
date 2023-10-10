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
	"time"
)

// StoreConfTests is defined here to include the methods that are to be implemented for conftests.
// This file has the "conftests" build tag.
// The same is defined in actorstore_no_conftests.go, where there's no build tag, as an empty interface.
type StoreConfTests interface {
	// Cleanup performs a cleanup of test resources.
	Cleanup()

	// GetAllHosts returns the entire list of hosts in the database.
	GetAllHosts() (map[string]TestDataHost, error)

	// GetAllReminders returns the entire list of reminders in the database.
	GetAllReminders() (map[string]TestDataReminder, error)

	// LoadTestData loads all test data in the database.
	LoadTestData(testData TestData) error
}

type TestData struct {
	Hosts     map[string]TestDataHost
	Reminders map[string]TestDataReminder
}

type TestDataHost struct {
	Address         string
	AppID           string
	APILevel        int
	LastHealthCheck time.Time
	ActorTypes      map[string]TestDataActorType
}

func (t TestDataHost) IsActive(hostHealthCheckInterval time.Duration) bool {
	return time.Since(t.LastHealthCheck) < hostHealthCheckInterval
}

type TestDataActorType struct {
	IdleTimeout time.Duration
	ActorIDs    []string
}

type TestDataReminder struct {
	ActorType     string
	ActorID       string
	Name          string
	ExecutionTime time.Time
	LeaseID       *string
	LeaseTime     *time.Time
	LeasePID      *string
}

func (t TestData) HostsByActorType(hostHealthCheckInterval time.Duration) map[string][]string {
	res := make(map[string][]string)
	for hostID, host := range t.Hosts {
		for at := range host.ActorTypes {
			if !host.IsActive(hostHealthCheckInterval) {
				continue
			}

			if res[at] == nil {
				res[at] = []string{hostID}
			} else {
				res[at] = append(res[at], hostID)
			}
		}
	}
	return res
}

func (t TestData) HostsForActorType(actorType string, hostHealthCheckInterval time.Duration) []string {
	res := make([]string, 0)
	for hostID, host := range t.Hosts {
		if !host.IsActive(hostHealthCheckInterval) {
			continue
		}

		_, ok := host.ActorTypes[actorType]
		if ok {
			res = append(res, hostID)
		}
	}
	return res
}
