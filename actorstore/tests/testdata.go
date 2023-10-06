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

package tests

import (
	"time"

	"github.com/dapr/components-contrib/actorstore"
)

var (
	actorsConfiguration actorstore.ActorsConfiguration
	testData            TestData
)

const testPID = "a1b2c3d4"

func init() {
	now := time.Now()

	actorsConfiguration = actorstore.ActorsConfiguration{
		HostHealthCheckInterval:      time.Minute,
		RemindersFetchAheadInterval:  5 * time.Second,
		RemindersLeaseDuration:       10 * time.Second,
		RemindersFetchAheadBatchSize: 5,
	}

	testData = TestData{
		Hosts: map[string]TestDataHost{
			"7de434ce-e285-444f-9857-4d30cade3111": {
				Address:         "1.1.1.1",
				AppID:           "myapp1",
				LastHealthCheck: now,
				ActorTypes: map[string]TestDataActorType{
					"type-A": {
						IdleTimeout: 10 * time.Minute,
						ActorIDs: []string{
							"type-A.11",
							"type-A.12",
							"type-A.13",
						},
					},
					"type-B": {
						IdleTimeout: time.Hour,
						ActorIDs: []string{
							"type-B.111",
							"type-B.112",
						},
					},
				},
			},
			"50d7623f-b165-4f9e-9f05-3b7a1280b222": {
				Address:         "1.1.1.2",
				AppID:           "myapp1",
				LastHealthCheck: now.Add(-2 * time.Minute),
				ActorTypes: map[string]TestDataActorType{
					"type-A": {
						IdleTimeout: 10 * time.Minute,
						ActorIDs: []string{
							"type-A.21",
							"type-A.22",
						},
					},
					"type-B": {
						IdleTimeout: time.Hour,
						ActorIDs: []string{
							"type-B.121",
						},
					},
				},
			},
			"ded1e507-ed4a-4322-a3a4-b5e8719a9333": {
				Address:         "1.2.1.1",
				AppID:           "myapp2",
				LastHealthCheck: now,
				ActorTypes: map[string]TestDataActorType{
					"type-B": {
						IdleTimeout: time.Hour,
						ActorIDs: []string{
							"type-B.211",
						},
					},
					"type-C": {
						IdleTimeout: 30 * time.Second,
						ActorIDs: []string{
							"type-C.11",
							"type-C.12",
							"type-C.13",
						},
					},
				},
			},
			"f4c7d514-3468-48dd-9103-297bf7fe91fd": {
				Address:         "1.2.1.2",
				AppID:           "myapp2",
				LastHealthCheck: now,
				ActorTypes: map[string]TestDataActorType{
					"type-B": {
						IdleTimeout: time.Hour,
						ActorIDs: []string{
							"type-B.221",
							"type-B.222",
							"type-B.223",
							"type-B.224",
						},
					},
					"type-C": {
						IdleTimeout: 30 * time.Second,
					},
				},
			},
		},
		Reminders: map[string]TestDataReminder{
			"f647315e-ffeb-4727-8a7a-539bb0d3e3cc": {
				ActorType:     "type-A",
				ActorID:       "type-A.11",
				Name:          "type-A.11.1",
				ExecutionTime: now.Add(1 * time.Second),
			},
			"a51dfaa1-dbac-4140-a505-ba3a972c25b8": {
				ActorType:     "type-A",
				ActorID:       "type-A.11",
				Name:          "type-A.11.2",
				ExecutionTime: now.Add(2 * time.Second),
			},
			"f0093001-649a-4767-b0fa-b26acdc02586": {
				ActorType:     "type-A",
				ActorID:       "type-A.11",
				Name:          "type-A.11.3",
				ExecutionTime: now.Add(5 * time.Minute),
			},
			"76d619d4-ccb1-4069-8c7a-19298330e1ba": {
				ActorType:     "type-C",
				ActorID:       "type-C.12",
				Name:          "type-C.12.1",
				ExecutionTime: now.Add(1 * time.Second),
			},
			"bda35196-d8bd-4426-a0a3-bc6ba6569b59": {
				ActorType:     "type-B",
				ActorID:       "type-B.221",
				Name:          "type-B.221.1",
				ExecutionTime: now.Add(2 * time.Second),
			},
			"9885b201-072b-4a0a-9e2c-25fe76ff6356": {
				ActorType:     "type-A",
				ActorID:       "type-A.inactivereminder",
				Name:          "type-A.inactivereminder.1",
				ExecutionTime: now.Add(2 * time.Second),
			},
			"996a0e70-f9ed-41f5-bcf2-5be53ec1a894": {
				ActorType:     "type-A",
				ActorID:       "type-A.inactivereminder",
				Name:          "type-A.inactivereminder.2",
				ExecutionTime: now.Add(3 * time.Second),
			},
			"2244b360-a448-4273-a2e1-bbc76791ccfa": {
				ActorType:     "type-C",
				ActorID:       "type-C.inactivereminder",
				Name:          "type-C.inactivereminder.1",
				ExecutionTime: now,
			},
		},
	}
}

func GetTestPID() string {
	return testPID
}

func GetActorsConfiguration() actorstore.ActorsConfiguration {
	return actorsConfiguration
}

func GetTestData() TestData {
	return testData
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

func (t TestDataHost) IsActive() bool {
	return time.Since(t.LastHealthCheck) < actorsConfiguration.HostHealthCheckInterval
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

func (t TestData) HostsByActorType() map[string][]string {
	res := make(map[string][]string)
	for hostID, host := range t.Hosts {
		for at := range host.ActorTypes {
			if !host.IsActive() {
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

func (t TestData) HostsForActorType(actorType string) []string {
	res := make([]string, 0)
	for hostID, host := range t.Hosts {
		if !host.IsActive() {
			continue
		}

		_, ok := host.ActorTypes[actorType]
		if ok {
			res = append(res, hostID)
		}
	}
	return res
}
