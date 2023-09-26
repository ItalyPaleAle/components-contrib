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
	"fmt"
	"io"
	"time"

	"github.com/dapr/components-contrib/health"
	"github.com/dapr/components-contrib/metadata"
)

// Store is the interface for the actor store.
type Store interface {
	io.Closer
	health.Pinger

	StoreActorState

	// Init the actor store.
	Init(ctx context.Context, md Metadata) error
}

// Metadata contains a specific set of metadata properties for actorstate components.
type Metadata struct {
	metadata.Base `json:",inline"`

	Configuration ActorsConfiguration
}

// ActorsConfiguration contains the configuration for the actor subsystem.
type ActorsConfiguration struct {
	// Maximum interval between pings received from an actor host.
	HostHealthCheckInterval time.Duration
}

// String implements fmt.Stringer and is used for debugging.
func (c ActorsConfiguration) String() string {
	return fmt.Sprintf(
		"host-healthcheck-interval='%v'",
		c.HostHealthCheckInterval,
	)
}

// FailedInterval returns the interval that can be used to detect if an actor host is unhealthy.
func (c ActorsConfiguration) FailedInterval() time.Duration {
	return c.HostHealthCheckInterval
}
