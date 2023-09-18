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
	"io"
	"time"

	"github.com/dapr/components-contrib/health"
	"github.com/dapr/components-contrib/metadata"
)

var (
	// ErrActorHostNotFound is returned by RemoveActorHost when the host doesn't exist.
	ErrActorHostNotFound = errors.New("actor host not found")

	// ErrNoActorHost is returned by LookupActor when there's no suitable host for actors of the given type.
	ErrNoActorHost = errors.New("could not find a suitable host for actor of the given type")

	// ErrActorNotFound is returned by RemoveActor when the actor doesn't exist.
	ErrActorNotFound = errors.New("actor not found")
)

// Store is the interface for the actor store.
type Store interface {
	io.Closer
	health.Pinger

	// Init the actor store.
	Init(ctx context.Context, md Metadata) error

	// AddActorHost adds a new actor host.
	// Returns the ID of the actor host.
	AddActorHost(ctx context.Context, properties AddActorHostRequest) (string, error)

	// UpdateActorHost updates an actor host.
	UpdateActorHost(ctx context.Context, actorHostID string, properties UpdateActorHostRequest) error

	// RemoveActorHost deletes an actor host.
	// If the host doesn't exist, returns ErrActorHostNotFound.
	RemoveActorHost(ctx context.Context, actorHostID string) error

	// LookupActor returns the address of the actor host for a given actor type and ID.
	// If the actor is not currently active on any host, it's created in the database and assigned to a random host.
	// If it's not possible to find an instance capable of hosting the given actor, ErrNoActorHost is returned instead.
	LookupActor(ctx context.Context, ref ActorRef) (LookupActorResponse, error)

	// RemoveActor removes an actor from the list of active actors.
	// If the actor doesn't exist, returns ErrActorNotFound.
	RemoveActor(ctx context.Context, ref ActorRef) error
}

// Metadata contains a specific set of metadata properties for actorstate components.
type Metadata struct {
	metadata.Base `json:",inline"`
}

// AddActorHostRequest is the request object for the AddActorHost method.
type AddActorHostRequest struct {
	// Dapr App ID of the host
	// Format is 'namespace/app-id'
	AppID string
	// Host address (including port)
	Address string
	// List of supported actor types
	ActorTypes []ActorHostType
	// Version of the Actor APIs supported by the Dapr runtime
	ApiLevel uint32
}

// UpdateActorHostRequest is the request object for the UpdateActorHost method.
type UpdateActorHostRequest struct {
	// Last healthcheck time
	// If non-nil, will update the value in the database
	LastHealthCheck *time.Time

	// List of supported actor types
	// If non-nil, will replace all existing, registered actor types
	ActorTypes []ActorHostType
}

// ActorHostType references a supported actor type.
type ActorHostType struct {
	// Actor type name
	ActorType string
	// Actor idle timeout, in seconds
	IdleTimeout uint32
}

// ActorRef is the reference to an actor (type and ID).
type ActorRef struct {
	ActorType string
	ActorID   string
}

// LookupActorResponse is the response object for the LookupActor method.
type LookupActorResponse struct {
	// Dapr App ID of the host
	AppID string
	// Host address (including port)
	Address string
	// Actor idle timeout, in seconds
	// (Note that this is the absolute idle timeout, and not the remaining lifetime of the actor)
	IdleTimeout uint32
}
