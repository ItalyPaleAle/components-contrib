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
)

var (
	// ErrActorHostConflict is returned by AddActorHost when a host is already registered at the same address.
	ErrActorHostConflict = errors.New("an actor host is already registered at the same address")

	// ErrActorHostNotFound is returned by RemoveActorHost and UpdateActorHost when the host doesn't exist.
	ErrActorHostNotFound = errors.New("actor host not found")

	// ErrNoActorHost is returned by LookupActor when there's no suitable host for actors of the given type.
	ErrNoActorHost = errors.New("could not find a suitable host for actors of the given type")

	// ErrActorNotFound is returned by RemoveActor when the actor doesn't exist.
	ErrActorNotFound = errors.New("actor not found")

	// ErrInvalidRequestMissingParameters is returned by various methods when the request is missing required parameters.
	ErrInvalidRequestMissingParameters = errors.New("invalid request: missing required parameters")
)

// StoreActorState is the part of the Store interface for managing actor state.
type StoreActorState interface {
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
	LookupActor(ctx context.Context, ref ActorRef, opts LookupActorOpts) (LookupActorResponse, error)

	// RemoveActor removes an actor from the list of active actors.
	// If the actor doesn't exist, returns ErrActorNotFound.
	RemoveActor(ctx context.Context, ref ActorRef) error
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
	APILevel uint32
}

// UpdateActorHostRequest is the request object for the UpdateActorHost method.
type UpdateActorHostRequest struct {
	// Updates last healthcheck time
	// If true, will update the value in the database with the current time (using the server's clock)
	UpdateLastHealthCheck bool

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

// LookupActorOpts contains options for LookupActor.
type LookupActorOpts struct {
	// List of hosts on which the actor can be activated.
	// If the actor is active on a different host, ErrNoActorHost is returned.
	Hosts []string
}

// LookupActorResponse is the response object for the LookupActor method.
type LookupActorResponse struct {
	// Host ID
	HostID string
	// Dapr App ID of the host
	AppID string
	// Host address (including port)
	Address string
	// Actor idle timeout, in seconds
	// (Note that this is the absolute idle timeout, and not the remaining lifetime of the actor)
	IdleTimeout uint32
}
