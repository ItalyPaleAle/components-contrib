/*
Copyright 2021 The Dapr Authors
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

package state

import (
	"context"
	"errors"

	"github.com/dapr/components-contrib/health"
	"github.com/dapr/components-contrib/metadata"
)

// Store is an interface to perform operations on store.
type Store interface {
	metadata.ComponentWithMetadata

	BaseStore
	BulkStore
}

// BaseStore is an interface that contains the base methods for each state store.
type BaseStore interface {
	Init(ctx context.Context, metadata Metadata) error
	Features() []Feature
	Delete(ctx context.Context, req *DeleteRequest) error
	Get(ctx context.Context, req *GetRequest) (*GetResponse, error)
	Set(ctx context.Context, req *SetRequest) error
}

// TransactionalStore is an interface for initialization and support multiple transactional requests.
type TransactionalStore interface {
	Multi(ctx context.Context, request *TransactionalStateRequest) error
}

// WorkflowStateStore is an interface for state stores that support methods used by workflows to store their state.
type WorkflowStateStore interface {
	// GetWorkflowState returns the state of a workflow.
	GetWorkflowState(ctx context.Context, req GetWorkflowStateRequest) (*GetWorkflowStateResponse, error)
	// SetWorkflowState sets or updates the state of a workflow.
	SetWorkflowState(ctx context.Context, req SetWorkflowStateRequest) error
	// DeleteWorkflowState deletes the state of a workflow.
	DeleteWorkflowState(ctx context.Context, req DeleteWorkflowStateRequest) error
}

// TransactionalStoreMultiMaxSize is an optional interface transactional state stores can implement to indicate the maximum size for a transaction.
type TransactionalStoreMultiMaxSize interface {
	MultiMaxSize() int
}

// Querier is an interface to execute queries.
type Querier interface {
	Query(ctx context.Context, req *QueryRequest) (*QueryResponse, error)
}

func Ping(ctx context.Context, store Store) error {
	// checks if this store has the ping option then executes
	if storeWithPing, ok := store.(health.Pinger); ok {
		return storeWithPing.Ping(ctx)
	} else {
		return errors.New("ping is not implemented by this state store")
	}
}
