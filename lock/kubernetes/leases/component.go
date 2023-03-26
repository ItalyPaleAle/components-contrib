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

package leases

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	coordinationV1 "k8s.io/api/coordination/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	kubeclient "github.com/dapr/components-contrib/internal/authentication/kubernetes"
	"github.com/dapr/components-contrib/lock"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/martinlindhe/base36"
)

const (
	requestTimeout = 30 * time.Second
)

// KubernetesLeaseLock is a lock component that uses Kubernetes leases.
type KubernetesLeaseLock struct {
	md         leasesMetadata
	kubeClient kubernetes.Interface
	logger     logger.Logger
}

// NewKubernetesLeaseLock returns a new KubernetesLeaseLock object.
func NewKubernetesLeaseLock(logger logger.Logger) lock.Store {
	return &KubernetesLeaseLock{
		logger: logger,
	}
}

// Init KubernetesLeaseLock.
func (k *KubernetesLeaseLock) InitLockStore(ctx context.Context, metadata lock.Metadata) error {
	// Init metadata
	err := k.md.InitWithMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	// Init Kubernetes client
	k.kubeClient, err = kubeclient.GetKubeClient()
	if err != nil {
		return fmt.Errorf("failed to init Kubernetes client: %w", err)
	}

	return nil
}

// Try to acquire a redis lock.
func (k *KubernetesLeaseLock) TryLock(parentCtx context.Context, req *lock.TryLockRequest) (*lock.TryLockResponse, error) {
	namespace, lease, err := k.parseResourceID(req.ResourceID)
	if err != nil {
		return &lock.TryLockResponse{}, err
	}

	ctx, cancel := context.WithTimeout(parentCtx, requestTimeout)
	defer cancel()

	leaseObj := &coordinationV1.Lease{
		ObjectMeta: metaV1.ObjectMeta{
			Name: lease,
		},
		Spec: coordinationV1.LeaseSpec{
			HolderIdentity:       &req.LockOwner,
			LeaseDurationSeconds: &req.ExpiryInSeconds,
		},
	}
	leaseObj, err = k.kubeClient.CoordinationV1().
		Leases(namespace).
		Create(ctx, leaseObj, metaV1.CreateOptions{})
	if err != nil {
		// Check if the lease already exists
		return &lock.TryLockResponse{}, fmt.Errorf("failed to create lease: %w", err)
	}
	fmt.Println(leaseObj)

	return &lock.TryLockResponse{
		Success: true,
	}, nil
}

// Try to release a redis lock.
func (r *KubernetesLeaseLock) Unlock(ctx context.Context, req *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	var i int
	var status lock.Status
	if i >= 0 {
		status = lock.Success
	} else if i == -1 {
		status = lock.LockDoesNotExist
	} else if i == -2 {
		status = lock.LockBelongsToOthers
	}
	return &lock.UnlockResponse{
		Status: status,
	}, nil
}

// parseResourceID returns the lease name and optional namespace from the resource ID parameter.
// If the key parameter doesn't contain a namespace, returns the default one.
func (k *KubernetesLeaseLock) parseResourceID(param string) (namespace string, lease string, err error) {
	// Extract the namespace if present
	parts := strings.Split(param, "/")
	switch len(parts) {
	case 2:
		namespace = parts[0]
		lease = parts[1]
	case 1:
		namespace = k.md.DefaultNamespace
		lease = parts[0]
	default:
		err = errors.New("resource ID is not in a valid format: required 'namespace/lease' or 'lease'")
	}

	if namespace == "" {
		err = errors.New("resource ID doesn't have a namespace and the default namespace isn't set")
	}

	// The lease name can contain a prefix and other characters that cannot be used for a Kubernetes resource name
	// So, we need to compute its hash
	h := sha256.Sum224([]byte(lease))
	lease = strings.ToLower(base36.EncodeBytes(h[:]))

	return
}

// GetComponentMetadata returns the metadata of the component.
func (r *KubernetesLeaseLock) GetComponentMetadata() map[string]string {
	metadataStruct := leasesMetadata{}
	metadataInfo := map[string]string{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}
