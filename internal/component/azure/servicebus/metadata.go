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

package servicebus

import (
	"fmt"

	"github.com/dapr/components-contrib/internal/utils"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// SharedMetadata contains shared for Service Bus topics and queues.
// Note: Additional metadata keys for Azure AD auth are handled separately.
type SharedMetadata struct {
	ConnectionString           string
	NamespaceName              string
	TimeoutInSec               int
	HandlerTimeoutInSec        int
	LockRenewalInSec           int
	MaxActiveMessages          int
	MinConnectionRecoveryInSec int
	MaxConnectionRecoveryInSec int
	MaxRetriableErrorsPerSec   int
	MaxConcurrentHandlers      *int

	/* For topic/queue creation only */

	DisableEntityManagement                bool
	LockDurationInSec                      *int
	EnableDeadLetteringOnMessageExpiration *bool
	DefaultMessageTimeToLiveInSec          *int // Alias: "ttlInSeconds"
	MaxDeliveryCount                       *int
	AutoDeleteOnIdleInSec                  *int
}

// TopicMetadata contains metadata specific to Service Bus topic subscriptions.
type TopicMetadata struct {
	TopicName  string
	ConsumerID string // = subscription name
}

// QueueMetadata contains metadata specific to Service Bus queues.
type QueueMetadata struct {
	QueueName string
}

const (
	/* Keys. */

	keyConnectionString           = "connectionString"
	keyNamespaceName              = "namespaceName"
	keyTimeoutInSec               = "timeoutInSec"
	keyHandlerTimeoutInSec        = "handlerTimeoutInSec"
	keyLockRenewalInSec           = "lockRenewalInSec"
	keyMaxActiveMessages          = "maxActiveMessages"
	keyMinConnectionRecoveryInSec = "minConnectionRecoveryInSec"
	keyMaxConnectionRecoveryInSec = "maxConnectionRecoveryInSec"
	keyMaxRetriableErrorsPerSec   = "maxRetriableErrorsPerSec"
	keyMaxConcurrentHandlers      = "maxConcurrentHandlers"

	keyDisableEntityManagement                = "disableEntityManagement"
	keyLockDurationInSec                      = "lockDurationInSec"
	keyDefaultMessageTimeToLiveInSec          = "defaultMessageTimeToLiveInSec"
	keyDefaultMessageTimeToLiveInSec_alias    = "ttlInSeconds" // This key must be equal to "github.com/dapr/components-contrib/metadata".TTLMetadataKey
	keyEnableDeadLetteringOnMessageExpiration = "enableDeadLetteringOnMessageExpiration"
	keyMaxDeliveryCount                       = "maxDeliveryCount"
	keyAutoDeleteOnIdleInSec                  = "autoDeleteOnIdleInSec"

	keyTopicName  = "topicName"
	keyConsumerID = "consumerID"

	keyQueueName = "queueName"

	/* Deprecated keys. */

	keyMaxReconnectionAttempts         = "maxReconnectionAttempts"
	keyConnectionRecoveryInSec         = "connectionRecoveryInSec"
	keyPublishMaxRetries               = "publishMaxRetries"
	keyPublishInitialRetryIntervalInMs = "publishInitialRetryInternalInMs" // The typo ("internal") was already there

	/* Defaults. */

	// Timeout for network operations
	defaultTimeoutInSec = 60
	// After this interval, the handler's context is canceled (but the goroutine is allowed to continue running)
	defaultHandlerTimeoutInSec = 60
	defaultLockRenewalInSec    = 20
	// Max active messages should be >= max concurrent handlers
	// TODO: ASB Messages can be up to 256Kb. 10000 messages at this size would roughly use 2.56Gb.
	// We should change this if performance testing suggests a more sensible default.
	defaultMaxActiveMessages          = 10000
	defaultMinConnectionRecoveryInSec = 2
	defaultMaxConnectionRecoveryInSec = 300
	defaultMaxRetriableErrorsPerSec   = 10

	defaultDisableEntityManagement = false
)

// ParseMap populates the metadata with values from the map.
func (m *SharedMetadata) ParseMap(meta map[string]string, logger logger.Logger, logPrefix string) (err error) {
	var (
		intVal int
		ok     bool
	)

	/* Required configuration settings - no defaults. */

	if val, ok := meta[keyConnectionString]; ok && val != "" {
		m.ConnectionString = val

		// The connection string and the namespace cannot both be present.
		if namespace, present := meta[keyNamespaceName]; present && namespace != "" {
			return fmt.Errorf("%s properties 'connectionString' and 'namespaceName' cannot both be specified", logPrefix)
		}
	} else if val, ok := meta[keyNamespaceName]; ok && val != "" {
		m.NamespaceName = val
	} else {
		return fmt.Errorf("%s either one of 'connectionString' and 'namespaceName' must be specified", logPrefix)
	}

	/* Optional configuration settings - defaults will be set by the client. */

	m.TimeoutInSec = defaultTimeoutInSec
	intVal, ok, err = contrib_metadata.GetIntMetadataProperty(meta, keyTimeoutInSec)
	if err != nil {
		return fmt.Errorf("%s %v", logPrefix, err)
	} else if ok {
		m.TimeoutInSec = intVal
	}

	m.DisableEntityManagement = defaultDisableEntityManagement
	if val, ok := meta[keyDisableEntityManagement]; ok && val != "" {
		m.DisableEntityManagement = utils.IsTruthy(val)
	}

	m.HandlerTimeoutInSec = defaultHandlerTimeoutInSec
	intVal, ok, err = contrib_metadata.GetIntMetadataProperty(meta, keyHandlerTimeoutInSec)
	if err != nil {
		return fmt.Errorf("%s %v", logPrefix, err)
	} else if ok {
		m.HandlerTimeoutInSec = intVal
	}

	m.LockRenewalInSec = defaultLockRenewalInSec
	intVal, ok, err = contrib_metadata.GetIntMetadataProperty(meta, keyLockRenewalInSec)
	if err != nil {
		return fmt.Errorf("%s %v", logPrefix, err)
	} else if ok {
		m.LockRenewalInSec = intVal
	}

	m.MaxActiveMessages = defaultMaxActiveMessages
	intVal, ok, err = contrib_metadata.GetIntMetadataProperty(meta, keyMaxActiveMessages)
	if err != nil {
		return fmt.Errorf("%s %v", logPrefix, err)
	} else if ok {
		m.MaxActiveMessages = intVal
	}

	m.MaxRetriableErrorsPerSec = defaultMaxRetriableErrorsPerSec
	intVal, ok, err = contrib_metadata.GetIntMetadataProperty(meta, keyMaxRetriableErrorsPerSec)
	if err != nil {
		return fmt.Errorf("%s %v", logPrefix, err)
	} else if ok {
		m.MaxRetriableErrorsPerSec = intVal
	}

	m.MinConnectionRecoveryInSec = defaultMinConnectionRecoveryInSec
	intVal, ok, err = contrib_metadata.GetIntMetadataProperty(meta, keyMinConnectionRecoveryInSec)
	if err != nil {
		return fmt.Errorf("%s %v", logPrefix, err)
	} else if ok {
		m.MinConnectionRecoveryInSec = intVal
	}

	m.MaxConnectionRecoveryInSec = defaultMaxConnectionRecoveryInSec
	intVal, ok, err = contrib_metadata.GetIntMetadataProperty(meta, keyMaxConnectionRecoveryInSec)
	if err != nil {
		return fmt.Errorf("%s %v", logPrefix, err)
	} else if ok {
		m.MaxConnectionRecoveryInSec = intVal
	}

	/* Nullable configuration settings - defaults will be set by the server. */

	intVal, ok, err = contrib_metadata.GetIntMetadataProperty(meta, keyMaxDeliveryCount)
	if err != nil {
		return fmt.Errorf("%s %v", logPrefix, err)
	} else if ok {
		m.MaxDeliveryCount = &intVal
	}

	intVal, ok, err = contrib_metadata.GetIntMetadataProperty(meta, keyLockDurationInSec)
	if err != nil {
		return fmt.Errorf("%s %v", logPrefix, err)
	} else if ok {
		m.LockDurationInSec = &intVal
	}

	intVal, ok, err = contrib_metadata.GetIntMetadataProperty(meta, keyDefaultMessageTimeToLiveInSec, keyDefaultMessageTimeToLiveInSec_alias)
	if err != nil {
		return fmt.Errorf("%s %v", logPrefix, err)
	} else if ok {
		m.DefaultMessageTimeToLiveInSec = &intVal
	}

	intVal, ok, err = contrib_metadata.GetIntMetadataProperty(meta, keyAutoDeleteOnIdleInSec)
	if err != nil {
		return fmt.Errorf("%s %v", logPrefix, err)
	} else if ok {
		m.AutoDeleteOnIdleInSec = &intVal
	}

	intVal, ok, err = contrib_metadata.GetIntMetadataProperty(meta, keyMaxConcurrentHandlers)
	if err != nil {
		return fmt.Errorf("%s %v", logPrefix, err)
	} else if ok {
		m.MaxConcurrentHandlers = &intVal
	}

	/* Deprecated properties that were used by pubsub only - show a warning. */

	// TODO: Deprecated in 1.8. Remove in the future
	if _, ok := meta[keyConnectionRecoveryInSec]; ok && logger != nil {
		logger.Warnf("%s: metadata property 'connectionRecoveryInSec' has been deprecated and is now ignored - use 'minConnectionRecoveryInSec' and 'maxConnectionRecoveryInSec' instead. See: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-azure-servicebus/", logPrefix)
	}
	// TODO: Deprecated in 1.8. Remove in the future
	if _, ok := meta[keyMaxReconnectionAttempts]; ok && logger != nil {
		logger.Warnf("%s: metadata property 'maxReconnectionAttempts' has been deprecated and is now ignored. See: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-azure-servicebus/", logPrefix)
	}
	// TODO: Deprecated in 1.9. Remove in the future
	if _, ok := meta[keyPublishMaxRetries]; ok && logger != nil {
		logger.Warnf("%s: metadata property 'publishMaxRetries' has been deprecated and is now ignored. See: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-azure-servicebus/", logPrefix)
	}
	// TODO: Deprecated in 1.9. Remove in the future
	if _, ok := meta[keyPublishInitialRetryIntervalInMs]; ok && logger != nil {
		logger.Warnf("%s: metadata property 'publishInitialRetryInternalInMs' has been deprecated and is now ignored. See: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-azure-servicebus/", logPrefix)
	}

	return nil
}

// ParseMap populates the metadata with values from the map.
func (m *TopicMetadata) ParseMap(meta map[string]string, logger logger.Logger, logPrefix string) error {
	/* Required configuration settings - no defaults. */

	if val, ok := meta[keyConsumerID]; ok && val != "" {
		m.ConsumerID = val
	} else {
		return fmt.Errorf("%s missing consumerID", logPrefix)
	}

	return nil
}
