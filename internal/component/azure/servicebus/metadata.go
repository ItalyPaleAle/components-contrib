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
	"errors"
	"fmt"
	"strconv"

	"github.com/dapr/kit/logger"
)

// Metadata keys for Service Bus topics and queues
// Additional metadata keys for Azure AD are handled separately
type Metadata struct {
	/* Common metadata properties */
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

	/* For topic subscriptions only */
	TopicName  string // Used by bindings only
	ConsumerID string // = subscription name

	/* For queues only */
	QueueName string // Used by bindings only
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
	keyDefaultMessageTimeToLiveInSec_alias    = "ttlInSeconds"
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

// ParseMetadata returns a Metadata object from the map
func ParseMetadata(meta map[string]string, logger logger.Logger, logPrefix string) (Metadata, error) {
	m := Metadata{}

	/* Required configuration settings - no defaults. */
	if val, ok := meta[keyConnectionString]; ok && val != "" {
		m.ConnectionString = val

		// The connection string and the namespace cannot both be present.
		if namespace, present := meta[keyNamespaceName]; present && namespace != "" {
			return m, fmt.Errorf("%s connectionString and namespaceName cannot both be specified", logPrefix)
		}
	} else if val, ok := meta[keyNamespaceName]; ok && val != "" {
		m.NamespaceName = val
	} else {
		return m, fmt.Errorf("%s missing connection string and namespace name", logPrefix)
	}

	if val, ok := meta[keyConsumerID]; ok && val != "" {
		m.ConsumerID = val
	} else {
		return m, fmt.Errorf("%s missing consumerID", logPrefix)
	}

	/* Optional configuration settings - defaults will be set by the client. */
	m.TimeoutInSec = defaultTimeoutInSec
	if val, ok := meta[keyTimeoutInSec]; ok && val != "" {
		var err error
		m.TimeoutInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid timeoutInSec %s, %s", logPrefix, val, err)
		}
	}

	m.DisableEntityManagement = defaultDisableEntityManagement
	if val, ok := meta[keyDisableEntityManagement]; ok && val != "" {
		var err error
		m.DisableEntityManagement, err = strconv.ParseBool(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid disableEntityManagement %s, %s", logPrefix, val, err)
		}
	}

	m.HandlerTimeoutInSec = defaultHandlerTimeoutInSec
	if val, ok := meta[keyHandlerTimeoutInSec]; ok && val != "" {
		var err error
		m.HandlerTimeoutInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid handlerTimeoutInSec %s, %s", logPrefix, val, err)
		}
	}

	m.LockRenewalInSec = defaultLockRenewalInSec
	if val, ok := meta[keyLockRenewalInSec]; ok && val != "" {
		var err error
		m.LockRenewalInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid lockRenewalInSec %s, %s", logPrefix, val, err)
		}
	}

	m.MaxActiveMessages = defaultMaxActiveMessages
	if val, ok := meta[keyMaxActiveMessages]; ok && val != "" {
		var err error
		m.MaxActiveMessages, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxActiveMessages %s, %s", logPrefix, val, err)
		}
	}

	m.MaxRetriableErrorsPerSec = defaultMaxRetriableErrorsPerSec
	if val, ok := meta[keyMaxRetriableErrorsPerSec]; ok && val != "" {
		var err error
		m.MaxRetriableErrorsPerSec, err = strconv.Atoi(val)
		if err == nil && m.MaxRetriableErrorsPerSec < 0 {
			err = errors.New("must not be negative")
		}
		if err != nil {
			return m, fmt.Errorf("%s invalid maxRetriableErrorsPerSec %s, %s", logPrefix, val, err)
		}
	}

	m.MinConnectionRecoveryInSec = defaultMinConnectionRecoveryInSec
	if val, ok := meta[keyMinConnectionRecoveryInSec]; ok && val != "" {
		var err error
		m.MinConnectionRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid minConnectionRecoveryInSec %s, %s", logPrefix, val, err)
		}
	}

	m.MaxConnectionRecoveryInSec = defaultMaxConnectionRecoveryInSec
	if val, ok := meta[keyMaxConnectionRecoveryInSec]; ok && val != "" {
		var err error
		m.MaxConnectionRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxConnectionRecoveryInSec %s, %s", logPrefix, val, err)
		}
	}

	/* Nullable configuration settings - defaults will be set by the server. */
	if val, ok := meta[keyMaxDeliveryCount]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxDeliveryCount %s, %s", logPrefix, val, err)
		}
		m.MaxDeliveryCount = &valAsInt
	}

	if val, ok := meta[keyLockDurationInSec]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid lockDurationInSec %s, %s", logPrefix, val, err)
		}
		m.LockDurationInSec = &valAsInt
	}

	if val, ok := meta[keyDefaultMessageTimeToLiveInSec]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid defaultMessageTimeToLiveInSec %s, %s", logPrefix, val, err)
		}
		m.DefaultMessageTimeToLiveInSec = &valAsInt
	}

	if val, ok := meta[keyAutoDeleteOnIdleInSec]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid autoDeleteOnIdleInSecKey %s, %s", logPrefix, val, err)
		}
		m.AutoDeleteOnIdleInSec = &valAsInt
	}

	if val, ok := meta[keyMaxConcurrentHandlers]; ok && val != "" {
		var err error
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxConcurrentHandlers %s, %s", logPrefix, val, err)
		}
		m.MaxConcurrentHandlers = &valAsInt
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

	return m, nil
}
