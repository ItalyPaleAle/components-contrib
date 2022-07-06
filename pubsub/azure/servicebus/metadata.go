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

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

// Reference for settings:
// https://github.com/Azure/azure-service-bus-go/blob/54b2faa53e5216616e59725281be692acc120c34/subscription_manager.go#L101
type metadata struct {
	ConnectionString              string `json:"connectionString"`
	ConsumerID                    string `json:"consumerID"`
	TimeoutInSec                  int    `json:"timeoutInSec"`
	HandlerTimeoutInSec           int    `json:"handlerTimeoutInSec"`
	LockRenewalInSec              int    `json:"lockRenewalInSec"`
	MaxActiveMessages             int    `json:"maxActiveMessages"`
	MaxConnectionRecoveryInSec    int    `json:"maxConnectionRecoveryInSec"`
	MinConnectionRecoveryInSec    int    `json:"minConnectionRecoveryInSec"`
	DisableEntityManagement       bool   `json:"disableEntityManagement"`
	MaxRetriableErrorsPerSec      int    `json:"maxRetriableErrorsPerSec"`
	MaxDeliveryCount              *int   `json:"maxDeliveryCount"`
	LockDurationInSec             *int   `json:"lockDurationInSec"`
	DefaultMessageTimeToLiveInSec *int   `json:"defaultMessageTimeToLiveInSec"`
	AutoDeleteOnIdleInSec         *int   `json:"autoDeleteOnIdleInSec"`
	MaxConcurrentHandlers         *int   `json:"maxConcurrentHandlers"`
	NamespaceName                 string `json:"namespaceName,omitempty"`
}

const (
	// Keys.
	keyConnectionString              = "connectionString"
	keyConsumerID                    = "consumerID"
	keyTimeoutInSec                  = "timeoutInSec"
	keyHandlerTimeoutInSec           = "handlerTimeoutInSec"
	keyLockRenewalInSec              = "lockRenewalInSec"
	keyMaxActiveMessages             = "maxActiveMessages"
	keyMaxConnectionRecoveryInSec    = "maxConnectionRecoveryInSec"
	keyMinConnectionRecoveryInSec    = "minConnectionRecoveryInSec"
	keyDisableEntityManagement       = "disableEntityManagement"
	keyMaxRetriableErrorsPerSec      = "maxRetriableErrorsPerSec"
	keyMaxDeliveryCount              = "maxDeliveryCount"
	keyLockDurationInSec             = "lockDurationInSec"
	keyDefaultMessageTimeToLiveInSec = "defaultMessageTimeToLiveInSec"
	keyAutoDeleteOnIdleInSec         = "autoDeleteOnIdleInSec"
	keyMaxConcurrentHandlers         = "maxConcurrentHandlers"
	keyNamespaceName                 = "namespaceName"

	// Deprecated keys.
	keyMaxReconnectionAttempts         = "maxReconnectionAttempts"
	keyConnectionRecoveryInSec         = "connectionRecoveryInSec"
	keyPublishMaxRetries               = "publishMaxRetries"
	keyPublishInitialRetryIntervalInMs = "publishInitialRetryInternalInMs" // The typo ("internal") was already there

	// Defaults.
	defaultTimeoutInSec             = 60
	defaultHandlerTimeoutInSec      = 60
	defaultLockRenewalInSec         = 20
	defaultMaxRetriableErrorsPerSec = 10
	// ASB Messages can be up to 256Kb. 10000 messages at this size would roughly use 2.56Gb.
	// We should change this if performance testing suggests a more sensible default.
	defaultMaxActiveMessages          = 10000
	defaultDisableEntityManagement    = false
	defaultMinConnectionRecoveryInSec = 2
	defaultMaxConnectionRecoveryInSec = 300
	defaultMaxRetries                 = 5
	defaultMinRetryDelayInMs          = 3000
	defaultMaxRetryDelayInMs          = 120000
)

func parseAzureServiceBusMetadata(meta pubsub.Metadata, logger logger.Logger) (metadata, error) {
	m := metadata{}

	/* Required configuration settings - no defaults. */
	if val, ok := meta.Properties[keyConnectionString]; ok && val != "" {
		m.ConnectionString = val

		// The connection string and the namespace cannot both be present.
		if namespace, present := meta.Properties[keyNamespaceName]; present && namespace != "" {
			return m, fmt.Errorf("%s connectionString and namespaceName cannot both be specified", errorMessagePrefix)
		}
	} else if val, ok := meta.Properties[keyNamespaceName]; ok && val != "" {
		m.NamespaceName = val
	} else {
		return m, fmt.Errorf("%s missing connection string and namespace name", errorMessagePrefix)
	}

	if val, ok := meta.Properties[keyConsumerID]; ok && val != "" {
		m.ConsumerID = val
	} else {
		return m, fmt.Errorf("%s missing consumerID", errorMessagePrefix)
	}

	/* Optional configuration settings - defaults will be set by the client. */
	m.TimeoutInSec = defaultTimeoutInSec
	if val, ok := meta.Properties[keyTimeoutInSec]; ok && val != "" {
		var err error
		m.TimeoutInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid timeoutInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.DisableEntityManagement = defaultDisableEntityManagement
	if val, ok := meta.Properties[keyDisableEntityManagement]; ok && val != "" {
		var err error
		m.DisableEntityManagement, err = strconv.ParseBool(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid disableEntityManagement %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.HandlerTimeoutInSec = defaultHandlerTimeoutInSec
	if val, ok := meta.Properties[keyHandlerTimeoutInSec]; ok && val != "" {
		var err error
		m.HandlerTimeoutInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid handlerTimeoutInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.LockRenewalInSec = defaultLockRenewalInSec
	if val, ok := meta.Properties[keyLockRenewalInSec]; ok && val != "" {
		var err error
		m.LockRenewalInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid lockRenewalInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MaxActiveMessages = defaultMaxActiveMessages
	if val, ok := meta.Properties[keyMaxActiveMessages]; ok && val != "" {
		var err error
		m.MaxActiveMessages, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxActiveMessages %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MaxRetriableErrorsPerSec = defaultMaxRetriableErrorsPerSec
	if val, ok := meta.Properties[keyMaxRetriableErrorsPerSec]; ok && val != "" {
		var err error
		m.MaxRetriableErrorsPerSec, err = strconv.Atoi(val)
		if err == nil && m.MaxRetriableErrorsPerSec < 0 {
			err = errors.New("must not be negative")
		}
		if err != nil {
			return m, fmt.Errorf("%s invalid maxRetriableErrorsPerSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MinConnectionRecoveryInSec = defaultMinConnectionRecoveryInSec
	if val, ok := meta.Properties[keyMinConnectionRecoveryInSec]; ok && val != "" {
		var err error
		m.MinConnectionRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid minConnectionRecoveryInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MaxConnectionRecoveryInSec = defaultMaxConnectionRecoveryInSec
	if val, ok := meta.Properties[keyMaxConnectionRecoveryInSec]; ok && val != "" {
		var err error
		m.MaxConnectionRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxConnectionRecoveryInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	/* Nullable configuration settings - defaults will be set by the server. */
	if val, ok := meta.Properties[keyMaxDeliveryCount]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxDeliveryCount %s, %s", errorMessagePrefix, val, err)
		}
		m.MaxDeliveryCount = &valAsInt
	}

	if val, ok := meta.Properties[keyLockDurationInSec]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid lockDurationInSec %s, %s", errorMessagePrefix, val, err)
		}
		m.LockDurationInSec = &valAsInt
	}

	if val, ok := meta.Properties[keyDefaultMessageTimeToLiveInSec]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid defaultMessageTimeToLiveInSec %s, %s", errorMessagePrefix, val, err)
		}
		m.DefaultMessageTimeToLiveInSec = &valAsInt
	}

	if val, ok := meta.Properties[keyAutoDeleteOnIdleInSec]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid autoDeleteOnIdleInSecKey %s, %s", errorMessagePrefix, val, err)
		}
		m.AutoDeleteOnIdleInSec = &valAsInt
	}

	if val, ok := meta.Properties[keyMaxConcurrentHandlers]; ok && val != "" {
		var err error
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxConcurrentHandlers %s, %s", errorMessagePrefix, val, err)
		}
		m.MaxConcurrentHandlers = &valAsInt
	}

	/* Deprecated properties - show a warning. */

	// TODO: Deprecated in 1.8. Remove in the future
	if _, ok := meta.Properties[keyConnectionRecoveryInSec]; ok && logger != nil {
		logger.Warn("pubsub.azure.servicebus: metadata property 'connectionRecoveryInSec' has been deprecated and is now ignored - use 'minConnectionRecoveryInSec' and 'maxConnectionRecoveryInSec' instead. See: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-azure-servicebus/")
	}
	// TODO: Deprecated in 1.8. Remove in the future
	if _, ok := meta.Properties[keyMaxReconnectionAttempts]; ok && logger != nil {
		logger.Warn("pubsub.azure.servicebus: metadata property 'maxReconnectionAttempts' has been deprecated and is now ignored. See: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-azure-servicebus/")
	}
	// TODO: Deprecated in 1.9. Remove in the future
	if _, ok := meta.Properties[keyPublishMaxRetries]; ok && logger != nil {
		logger.Warn("pubsub.azure.servicebus: metadata property 'publishMaxRetries' has been deprecated and is now ignored. See: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-azure-servicebus/")
	}
	// TODO: Deprecated in 1.9. Remove in the future
	if _, ok := meta.Properties[keyPublishInitialRetryIntervalInMs]; ok && logger != nil {
		logger.Warn("pubsub.azure.servicebus: metadata property 'publishInitialRetryInternalInMs' has been deprecated and is now ignored. See: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-azure-servicebus/")
	}

	return m, nil
}
