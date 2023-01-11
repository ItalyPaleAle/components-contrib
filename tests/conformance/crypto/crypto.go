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

package crypto

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	daprcrypto "github.com/dapr/components-contrib/crypto"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/kit/config"
)

const (
	// List of required algorithms for private keys
	algsPrivateRequired = "RSA-OAEP PS256 PS384 PS512 RS256 RS384 RS512"
	// List of required algorithms for symmetric keys
	algsSymmetricRequired = "A256CBC A256GCM A256KW"

	// List of all possible symmetric encryption algorithms
	algsEncryptionSymmetric = "A128CBC A192CBC A256CBC A128GCM A192GCM A256GCM A128CBC-HS256 A192CBC-HS384 A256CBC-HS512 C20P XC20P"
	// List of all possible symmetric key wrapping algorithms
	algsKeywrapSymmetric = "A128KW A192KW A256KW A128GCMKW A192GCMKW A256GCMKW C20PKW XC20PKW"
	// List of all possible asymmetric encryption algorithms
	algsEncryptionAsymmetric = "ECDH-ES ECDH-ES+A128KW ECDH-ES+A192KW ECDH-ES+A256KW RSA1_5 RSA-OAEP RSA-OAEP-256 RSA-OAEP-384 RSA-OAEP-512"
	// List of all possible asymmetric key wrapping algorithms
	algsKeywrapAsymmetric = "ECDH-ES ECDH-ES+A128KW ECDH-ES+A192KW ECDH-ES+A256KW RSA1_5 RSA-OAEP RSA-OAEP-256 RSA-OAEP-384 RSA-OAEP-512"
	// List of all possible asymmetric signing algorithms
	algsSignAsymmetric = "ES256 ES384 ES512 EdDSA PS256 PS384 PS512 RS256 RS384 RS512"
	// List of all possible symmetric signing algorithms
	algsSignSymmetric = "HS256 HS384 HS512"
)

type testConfigKey struct {
	// "public", "private", or "symmetric"
	KeyType string `mapstructure:"type"`
	// Algorithm identifiers constant (e.g. "A256CBC")
	Algorithms []string `mapstructure:"algorithms"`
	// Name of the key
	Name string `mapstructure:"name"`
}

type TestConfig struct {
	utils.CommonConfig

	Keys []testConfigKey `mapstructure:"keys"`
}

func NewTestConfig(name string, allOperations bool, operations []string, configMap map[string]interface{}) (TestConfig, error) {
	testConfig := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "crypto",
			ComponentName: name,
			AllOperations: allOperations,
			Operations:    utils.NewStringSet(operations...),
		},
	}

	err := config.Decode(configMap, &testConfig)
	if err != nil {
		return testConfig, err
	}

	return testConfig, nil
}

func ConformanceTests(t *testing.T, props map[string]string, component daprcrypto.SubtleCrypto, config TestConfig) {
	// Parse all keys and algorithms, then ensure the required ones are present
	keys := newKeybagFromConfig(config)
	for _, alg := range strings.Split(algsPrivateRequired, " ") {
		require.Greaterf(t, len(keys.private[alg]), 0, "could not find a private key for algorithm '%s' in configuration, which is required", alg)
	}
	for _, alg := range strings.Split(algsSymmetricRequired, " ") {
		require.Greaterf(t, len(keys.symmetric[alg]), 0, "could not find a symmetric key for algorithm '%s' in configuration, which is required", alg)
	}
	// Require at least one public key
	found := false
	for _, v := range keys.public {
		found = len(v) > 0
		if found {
			break
		}
	}
	require.True(t, found, "could not find any public key in configuration; at least one is required")

	// Init
	t.Run("Init", func(t *testing.T) {
		err := component.Init(daprcrypto.Metadata{
			Base: metadata.Base{Properties: props},
		})
		require.NoError(t, err, "expected no error on initializing store")
	})

	// Don't run more tests if init failed
	if t.Failed() {
		t.Fatal("Init test failed, stopping further tests")
	}

	t.Run("GetKey method", func(t *testing.T) {
		t.Run("Get public keys", func(t *testing.T) {
			keys.public.testForAllAlgorithms(t, func(algorithm, keyName string) func(t *testing.T) {
				return func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					key, err := component.GetKey(ctx, keyName)
					require.NoError(t, err)
					assert.NotNil(t, key)
					requireKeyPublic(t, key)
				}
			})
		})

		t.Run("Get public part from private keys", func(t *testing.T) {
			keys.private.testForAllAlgorithms(t, func(algorithm, keyName string) func(t *testing.T) {
				return func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					key, err := component.GetKey(ctx, keyName)
					require.NoError(t, err)
					assert.NotNil(t, key)
					requireKeyPublic(t, key)
				}
			})
		})

		t.Run("Cannot get symmetric keys", func(t *testing.T) {
			keys.symmetric.testForAllAlgorithms(t, func(algorithm, keyName string) func(t *testing.T) {
				return func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					key, err := component.GetKey(ctx, keyName)
					require.Error(t, err)
					assert.Nil(t, key)
				}
			})
		})
	})

	t.Run("Symmetric encryption", func(t *testing.T) {
		keys.symmetric.testForAllAlgorithmsInList(t, algsEncryptionSymmetric, func(algorithm, keyName string) func(t *testing.T) {
			return func(t *testing.T) {
				nonce := randomBytes(t, nonceSizeForAlgorithm(algorithm))

				const message = "Quel ramo del lago di Como"

				// Encrypt the message
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				ciphertext, tag, err := component.Encrypt(ctx, []byte(message), algorithm, keyName, nonce, nil)
				require.NoError(t, err)
				assert.NotEmpty(t, ciphertext)
				assert.NotEmpty(t, tag)

				// Decrypt the message
				ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				plaintext, err := component.Decrypt(ctx, ciphertext, algorithm, keyName, nonce, tag, nil)
				require.NoError(t, err)
				assert.Equal(t, message, string(plaintext))

				// Invalid key
				ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_, err = component.Decrypt(ctx, ciphertext, algorithm, "foo", nonce, tag, nil)
				require.Error(t, err)

				// Tag mismatch
				ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				badTag := randomBytes(t, 16)
				_, err = component.Decrypt(ctx, ciphertext, algorithm, keyName, nonce, badTag, nil)
				require.Error(t, err)
			}
		})
	})
}
