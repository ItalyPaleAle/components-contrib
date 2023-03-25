package leases

import (
	contribLock "github.com/dapr/components-contrib/lock"
	"github.com/dapr/components-contrib/metadata"
)

type leasesMetadata struct {
	// Default namespace to store the lease in.
	// If unset, the namespace must be specified for each resourceID, as `namespace/lease`
	DefaultNamespace string `json:"defaultNamespace" mapstructure:"defaultNamespace"`
}

func (m *leasesMetadata) InitWithMetadata(meta contribLock.Metadata) error {
	m.reset()

	// Decode the metadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	return nil
}

// Reset the object
func (m *leasesMetadata) reset() {
	m.DefaultNamespace = ""
}
