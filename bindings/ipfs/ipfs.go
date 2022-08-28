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

package ipfs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	ipfsHttpclient "github.com/ipfs/go-ipfs-http-client"
	ipfsIcore "github.com/ipfs/interface-go-ipfs-core"
	"github.com/multiformats/go-multiaddr"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

var httpClient *http.Client

func init() {
	httpClient = &http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 5 * time.Second,
			}).Dial,
		},
	}
}

// IPFSBinding is a binding for interacting with an IPFS network.
type IPFSBinding struct {
	metadata ipfsMetadata
	ipfsAPI  ipfsIcore.CoreAPI
	ctx      context.Context
	cancel   context.CancelFunc
	logger   logger.Logger
}

// NewIPFSBinding returns a new IPFSBinding.
func NewIPFSBinding(logger logger.Logger) bindings.OutputBinding {
	return &IPFSBinding{
		logger: logger,
	}
}

// Init the binding.
func (b *IPFSBinding) Init(metadata bindings.Metadata) (err error) {
	b.ctx, b.cancel = context.WithCancel(context.Background())

	err = b.metadata.FromMap(metadata.Properties)
	if err != nil {
		return err
	}

	if b.metadata.NodeAddress == "" {
		return errors.New("metadata option 'nodeAddress' is required")
	}

	if b.metadata.NodeAddress[0] == '/' {
		var maddr multiaddr.Multiaddr
		maddr, err = multiaddr.NewMultiaddr(b.metadata.NodeAddress)
		if err != nil {
			return fmt.Errorf("failed to parse external API multiaddr: %v", err)
		}
		b.ipfsAPI, err = ipfsHttpclient.NewApiWithClient(maddr, httpClient)
	} else {
		b.ipfsAPI, err = ipfsHttpclient.NewURLApiWithClient(b.metadata.NodeAddress, httpClient)
	}
	if err != nil {
		return fmt.Errorf("failed to initialize external IPFS API: %v", err)
	}
	b.logger.Infof("Using IPFS APIs at %s", b.metadata.NodeAddress)

	return nil
}

func (b *IPFSBinding) Close() (err error) {
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}

	return nil
}

// Operations returns the supported operations for this binding.
func (b *IPFSBinding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.GetOperation,
		bindings.CreateOperation, // alias for "add"
		bindings.ListOperation,   // alias for "ls"
		bindings.DeleteOperation, // alias for "pin-rm"
		"add",
		"ls",
		"pin-add",
		"pin-rm",
		"pin-ls",
	}
}

// Invoke performs an HTTP request to the configured HTTP endpoint.
func (b *IPFSBinding) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.GetOperation:
		return b.getOperation(ctx, req)
	case "add", bindings.CreateOperation:
		return b.addOperation(ctx, req)
	case "ls", bindings.ListOperation:
		return b.lsOperation(ctx, req)
	case "pin-add":
		return b.pinAddOperation(ctx, req)
	case "pin-ls":
		return b.pinLsOperation(ctx, req)
	case "pin-rm", bindings.DeleteOperation:
		return b.pinRmOperation(ctx, req)
	}
	return &bindings.InvokeResponse{
		Data:     nil,
		Metadata: nil,
	}, nil
}
