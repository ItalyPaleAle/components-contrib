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

package consul

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"

	consul "github.com/hashicorp/consul/api"

	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

const daprMeta string = "DAPR_PORT" // default key for DAPR_PORT metadata

type client struct {
	*consul.Client
}

func (c *client) InitClient(config *consul.Config) error {
	var err error

	c.Client, err = consul.NewClient(config)
	if err != nil {
		return fmt.Errorf("consul api error initing client: %w", err)
	}

	return nil
}

func (c *client) Agent() agentInterface {
	return c.Client.Agent()
}

func (c *client) Health() healthInterface {
	return c.Client.Health()
}

type clientInterface interface {
	InitClient(config *consul.Config) error
	Agent() agentInterface
	Health() healthInterface
}

type agentInterface interface {
	Self() (map[string]map[string]interface{}, error)
	ServiceRegister(service *consul.AgentServiceRegistration) error
}

type healthInterface interface {
	Service(service, tag string, passingOnly bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error)
}

type resolver struct {
	config resolverConfig
	logger logger.Logger
	client clientInterface
}

type resolverConfig struct {
	Client          *consul.Config
	QueryOptions    *consul.QueryOptions
	Registration    *consul.AgentServiceRegistration
	DaprPortMetaKey string
}

// NewResolver creates Consul name resolver.
func NewResolver(logger logger.Logger) nr.Resolver {
	return newResolver(logger, &client{})
}

func newResolver(logger logger.Logger, client clientInterface) *resolver {
	return &resolver{
		logger: logger,
		client: client,
	}
}

// Init will configure component. It will also register service or validate client connection based on config.
func (r *resolver) Init(metadata nr.Metadata) (err error) {
	r.config, err = getConfig(metadata)
	if err != nil {
		return err
	}

	if r.config.Client.TLSConfig.InsecureSkipVerify {
		r.logger.Infof("hashicorp consul: you are using 'insecureSkipVerify' to skip server config verify which is unsafe!")
	}

	err = r.client.InitClient(r.config.Client)
	if err != nil {
		return fmt.Errorf("failed to init consul client: %w", err)
	}

	// Register service to consul
	if r.config.Registration != nil {
		agent := r.client.Agent()

		err = agent.ServiceRegister(r.config.Registration)
		if err != nil {
			return fmt.Errorf("failed to register consul service: %w", err)
		}

		r.logger.Infof("service:%s registered on consul agent", r.config.Registration.Name)
	} else {
		_, err = r.client.Agent().Self()
		if err != nil {
			return fmt.Errorf("failed check on consul agent: %w", err)
		}
	}

	return nil
}

// ResolveID resolves name to address via consul.
func (r *resolver) ResolveID(req nr.ResolveRequest) (addr string, err error) {
	cfg := r.config
	services, _, err := r.client.Health().Service(req.ID, "", true, cfg.QueryOptions)
	if err != nil {
		return "", fmt.Errorf("failed to query healthy consul services: %w", err)
	}

	if len(services) == 0 {
		return "", fmt.Errorf("no healthy services found with AppID '%s'", req.ID)
	}

	// Pick a random service from the result
	// Note: we're using math/random here as PRNG and that's ok since we're just using this for selecting a random address from a list for load-balancing, so we don't need a CSPRNG
	//nolint:gosec
	svc := services[rand.Int()%len(services)]

	port := svc.Service.Meta[cfg.DaprPortMetaKey]
	if port == "" {
		return "", fmt.Errorf("target service AppID '%s' found but DAPR_PORT missing from meta", req.ID)
	}

	if svc.Service.Address != "" {
		addr = svc.Service.Address
	} else if svc.Node.Address != "" {
		addr = svc.Node.Address
	} else {
		return "", fmt.Errorf("no healthy services found with AppID '%s'", req.ID)
	}

	return formatAddress(addr, port)
}

func formatAddress(address string, port string) (addr string, err error) {
	if net.ParseIP(address).To4() != nil {
		return address + ":" + port, nil
	} else if net.ParseIP(address).To16() != nil {
		return fmt.Sprintf("[%s]:%s", address, port), nil
	}

	return "", fmt.Errorf("invalid ip address %s", address)
}

// getConfig configuration from metadata, defaults are best suited for self-hosted mode.
func getConfig(metadata nr.Metadata) (resolverCfg resolverConfig, err error) {
	if metadata.Instance.DaprInternalPort <= 0 {
		return resolverCfg, errors.New("missing Dapr port")
	}

	cfg, err := parseConfig(metadata.Configuration)
	if err != nil {
		return resolverCfg, err
	}

	// set DaprPortMetaKey used for registring DaprPort and resolving from Consul
	if cfg.DaprPortMetaKey == "" {
		resolverCfg.DaprPortMetaKey = daprMeta
	} else {
		resolverCfg.DaprPortMetaKey = cfg.DaprPortMetaKey
	}

	resolverCfg.Client = getClientConfig(cfg)
	resolverCfg.Registration, err = getRegistrationConfig(cfg, metadata.Instance)
	if err != nil {
		return resolverCfg, err
	}
	resolverCfg.QueryOptions = getQueryOptionsConfig(cfg)

	// if registering, set DaprPort in meta, needed for resolution
	if resolverCfg.Registration != nil {
		if resolverCfg.Registration.Meta == nil {
			resolverCfg.Registration.Meta = map[string]string{}
		}

		resolverCfg.Registration.Meta[resolverCfg.DaprPortMetaKey] = strconv.Itoa(metadata.Instance.DaprInternalPort)
	}

	return resolverCfg, nil
}

func getClientConfig(cfg configSpec) *consul.Config {
	// If no client config use library defaults
	if cfg.Client != nil {
		return cfg.Client
	}

	return consul.DefaultConfig()
}

func getRegistrationConfig(cfg configSpec, instance nr.Instance) (*consul.AgentServiceRegistration, error) {
	// if advanced registration configured ignore other registration related configs
	if cfg.AdvancedRegistration != nil {
		return cfg.AdvancedRegistration, nil
	}
	if !cfg.SelfRegister {
		return nil, nil
	}

	if instance.AppID == "" {
		return nil, errors.New("missing app ID")
	}

	if instance.AppPort <= 0 {
		return nil, errors.New("missing app port")
	}

	if instance.Address == "" {
		return nil, errors.New("missing address")
	}

	if instance.DaprHTTPPort <= 0 {
		return nil, errors.New("missing Dapr HTTP port")
	}

	httpPort := strconv.Itoa(instance.DaprHTTPPort)
	id := instance.AppID + "-" + instance.Address + "-" + httpPort
	// if no health checks configured add dapr sidecar health check by default
	if len(cfg.Checks) == 0 {
		cfg.Checks = []*consul.AgentServiceCheck{
			{
				Name:     "Dapr Health Status",
				CheckID:  "daprHealth:" + id,
				Interval: "15s",
				HTTP:     fmt.Sprintf("http://%s/v1.0/healthz?appid=%s", net.JoinHostPort(instance.Address, httpPort), instance.AppID),
			},
		}
	}

	return &consul.AgentServiceRegistration{
		ID:      id,
		Name:    instance.AppID,
		Address: instance.Address,
		Port:    instance.AppPort,
		Checks:  cfg.Checks,
		Tags:    cfg.Tags,
		Meta:    cfg.Meta,
	}, nil
}

func getQueryOptionsConfig(cfg configSpec) *consul.QueryOptions {
	// if no query options configured add default filter matching every tag in config
	if cfg.QueryOptions == nil {
		return &consul.QueryOptions{
			UseCache: true,
		}
	}

	return cfg.QueryOptions
}
