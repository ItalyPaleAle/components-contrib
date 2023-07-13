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

package sidecar

import (
	"time"

	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/flow"

	rtembedded "github.com/dapr/components-contrib/tests/certification/embedded"
	// Go SDK
	dapr "github.com/dapr/go-sdk/client"
)

type (
	Client struct {
		dapr.Client
		rt *runtime.DaprRuntime
	}

	Sidecar struct {
		appID                    string
		options                  []rtembedded.Option
		gracefulShutdownDuration time.Duration
	}
)

func GetClient(ctx flow.Context, sidecarName string) *Client {
	var client *Client
	ctx.MustGet(sidecarName, &client)
	return client
}

func Run(appID string, options ...rtembedded.Option) (string, flow.Runnable, flow.Runnable) {
	return New(appID, options...).ToStep()
}

func New(appID string, options ...rtembedded.Option) Sidecar {
	return Sidecar{
		appID:   appID,
		options: options,
	}
}

func (s Sidecar) AppID() string {
	return s.appID
}

func (s Sidecar) ToStep() (string, flow.Runnable, flow.Runnable) {
	return s.appID, s.Start, s.Stop
}

func Start(appID string, options ...rtembedded.Option) flow.Runnable {
	return Sidecar{appID, options, 0}.Start
}

func (s Sidecar) Start(ctx flow.Context) error {
	logContrib := logger.NewLogger("dapr.contrib")

	registry := rtembedded.GetComponentRegistry(logContrib)

	rtoptions := []rtembedded.Option{
		rtembedded.WithComponentRegistry(registry),
	}

	rt, rtConf, err := rtembedded.NewRuntime(s.appID, rtoptions...)
	if err != nil {
		return err
	}
	s.gracefulShutdownDuration = time.Duration(rtConf.DaprGracefulShutdownSeconds) * time.Second

	client := Client{
		rt: rt,
	}

	if err = rt.Run(ctx); err != nil {
		return err
	}

	daprClient, err := dapr.NewClientWithPort(rtConf.DaprAPIGRPCPort)
	if err != nil {
		return err
	}

	client.Client = daprClient

	ctx.Set(s.appID, &client)

	return nil
}

func Stop(appID string) flow.Runnable {
	return Sidecar{appID: appID}.Stop
}

func (s Sidecar) Stop(ctx flow.Context) error {
	var client *Client
	if ctx.Get(s.appID, &client) {
		client.rt.SetRunning(true)
		client.rt.Shutdown(s.gracefulShutdownDuration)

		return client.rt.WaitUntilShutdown()
	}

	return nil
}
