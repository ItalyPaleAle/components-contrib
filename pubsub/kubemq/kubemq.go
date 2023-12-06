package kubemq

import (
	"context"
	"reflect"
	"strconv"
	"time"

	"github.com/google/uuid"

	contribMetadata "github.com/dapr/components-contrib/common/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type kubeMQ struct {
	metadata         *kubemqMetadata
	logger           logger.Logger
	eventsClient     *kubeMQEvents
	eventStoreClient *kubeMQEventStore
}

func NewKubeMQ(logger logger.Logger) pubsub.PubSub {
	return &kubeMQ{
		logger: logger,
	}
}

func (k *kubeMQ) Init(_ context.Context, metadata pubsub.Metadata) error {
	meta, err := createMetadata(metadata)
	if err != nil {
		k.logger.Errorf("error init kubemq client error: %s", err.Error())
		return err
	}
	k.metadata = meta
	if meta.IsStore {
		k.eventStoreClient = newKubeMQEventsStore(k.logger)
		return k.eventStoreClient.Init(meta)
	} else {
		k.eventsClient = newkubeMQEvents(k.logger)
		return k.eventsClient.Init(meta)
	}
}

func (k *kubeMQ) Features() []pubsub.Feature {
	return nil
}

func (k *kubeMQ) Publish(_ context.Context, req *pubsub.PublishRequest) error {
	if k.metadata.IsStore {
		return k.eventStoreClient.Publish(req)
	} else {
		return k.eventsClient.Publish(req)
	}
}

func (k *kubeMQ) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if k.metadata.IsStore {
		return k.eventStoreClient.Subscribe(ctx, req, handler)
	} else {
		return k.eventsClient.Subscribe(ctx, req, handler)
	}
}

func (k *kubeMQ) Close() error {
	if k.metadata.IsStore {
		return k.eventStoreClient.Close()
	} else {
		return k.eventsClient.Close()
	}
}

func getRandomID() string {
	randomUUID, err := uuid.NewRandom()
	if err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return randomUUID.String()
}

// GetComponentMetadata returns the metadata of the component.
func (k *kubeMQ) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := &kubemqMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.PubSubType)
	return
}
