package opentracingkafka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/olimpias/go-kafka-client/kafkatracer"
	"github.com/opentracing/opentracing-go"
)

// Producer is a wrapper for Kafka.Producer.
type Producer struct {
	*kafka.Producer
}

// NewProducer creates a Producer. If trace is not null, trace will be usable.
// It uses kafka.NewProducer under the hood to initiate producer configuration.
func NewProducer(conf *kafka.ConfigMap) (*Producer, error) {
	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, err
	}
	localP := &Producer{p}
	return localP, nil
}

// ProduceWithSpan allows you to use opentracing specifications. The message that is produced, will also contain span context.
func (p *Producer) ProduceWithSpan(ctx context.Context, msg *kafka.Message, deliveryChan chan kafka.Event) error {
	sp := opentracing.SpanFromContext(ctx)
	if err := kafkatracer.Inject(sp, msg.Headers); err != nil {
		return err
	}
	return p.Produce(msg, deliveryChan)
}
