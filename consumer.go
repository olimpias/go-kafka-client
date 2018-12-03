package opentracingkafka

import (
	"context"
	"errors"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/olimpias/go-kafka-client/kafkatracer"
	"github.com/opentracing/opentracing-go"
)

// Consumer is a wrapper for kafka.Consumer
type Consumer struct {
	*kafka.Consumer
}

// MessageCallback is a callback method for receiving kafka messages. Span subsequent reference can be extracted from context
type MessageCallback func(context.Context, *kafka.Message)

// ErrorCallBack is a callback method for error handling. If kafka sends a error this method will be called.
// error returns the error that occured in kafka. bool value show the if the kafka listen loop is closed.
type ErrorCallBack func(error, bool)

// NewConsumer is a constructor for wrapper Consumer.
func NewConsumer(conf *kafka.ConfigMap) (*Consumer, error) {
	c, err := kafka.NewConsumer(conf)
	if err != nil {
		return nil, err
	}
	localC := &Consumer{c}
	return localC, nil
}

// ConsumeWithSpan consumes the kafka bus also extract the span values
// Note that input context does not have affect on loop. It is just used for Opentracing
func (c *Consumer) ConsumeWithSpan(ctx context.Context, callback MessageCallback, errCallback ErrorCallBack) {
	for {
		ev := c.Poll(100)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *kafka.Message:
			spCtx, err := kafkatracer.Extract(e.Headers)
			if err != nil {
				errCallback(err, false)
			}
			sp := opentracing.StartSpan("ConsumerMessage", opentracing.FollowsFrom(spCtx))
			defer sp.Finish()
			ctx = opentracing.ContextWithSpan(ctx, sp)
			callback(ctx, e)
		case kafka.PartitionEOF:
			errCallback(e.Error, false)
		case kafka.Error:
			errCallback(errors.New(e.Error()), true)
			break
		}
	}
}

// AsyncConsumeWithSpan triggers the ConsumeWithSpan with goroutine.
// Note that input context does not have affect on loop. It is just used for Opentracing
func (c *Consumer) AsyncConsumeWithSpan(ctx context.Context, callback MessageCallback, errCallback ErrorCallBack) {
	go c.ConsumeWithSpan(ctx, callback, errCallback)
}
