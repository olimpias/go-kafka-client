package kafkatracer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	opentracing "github.com/opentracing/opentracing-go"
)

// Inject injects the span context into the Kafka header.
func Inject(span opentracing.Span, hdrs []kafka.Header) error {
	c := newKafkaHeaderCarrier(hdrs)
	return span.Tracer().Inject(span.Context(), opentracing.TextMap, c)
}

// Extract extracts the span context out of the Kafka header.
func Extract(hdrs []kafka.Header) (opentracing.SpanContext, error) {
	c := newKafkaHeaderCarrier(hdrs)
	return opentracing.GlobalTracer().Extract(opentracing.TextMap, c)
}
