package kafkatracer

import "github.com/confluentinc/confluent-kafka-go/kafka"

type kafkaHeadersCarrier map[string]interface{}

func newKafkaHeaderCarrier(hdrs []kafka.Header) kafkaHeadersCarrier {
	carrier := kafkaHeadersCarrier{}
	for _, hdr := range hdrs {
		carrier[hdr.Key] = hdr.Value
	}
	return carrier
}

// ForeachKey conforms to the TextMapReader interface.
func (c kafkaHeadersCarrier) ForeachKey(handler func(key, val string) error) error {
	for k, val := range c {
		v, ok := val.(string)
		if !ok {
			continue
		}
		if err := handler(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Set implements Set() of opentracing.TextMapWriter.
func (c kafkaHeadersCarrier) Set(key, val string) {
	c[key] = val
}
