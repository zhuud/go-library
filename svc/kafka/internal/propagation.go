package internal

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/zhuud/go-library/utils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

var (
	kafkaTracer = otel.Tracer("github.com/zhuud/go-library/svc/kafka")
)

// Message wraps kafka.Message and provides convenient methods for header manipulation.
type Message struct {
	*kafka.Message
}

// NewMessage returns a new Message wrapper.
func NewMessage(msg *kafka.Message) *Message {
	return &Message{Message: msg}
}

// GetHeader returns the value associated with the passed key.
func (m *Message) GetHeader(key string) string {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// SetHeader stores the key-value pair, ensuring uniqueness of keys.
func (m *Message) SetHeader(key, val string) {
	// Ensure uniqueness of keys
	for i := 0; i < len(m.Headers); i++ {
		if m.Headers[i].Key == key {
			m.Headers = append(m.Headers[:i], m.Headers[i+1:]...)
			i--
		}
	}
	m.Headers = append(m.Headers, kafka.Header{
		Key:   key,
		Value: []byte(val),
	})
}

var _ propagation.TextMapCarrier = (*MessageCarrier)(nil)

// MessageCarrier injects and extracts traces from a Message.
// It implements propagation.TextMapCarrier for OpenTelemetry integration.
type MessageCarrier struct {
	msg *Message
}

// NewMessageCarrier returns a new MessageCarrier.
func NewMessageCarrier(msg *Message) MessageCarrier {
	return MessageCarrier{msg: msg}
}

// Get returns the value associated with the passed key.
func (m MessageCarrier) Get(key string) string {
	return m.msg.GetHeader(key)
}

// Set stores the key-value pair.
func (m MessageCarrier) Set(key string, value string) {
	m.msg.SetHeader(key, value)
}

// Keys lists the keys stored in this carrier.
func (m MessageCarrier) Keys() []string {
	out := make([]string, len(m.msg.Headers))
	for i, h := range m.msg.Headers {
		out[i] = h.Key
	}
	return out
}

func injectContextToMessage(ctx context.Context, msg *kafka.Message) {
	// wrap message into message carrier
	mc := NewMessageCarrier(NewMessage(msg))
	// inject trace context into message
	otel.GetTextMapPropagator().Inject(ctx, mc)
}

func contextFromMessage(msg kafka.Message) context.Context {
	// wrap message into message carrier
	mc := NewMessageCarrier(NewMessage(&msg))
	// extract trace context from message
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), mc)
	// remove deadline and cancel to isolate consumer processing timeout/cancel semantics
	ctx = utils.StripContext(ctx)
	return ctx
}
