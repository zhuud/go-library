package internal

import (
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/propagation"
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
