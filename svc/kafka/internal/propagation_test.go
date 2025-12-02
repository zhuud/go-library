package internal

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

// ===== Message Tests =====

func TestMessageGetHeader(t *testing.T) {
	testCases := []struct {
		name     string
		msg      *Message
		key      string
		expected string
	}{
		{
			name: "exists",
			msg: &Message{
				Message: &kafka.Message{Headers: []kafka.Header{
					{Key: "foo", Value: []byte("bar")},
				}}},
			key:      "foo",
			expected: "bar",
		},
		{
			name:     "not exists",
			msg:      &Message{Message: &kafka.Message{Headers: []kafka.Header{}}},
			key:      "foo",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.msg.GetHeader(tc.key)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestMessageSetHeader(t *testing.T) {
	msg := &Message{Message: &kafka.Message{Headers: []kafka.Header{
		{Key: "foo", Value: []byte("bar")}},
	}}

	msg.SetHeader("foo", "bar2")
	msg.SetHeader("foo2", "bar2")
	msg.SetHeader("foo2", "bar3")
	msg.SetHeader("foo3", "bar4")

	assert.ElementsMatch(t, msg.Headers, []kafka.Header{
		{Key: "foo", Value: []byte("bar2")},
		{Key: "foo2", Value: []byte("bar3")},
		{Key: "foo3", Value: []byte("bar4")},
	})
}

// ===== MessageCarrier Tests =====

func TestMessageCarrierGet(t *testing.T) {
	testCases := []struct {
		name     string
		carrier  MessageCarrier
		key      string
		expected string
	}{
		{
			name: "exists",
			carrier: NewMessageCarrier(&Message{&kafka.Message{Headers: []kafka.Header{
				{Key: "foo", Value: []byte("bar")},
			}}}),
			key:      "foo",
			expected: "bar",
		},
		{
			name:     "not exists",
			carrier:  NewMessageCarrier(&Message{&kafka.Message{Headers: []kafka.Header{}}}),
			key:      "foo",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.carrier.Get(tc.key)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestMessageCarrierSet(t *testing.T) {
	msg := Message{&kafka.Message{Headers: []kafka.Header{
		{Key: "foo", Value: []byte("bar")},
	}}}
	carrier := MessageCarrier{msg: &msg}

	carrier.Set("foo", "bar2")
	carrier.Set("foo2", "bar2")
	carrier.Set("foo2", "bar3")
	carrier.Set("foo3", "bar4")

	assert.ElementsMatch(t, carrier.msg.Headers, []kafka.Header{
		{Key: "foo", Value: []byte("bar2")},
		{Key: "foo2", Value: []byte("bar3")},
		{Key: "foo3", Value: []byte("bar4")},
	})
}

func TestMessageCarrierKeys(t *testing.T) {
	testCases := []struct {
		name     string
		carrier  MessageCarrier
		expected []string
	}{
		{
			name: "one",
			carrier: MessageCarrier{msg: &Message{&kafka.Message{Headers: []kafka.Header{
				{Key: "foo", Value: []byte("bar")},
			}}}},
			expected: []string{"foo"},
		},
		{
			name:     "none",
			carrier:  MessageCarrier{msg: &Message{&kafka.Message{Headers: []kafka.Header{}}}},
			expected: []string{},
		},
		{
			name: "many",
			carrier: MessageCarrier{msg: &Message{&kafka.Message{Headers: []kafka.Header{
				{Key: "foo", Value: []byte("bar")},
				{Key: "baz", Value: []byte("quux")},
			}}}},
			expected: []string{"foo", "baz"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.carrier.Keys()
			assert.Equal(t, tc.expected, result)
		})
	}
}
