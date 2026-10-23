package rabbitmq

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/nostosx/servex/messaging/broker"
	"github.com/nostosx/servex/metadata"
)

// AMQP headers used by the Broker. Their capital letters keep them apart from
// metadata keys, which are always lower case; they are not exposed in
// Message.Header of consumed messages.
const (
	// HeaderKey carries Message.Key.
	HeaderKey = "Servex-Key"
	// HeaderTopic carries the original routing key of a message that was
	// republished for redelivery.
	HeaderTopic = "Servex-Topic"
	// HeaderRedeliveries counts how many times a message was redelivered
	// after a retryable error.
	HeaderRedeliveries = "Servex-Redeliveries"
)

// toPublishing converts msg to a persistent AMQP message.
func toPublishing(msg *broker.Message) amqp.Publishing {
	p := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         slices.Clone(msg.Body),
	}
	if len(msg.Header) > 0 || msg.Key != "" {
		p.Headers = make(amqp.Table, len(msg.Header)+1)
		for k, vs := range msg.Header.All() {
			switch len(vs) {
			case 0:
			case 1:
				p.Headers[k] = vs[0]
			default:
				list := make([]any, len(vs))
				for i, v := range vs {
					list[i] = v
				}
				p.Headers[k] = list
			}
		}
		if msg.Key != "" {
			p.Headers[HeaderKey] = msg.Key
		}
	}
	return p
}

// toMessage converts a delivery to a broker message. Header values that are
// not strings are formatted; nested tables, such as the x-death header added
// by RabbitMQ, are skipped.
func toMessage(d amqp.Delivery) *broker.Message {
	msg := &broker.Message{Topic: d.RoutingKey, Body: slices.Clone(d.Body)}
	for _, k := range slices.Sorted(maps.Keys(d.Headers)) {
		v := d.Headers[k]
		switch k {
		case HeaderKey:
			msg.Key, _ = v.(string)
			continue
		case HeaderTopic:
			if topic, ok := v.(string); ok && topic != "" {
				msg.Topic = topic
			}
			continue
		case HeaderRedeliveries:
			continue
		}
		values := headerValues(v)
		if len(values) == 0 {
			continue
		}
		if msg.Header == nil {
			msg.Header = make(metadata.MD)
		}
		for _, s := range values {
			msg.Header.Add(k, s)
		}
	}
	return msg
}

// headerValues returns the string forms of an AMQP header value.
func headerValues(v any) []string {
	switch v := v.(type) {
	case []any:
		var out []string
		for _, e := range v {
			out = append(out, headerValues(e)...)
		}
		return out
	case amqp.Table, nil:
		return nil
	case string:
		return []string{v}
	case []byte:
		return []string{string(v)}
	case time.Time:
		return []string{v.UTC().Format(time.RFC3339)}
	default:
		return []string{fmt.Sprint(v)}
	}
}

// redeliveries returns the redelivery counter of a delivery.
func redeliveries(h amqp.Table) int {
	switch v := h[HeaderRedeliveries].(type) {
	case int:
		return max(v, 0)
	case int8:
		return max(int(v), 0)
	case int16:
		return max(int(v), 0)
	case int32:
		return max(int(v), 0)
	case int64:
		return max(int(v), 0)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case string:
		n, _ := strconv.Atoi(v)
		return max(n, 0)
	}
	return 0
}

// redelivery returns the copy of d that is republished for redelivery number
// count; topic is the original routing key.
func redelivery(d amqp.Delivery, topic string, count int) amqp.Publishing {
	headers := maps.Clone(d.Headers)
	if headers == nil {
		headers = make(amqp.Table, 2)
	}
	headers[HeaderRedeliveries] = int64(count)
	headers[HeaderTopic] = topic
	return amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Expiration:      d.Expiration,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		AppId:           d.AppId,
		Body:            d.Body,
	}
}
