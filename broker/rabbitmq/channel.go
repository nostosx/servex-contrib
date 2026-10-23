package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// amqpChannel is the part of an AMQP channel used by the Broker. Production
// code uses *amqp.Channel through liveChannel; tests replace the server with
// an in-memory fake.
type amqpChannel interface {
	IsClosed() bool
	Close() error
	Confirm(noWait bool) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	NotifyCancel(c chan string) chan string
	// publishConfirmed publishes p on a channel in confirm mode and waits
	// for the confirmation, reporting whether the server acknowledged it.
	publishConfirmed(ctx context.Context, exchange, key string, p amqp.Publishing) (bool, error)
}

// liveChannel adapts *amqp.Channel to amqpChannel.
type liveChannel struct {
	*amqp.Channel
}

func (c liveChannel) publishConfirmed(ctx context.Context, exchange, key string, p amqp.Publishing) (bool, error) {
	dc, err := c.PublishWithDeferredConfirmWithContext(ctx, exchange, key, false, false, p)
	if err != nil {
		return false, err
	}
	return dc.WaitContext(ctx)
}

// dialer returns a function that opens channels on conn.
func dialer(conn *amqp.Connection) func() (amqpChannel, error) {
	return func() (amqpChannel, error) {
		ch, err := conn.Channel()
		if err != nil {
			return nil, err
		}
		return liveChannel{ch}, nil
	}
}
