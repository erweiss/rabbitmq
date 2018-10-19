package receiver

import (
	"fmt"
	"log"

	"github.com/erweiss/rabbitmq/messaging"
	"github.com/streadway/amqp"
)

type (
	RabbitMQReceiver interface {
		Consume() (<-chan *messaging.Message, error)
		DeclareQueue(channel *amqp.Channel) (*amqp.Queue, error)
		ConsumeChannel(queue *amqp.Queue, channel *amqp.Channel) (<-chan amqp.Delivery, error)
		GetNotification() string
		GetHost() string
		GetUser() string
		GetPassword() string
		IsDurable() bool
		GetDeliveryMode() uint8
		IsAutoAck() bool
		SetQosPrefetch(channel *amqp.Channel) error
	}
)

func Consume(r RabbitMQReceiver) (<-chan *messaging.Message, error) {

	out := make(chan *messaging.Message)

	ch, err := Connect(r)
	if err != nil {
		return nil, err
	}

	q, err := r.DeclareQueue(ch)

	if err != nil {
		defer ch.Close()
		return nil, fmt.Errorf("Failed to declare a queue: %s", err)
	}

	err = r.SetQosPrefetch(ch)
	if err != nil {
		defer ch.Close()
		return nil, err
	}

	msgs, err := r.ConsumeChannel(q, ch)

	if err != nil {
		defer ch.Close()
		return nil, fmt.Errorf("Failed to register a consumer: %s", err)
	}

	go func(msgs <-chan amqp.Delivery, r RabbitMQReceiver, out chan<- *messaging.Message) {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			out <- messaging.NewMessage(r.GetNotification(), d.Body, "")

			if !r.IsAutoAck() {
				d.Ack(false)
			}
		}
	}(msgs, r, out)

	return out, nil
}

func Connect(r RabbitMQReceiver) (*amqp.Channel, error) {

	strConnection := fmt.Sprintf("amqp://%s:%s@%s:5672/", r.GetUser(), r.GetPassword(), r.GetHost())

	conn, err := amqp.Dial(strConnection)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to RabbitMQ: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		defer conn.Close()
		return nil, fmt.Errorf("Failed to open a channel: %s", err)
	}

	return ch, nil
}
