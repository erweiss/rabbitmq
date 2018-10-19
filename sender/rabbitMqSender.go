package sender

import (
	"fmt"
	"log"

	"github.com/erweiss/rabbitmq/messaging"
	"github.com/streadway/amqp"
)

type (
	RabbitMQSender interface {
		Send(message *messaging.Message) error
		DeclareQueue(channel *amqp.Channel, message *messaging.Message) error
		Publish(channel *amqp.Channel, message *messaging.Message) error
		GetHost() string
		GetUser() string
		GetPassword() string
		IsDurable() bool
		GetDeliveryMode() uint8
	}
)

func Send(s RabbitMQSender, message *messaging.Message) error {

	ch, err := Connect(s)
	if err != nil {
		return err
	}

	err = s.DeclareQueue(ch, message)

	if err != nil {
		defer ch.Close()
		return fmt.Errorf("Failed to declare a queue: %s", err)
	}

	err = s.Publish(ch, message)

	log.Printf(" [x] Sent %s", message)
	if err != nil {
		defer ch.Close()
		return fmt.Errorf("Failed to publish a message: %s", err)
	}
	return nil
}

func Connect(s RabbitMQSender) (*amqp.Channel, error) {

	strConnection := fmt.Sprintf("amqp://%s:%s@%s:5672/", s.GetUser(), s.GetPassword(), s.GetHost())

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
