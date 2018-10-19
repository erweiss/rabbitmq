package sender

import (
	"github.com/erweiss/rabbitmq/messaging"
	"github.com/streadway/amqp"
)

type (
	SimpleSender struct {
		Host     string
		User     string
		Password string
	}
)

func NewSimpleSender(host string) *SimpleSender {
	return &SimpleSender{
		Host:     host,
		User:     "guest",
		Password: "guest",
	}
}

func (s *SimpleSender) GetHost() string {
	return s.Host
}

func (s *SimpleSender) IsDurable() bool {
	return false
}

func (s *SimpleSender) GetDeliveryMode() uint8 {
	return amqp.Transient
}

func (s *SimpleSender) GetUser() string {
	return s.User
}

func (s *SimpleSender) GetPassword() string {
	return s.Password
}

func (s *SimpleSender) Send(message *messaging.Message) error {
	return Send(s, message)
}

func (s *SimpleSender) DeclareQueue(channel *amqp.Channel, message *messaging.Message) error {
	_, err := channel.QueueDeclare(
		message.Notification, // name
		s.IsDurable(),        // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	return err
}

func (s *SimpleSender) Publish(channel *amqp.Channel, message *messaging.Message) error {
	return channel.Publish(
		"",                   // exchange
		message.Notification, // routing key
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			DeliveryMode: s.GetDeliveryMode(),
			ContentType:  "text/plain",
			Body:         message.Payload,
		})
}
