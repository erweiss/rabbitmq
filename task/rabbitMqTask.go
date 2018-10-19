package task

import (
	"log"
	"github.com/streadway/amqp"
	"github.comcast.com/viper-cog/rabbitmq/messaging"
	"github.comcast.com/viper-cog/rabbitmq/sender"
)

type (
	RabbitMQTask struct {
		sender *sender.SimpleSender
	}
)

func NewTask(host string) *RabbitMQTask {
	return &RabbitMQTask{
		sender: sender.NewSimpleSender(host),
	}
}

func (t *RabbitMQTask) Send(message *messaging.Message) error {
	return sender.Send(t, message)
}

func (t *RabbitMQTask) DeclareQueue(channel *amqp.Channel, message *messaging.Message) error {
	_, err := channel.QueueDeclare(
		message.Notification, // name
		t.IsDurable(),   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	return err
}

func (t *RabbitMQTask) Publish(channel *amqp.Channel, message *messaging.Message) error {
	return channel.Publish(
		"",     // exchange
		message.Notification, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			DeliveryMode: t.GetDeliveryMode(),
			ContentType: "text/plain",
			Body:        message.Payload,
		})
}

func (t *RabbitMQTask) GetHost() string {
	return t.sender.GetHost()
}

func (t *RabbitMQTask) IsDurable() bool {
	log.Printf("Inside Task...")
	return true
}

func (t *RabbitMQTask) GetDeliveryMode() uint8 {
	return amqp.Persistent
}

func (t *RabbitMQTask) GetUser() string {
	return t.sender.GetUser()
}

func (t *RabbitMQTask) GetPassword() string {
	return t.sender.GetPassword()
}
