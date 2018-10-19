package receiver

import (
	"github.com/streadway/amqp"
	"github.comcast.com/viper-cog/rabbitmq/messaging"
)

type (
	SimpleReceiver struct {
		Host         string
		User         string
		Password     string
		Notification string
	}
)

func NewSimpleReceiver(host string, notification string) *SimpleReceiver{
	return &SimpleReceiver{
		Host: host,
		User: "guest",
		Password: "guest",
		Notification: notification,
	}
}


func (s *SimpleReceiver) Consume() (<-chan *messaging.Message, error){

	return Consume(s)
}

func (s *SimpleReceiver) GetHost() string {
	return s.Host
}

func (s *SimpleReceiver) IsDurable() bool {
	return false
}

func (s *SimpleReceiver) GetDeliveryMode() uint8 {
	return amqp.Transient
}

func (s *SimpleReceiver) GetUser() string {
	return s.User
}

func (s *SimpleReceiver) GetPassword() string {
	return s.Password
}

func (s *SimpleReceiver) IsAutoAck() bool {
	return true
}

func (s *SimpleReceiver) GetNotification() string {
	return s.Notification
}

func (s *SimpleReceiver) SetQosPrefetch(channel *amqp.Channel) error {
	return nil
}

func (s *SimpleReceiver) DeclareQueue(channel *amqp.Channel) (*amqp.Queue, error) {

	q, err := channel.QueueDeclare(
		s.GetNotification(), // name
		s.IsDurable(),    // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	return &q, err
}

func (s *SimpleReceiver) ConsumeChannel(queue *amqp.Queue, channel *amqp.Channel) (<-chan amqp.Delivery, error) {
	return channel.Consume(
		queue.Name, // queue
		"",     				// consumer
		s.IsAutoAck(), 	// auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
}