package worker

import (
	"log"
	"github.com/streadway/amqp"
	"github.comcast.com/viper-cog/rabbitmq/receiver"
	"github.comcast.com/viper-cog/rabbitmq/messaging"
	"fmt"
)

type (
	RabbitMQWorker struct {
		receiver *receiver.SimpleReceiver
	}
)

func NewWorker(host string, notification string) *RabbitMQWorker {
	return &RabbitMQWorker{
		receiver: receiver.NewSimpleReceiver(host, notification),
	}
}

func (w RabbitMQWorker) Consume() (<-chan *messaging.Message, error){
	return receiver.Consume(w)
}

func (w RabbitMQWorker) GetHost() string {
	return w.receiver.GetHost()
}

func (w RabbitMQWorker) IsDurable() bool {
	log.Printf("Inside Worker Durable...")
	return true
}

func (w RabbitMQWorker) GetDeliveryMode() uint8 {
	return amqp.Persistent
}

func (w RabbitMQWorker) GetUser() string {
	return w.receiver.GetUser()
}

func (w RabbitMQWorker) GetPassword() string {
	return w.receiver.GetPassword()
}

func (w RabbitMQWorker) IsAutoAck() bool {
	return false
}

func (w RabbitMQWorker) GetNotification() string {
	return w.receiver.GetNotification()
}

func (w RabbitMQWorker) SetQosPrefetch(channel *amqp.Channel) error {
	log.Printf("Inside Worker SetQosPrefetch...")
	err := channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return fmt.Errorf("Failed to set QoS: %s", err)
	}
	return nil
}

func (w RabbitMQWorker) DeclareQueue(channel *amqp.Channel) (*amqp.Queue, error) {

	q, err := channel.QueueDeclare(
		w.GetNotification(), // name
		w.IsDurable(),    // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	return &q, err
}

func (w RabbitMQWorker) ConsumeChannel(queue *amqp.Queue, channel *amqp.Channel) (<-chan amqp.Delivery, error) {
	return channel.Consume(
		queue.Name, // queue
		"",     				// consumer
		w.IsAutoAck(), 	// auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
}