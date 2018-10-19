package subscriptions

import (
	"fmt"
	"log"
	"github.com/streadway/amqp"
	"github.comcast.com/viper-cog/rabbitmq/messaging"
	"github.comcast.com/viper-cog/rabbitmq/receiver"
)

type (
	RabbitMQConsumer struct {
		Direct   bool
		Exchange string
		receiver *receiver.SimpleReceiver
	}
)

func NewRabbitMQConsumer(notification string, host string, exchange string, direct bool) *RabbitMQConsumer {
	return &RabbitMQConsumer{
		Direct:   direct,
		Exchange: exchange,
		receiver: receiver.NewSimpleReceiver(host, notification),
	}
}

func (c *RabbitMQConsumer) GetHost() string {
	return c.receiver.GetHost()
}

func (c *RabbitMQConsumer) IsDurable() bool {
	return true
}

func (c *RabbitMQConsumer) GetDeliveryMode() uint8 {
	return amqp.Transient
}

func (c *RabbitMQConsumer) GetUser() string {
	return c.receiver.GetUser()
}

func (c *RabbitMQConsumer) GetPassword() string {
	return c.receiver.GetPassword()
}

func (c *RabbitMQConsumer) IsAutoAck() bool {
	return true
}

func (c *RabbitMQConsumer) GetNotification() string {
	return c.receiver.GetNotification()
}

func (c *RabbitMQConsumer) SetQosPrefetch(channel *amqp.Channel) error {
	log.Printf("Setting fake prefetch")
	return nil
}

func (c *RabbitMQConsumer) Consume() (<-chan *messaging.Message, error) {
	return receiver.Consume(c)
}

func (c *RabbitMQConsumer) DeclareQueue(channel *amqp.Channel) (*amqp.Queue, error) {

	msgType := "fanout"
	if c.Direct {
		msgType = "direct"
	}

	log.Printf("(Consumer) Exchange: %s; type: %s", c.Exchange, msgType)
	err := channel.ExchangeDeclare(
		c.Exchange, // name
		msgType,    // type
		true,       // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare an exchange: %s", err)
	}

	q, err := channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return nil, fmt.Errorf("Failed to declare a queue: %s", err)
	}

	routingKey := ""
	if c.Direct {
		routingKey = c.GetNotification()
	}

	log.Printf("Routing Key: %s", routingKey)
	log.Printf("Queue Name: %s", q.Name)
	log.Printf("Exchange Name: %s", c.Exchange)

	err = channel.QueueBind(
		q.Name,     // queue name
		routingKey, // routing key
		c.Exchange, // exchange
		false,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("Failed to bind a queue: %s", err)
	}
	log.Printf("Bound to Queue Name: %s", q.Name)

	return &q, nil
}

func (c *RabbitMQConsumer) ConsumeChannel(queue *amqp.Queue, channel *amqp.Channel) (<-chan amqp.Delivery, error) {

	log.Printf("(Consumer) Consume Channel")
	log.Printf("(Consumer) Queue Name: %s", queue.Name)

	return channel.Consume(
		queue.Name,    // queue
		"",            // consumer
		c.IsAutoAck(), // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
}
