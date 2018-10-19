package subscriptions

import (
	"fmt"
	"log"

	"github.com/erweiss/rabbitmq/messaging"
	"github.com/erweiss/rabbitmq/sender"
	"github.com/streadway/amqp"
)

type (
	RabbitMQPublisher struct {
		Direct   bool
		Exchange string
		sender   *sender.SimpleSender
	}
)

func NewRabbitMQPublisher(host string, exchange string, direct bool) *RabbitMQPublisher {
	return &RabbitMQPublisher{
		Direct:   direct,
		Exchange: exchange,
		sender:   sender.NewSimpleSender(host),
	}
}

func (p *RabbitMQPublisher) Send(message *messaging.Message) error {
	log.Printf("Inside publisher.Send()")
	return sender.Send(p, message)
}

func (p *RabbitMQPublisher) GetHost() string {
	return p.sender.GetHost()
}

func (p *RabbitMQPublisher) IsDurable() bool {
	log.Printf("Inside Publisher...")
	return false
}

func (p *RabbitMQPublisher) GetDeliveryMode() uint8 {
	return amqp.Transient
}

func (p *RabbitMQPublisher) GetUser() string {
	return p.sender.GetUser()
}

func (p *RabbitMQPublisher) GetPassword() string {
	return p.sender.GetPassword()
}

func (p *RabbitMQPublisher) DeclareQueue(channel *amqp.Channel, message *messaging.Message) error {

	msgType := "fanout"
	if p.Direct {
		msgType = "direct"
	}

	log.Printf("(Publisher) Exchange: %s; type: %s", p.Exchange, msgType)

	err := channel.ExchangeDeclare(
		p.Exchange, // name
		msgType,    // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return fmt.Errorf("Failed to declare an exchange: %s", err)
	}

	log.Printf("(Publisher) Finished Declaring Exchange: %s", p.Exchange)

	return nil
}

func (p *RabbitMQPublisher) Publish(channel *amqp.Channel, message *messaging.Message) error {

	log.Printf("(Publisher) Publish method")
	routingKey := ""
	if p.Direct {
		routingKey = message.Notification
	}

	log.Printf("(Publisher) Routing Key: %s", routingKey)

	return channel.Publish(
		p.Exchange, // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message.Payload,
		})
}
