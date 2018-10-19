package rpc

import (
	"fmt"
	"log"
	"github.com/streadway/amqp"
	"github.comcast.com/viper-cog/rabbitmq/messaging"
	"github.comcast.com/viper-cog/rabbitmq/receiver"
	"github.comcast.com/viper-cog/rabbitmq/sender"
	"github.com/pborman/uuid"
)

type (
	RabbitMQClient struct {
		sender   *sender.SimpleSender
	}
)

func NewClient(host string) *RabbitMQClient {
	return &RabbitMQClient{
		sender:   sender.NewSimpleSender(host),
	}
}

func (c *RabbitMQClient) GetHost() string {
	return c.sender.GetHost()
}

func (c *RabbitMQClient) IsDurable() bool {
	return c.sender.IsDurable()
}

func (c *RabbitMQClient) GetDeliveryMode() uint8 {
	return c.sender.GetDeliveryMode()
}

func (c *RabbitMQClient) GetUser() string {
	return c.sender.GetUser()
}

func (c *RabbitMQClient) GetPassword() string {
	return c.sender.GetPassword()
}

func (c *RabbitMQClient) SendCommand(message *messaging.Message) (*messaging.Message, error) {

	log.Printf("Client SendCommand...")
	cmdReceiver := receiver.NewSimpleReceiver(c.GetHost(), message.Notification)

	ch, err := receiver.Connect(cmdReceiver)
	if err != nil {
		return nil, err
	}

	// Create the queue
	q, err := c.DeclareQueue(ch)
	if err != nil {
		defer ch.Close()
		return nil, err
	}
	// Kick off the consuming portion of our rpc client
	msgs, err := cmdReceiver.ConsumeChannel(q, ch)
	if err != nil {
		defer ch.Close()
		return nil, fmt.Errorf("Failed to register a consumer: %s", err)
	}

	// Send the actual request to the rpc server
	correlationId, err := c.Publish(q, ch, message)
	log.Printf(" [x] Sent %s", message)
	if err != nil {
		defer ch.Close()
		return nil, fmt.Errorf("Failed to publish a message: %s", err)
	}

	// Create the channel to receive the response from the server
	out := make(chan *messaging.Message)
	go func(correlationId string, msgs <-chan amqp.Delivery, r *receiver.SimpleReceiver, out chan<- *messaging.Message) {
		for d := range msgs {
			if correlationId == d.CorrelationId {
				log.Printf("Received a message: %s", d.Body)
				out <- messaging.NewMessage(r.GetNotification(), d.Body, "")
			}
			if !r.IsAutoAck() {
				d.Ack(false)
			}
		}
	}(correlationId, msgs, cmdReceiver, out)

	forwardedMsg := <-out
	return forwardedMsg, nil
}

func (c *RabbitMQClient) DeclareQueue(channel *amqp.Channel) (*amqp.Queue, error) {
	q, err := channel.QueueDeclare(
		"",            // name
		c.IsDurable(), // durable
		false,         // delete when unused
		true,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)

	return &q, err
}

func (c *RabbitMQClient) Publish(queue *amqp.Queue, channel *amqp.Channel, message *messaging.Message) (string, error) {

	// Create a new UUID string
	corrId := uuid.New()
	log.Printf("Client Publish - %s", corrId)

	return corrId, channel.Publish(
		"",                   // exchange
		message.Notification, // routing key
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			DeliveryMode:  c.GetDeliveryMode(),
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       queue.Name,
			Body:          message.Payload,
		})
}

//func randomString(l int) string {
//	bytes := make([]byte, l)
//	for i := 0; i < l; i++ {
//		bytes[i] = byte(randInt(65, 90))
//	}
//	return string(bytes)
//}
//
//func randInt(min int, max int) int {
//	return min + rand.Intn(max-min)
//}
//
//func fibonacciRPC(n int) (res int, err error) {
//	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
//	failOnError(err, "Failed to connect to RabbitMQ")
//	defer conn.Close()
//
//	ch, err := conn.Channel()
//	failOnError(err, "Failed to open a channel")
//	defer ch.Close()
//
//	q, err := ch.QueueDeclare(
//		"",     // name
//		false,  // durable
//		false,  // delete when unused
//		true,   // exclusive
//		false,  // no-wait
//		nil,    // arguments
//	)
//	failOnError(err, "Failed to declare a queue")
//
//	msgs, err := ch.Consume(
//		q.Name, // queue
//		"",     // consumer
//		true,   // auto-ack
//		false,  // exclusive
//		false,  // no-local
//		false,  // no-wait
//		nil,    // args
//	)
//	failOnError(err, "Failed to register a consumer")
//
//	corrId := randomString(32)
//
//	err = ch.Publish(
//		"",          // exchange
//		"rpc_queue", // routing key
//		false,  // mandatory
//		false,  // immediate
//		amqp.Publishing{
//			ContentType: "text/plain",
//			CorrelationId: corrId,
//			ReplyTo:      q.Name,
//			Body:        []byte(strconv.Itoa(n)),
//		})
//		failOnError(err, "Failed to publish a message")
//
//	for d := range msgs {
//		if corrId == d.CorrelationId {
//			res, err = strconv.Atoi(string(d.Body))
//			failOnError(err, "Failed to convert body to integer")
//			break
//		}
//	}
//
//	return
//}
//
//func main() {
//	rand.Seed(time.Now().UTC().UnixNano())
//
//	n := bodyFrom(os.Args)
//
//	log.Printf(" [x] Requesting fib(%d)", n)
//	res, err := fibonacciRPC(n)
//	failOnError(err, "Failed to handle RPC request")
//
//	log.Printf(" [.] Got %d", res)
//}
//
//func bodyFrom(args []string) int {
//	var s string
//	if (len(args) < 2) || os.Args[1] == "" {
//		s = "30"
//	} else {
//		s = strings.Join(args[1:], " ")
//	}
//	n, err := strconv.Atoi(s)
//	failOnError(err, "Failed to convert arg to integer")
//	return n
//}
//
//func severityFrom(args []string) string {
//	var s string
//	if (len(args) < 2) || os.Args[1] == "" {
//		s = "info"
//	} else {
//		s = os.Args[1]
//	}
//	return s
//}
