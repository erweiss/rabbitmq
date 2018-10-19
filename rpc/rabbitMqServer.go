package rpc

import (
	"fmt"
	"log"

	"github.com/erweiss/rabbitmq/messaging"
	"github.com/erweiss/rabbitmq/receiver"
	"github.com/streadway/amqp"
)

type (
	RabbitMQServer struct {
		receiver *receiver.SimpleReceiver
		channel  *amqp.Channel
	}
)

func NewServer(host string, notification string) *RabbitMQServer {
	return &RabbitMQServer{
		receiver: receiver.NewSimpleReceiver(host, notification),
	}
}

func (s *RabbitMQServer) StartListening(callback func(*messaging.Message) (*messaging.Message, error)) error {

	// Open the channel for consuming requests
	requestChannel, err := s.Consume()
	if err != nil {
		return fmt.Errorf("Error while trying to start RabbitMQServer consumer: %s", err)
	}

	// Wait for the message to be received and forwarded to us
	requestMsg := <-requestChannel
	log.Printf("Server received channel relayed message")

	// Issue the callback method to process/address the message
	response, err := callback(requestMsg.Base)
	if err != nil {
		return fmt.Errorf("Error while performing callback to RabbitMQServer stakeholder: %s", err)
	}

	log.Printf("Server finished issuing callback, preparing to publish response")
	log.Printf("Notification: %s", requestMsg.Base.Notification)
	log.Printf("Request ID: %s", requestMsg.ID)
	log.Printf("Request ReplyTo: %s", requestMsg.ReplyTo)

	err = s.channel.Publish(
		"",                 // exchange
		requestMsg.ReplyTo, // routing key
		false,              // mandatory
		false,              // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: requestMsg.ID,
			Body:          response.Payload,
		})
	if err != nil {
		return fmt.Errorf("Error while publishing RabbitMqServer response: %s: %s", string(response.Payload), err)
	}

	return nil
}

func (s *RabbitMQServer) Consume() (<-chan *messaging.RpcMessage, error) {

	out := make(chan *messaging.RpcMessage)

	ch, err := receiver.Connect(s.receiver)
	if err != nil {
		return nil, err
	}
	// Save off the channel reference
	s.channel = ch

	q, err := s.DeclareQueue(ch)
	if err != nil {
		defer ch.Close()
		return nil, fmt.Errorf("Failed to declare a queue: %s", err)
	}

	err = s.SetQosPrefetch(ch)
	if err != nil {
		defer ch.Close()
		return nil, err
	}

	msgs, err := s.ConsumeChannel(q, ch)
	if err != nil {
		defer ch.Close()
		return nil, fmt.Errorf("Failed to register a consumer: %s", err)
	}

	go func(msgs <-chan amqp.Delivery, s *RabbitMQServer, out chan<- *messaging.RpcMessage) {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			out <- messaging.NewRpcMessage(d.CorrelationId, d.ReplyTo,
				messaging.NewMessage(s.GetNotification(), d.Body, ""))

			if !s.IsAutoAck() {
				d.Ack(false)
			}
		}
	}(msgs, s, out)

	return out, nil
}

func (s *RabbitMQServer) GetHost() string {
	return s.receiver.GetHost()
}

func (s *RabbitMQServer) IsDurable() bool {
	return s.receiver.IsDurable()
}

func (s *RabbitMQServer) GetDeliveryMode() uint8 {
	return s.receiver.GetDeliveryMode()
}

func (s *RabbitMQServer) GetUser() string {
	return s.receiver.GetUser()
}

func (s *RabbitMQServer) GetPassword() string {
	return s.receiver.GetPassword()
}

func (s *RabbitMQServer) IsAutoAck() bool {
	return false
}

func (s *RabbitMQServer) GetNotification() string {
	return s.receiver.GetNotification()
}

func (s *RabbitMQServer) SetQosPrefetch(channel *amqp.Channel) error {
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

func (s *RabbitMQServer) DeclareQueue(channel *amqp.Channel) (*amqp.Queue, error) {
	q, err := channel.QueueDeclare(
		s.GetNotification(), // name
		s.IsDurable(),       // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	return &q, err
}

func (s *RabbitMQServer) ConsumeChannel(queue *amqp.Queue, channel *amqp.Channel) (<-chan amqp.Delivery, error) {
	return channel.Consume(
		queue.Name,    // queue
		"",            // consumer
		s.IsAutoAck(), // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
}

//
//func fib(n int) int {
//	if n == 0 {
//		return 0
//	} else if n == 1 {
//		return 1
//	} else {
//		return fib(n-1) + fib(n-2)
//	}
//}
//
//func main() {
//	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
//	failOnError(err, "Failed to connect to RabbitMQ")
//	defer conn.Close()
//
//	ch, err := conn.Channel()
//	failOnError(err, "Failed to open a channel")
//	defer ch.Close()
//
//	q, err := ch.QueueDeclare(
//		"rpc_queue", // name
//		false, // durable
//		false, // delete when usused
//		false, // exclusive
//		false, // no-wait
//		nil,   // arguments
//	)
//	failOnError(err, "Failed to declare a queue")
//
//	err = ch.Qos(
//		1,     // prefetch count
//		0,     // prefetch size
//		false, // global
//	)
//	failOnError(err, "Failed to set QoS")
//
//	msgs, err := ch.Consume(
//		q.Name, // queue
//		"",     // consumer
//		false,   // auto-ack
//		false,  // exclusive
//		false,  // no-local
//		false,  // no-wait
//		nil,    // args
//	)
//	failOnError(err, "Failed to register a consumer")
//
//	forever := make(chan bool)
//
//	go func() {
//		for d := range msgs {
//			n, err := strconv.Atoi(string(d.Body))
//			failOnError(err, "Failed to convert body to integer")
//
//			log.Printf(" [.] fib(%d)", n)
//			response := fib(n)
//
//			err = ch.Publish(
//				"",        // exchange
//				d.ReplyTo, // routing key
//				false,     // mandatory
//				false,     // immediate
//				amqp.Publishing{
//					ContentType:   "text/plain",
//					CorrelationId: d.CorrelationId,
//					Body:          []byte(strconv.Itoa(response)),
//				})
//			failOnError(err, "Failed to publish a message")
//
//			d.Ack(false)
//		}
//	}()
//
//	log.Printf(" [*] Awaiting RPC requests")
//	<-forever
//}
