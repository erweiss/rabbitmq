package messaging

import (
	"testing"
	"log"
)

type (
	MockReceiver struct {
		Host string
		QueueName string
		MockChannel <-chan *Message
	}

	MockSender struct {
		Host string
		Channel chan<- *Message
	}
)

func TestSimpleMessage(t *testing.T) {

		in, sReceiver := GetMockReceiver("localhost", "queue_test")

		out, err := sReceiver.Consume()
		if err != nil {
			t.Error(err)
		}

		go func(in chan<- *Message) {
			sSender := GetMockSender("localhost", in)
			msg := NewMessage("queue_test", ([]byte)("Hello World"), "")
			sSender.Send(msg)
		}(in)

		msg := <- out
		log.Printf("Success: Forwarded Message: %s", string(msg.Payload))
}

func GetMockReceiver(host string, notification string) (chan *Message, Receiver) {
	in := make(chan *Message)

	return in, MockReceiver{
		Host: host,
		QueueName: notification,
		MockChannel: in,
	}
}

func GetMockSender(host string, channel chan<- *Message) Sender {
	return MockSender{
		Host: host,
		Channel: channel,
	}
}

func (m MockReceiver) Consume() (<-chan *Message, error) {

	out := make(chan *Message)

	go func(in <-chan *Message) {
		for {
			select {
				case msg := <-in:
					log.Printf("Success: Queue: %s; Received message: %s", msg.Notification, string(msg.Payload))

					out <- msg
					break
			}
		}
	}(m.MockChannel)

	return out, nil
}

func (m MockReceiver) Stop() error {
	return nil
}


func (m MockSender) Send(message *Message) error {
	log.Printf("[x] Sending message: %s", string(message.Payload))
	m.Channel <- message
	return nil
}



