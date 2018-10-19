package rabbitmq

import (
	"testing"
	"time"

	"github.com/erweiss/rabbitmq/messaging"
	"github.com/erweiss/rabbitmq/receiver"
	"github.com/erweiss/rabbitmq/sender"
)

func TestSimpleMessage(t *testing.T) {

	sSender := GetSender("localhost")
	msg := messaging.NewMessage("queue_test", ([]byte)("Hello World (Simple Test)"), "")
	sSender.Send(msg)

	sReceiver := GetReceiver("localhost", "queue_test")
	ch, err := sReceiver.Consume()
	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Millisecond)
	forwardedMsg := <-ch
	t.Logf("Success: Forwarded Message: %s", string(forwardedMsg.Payload))
}

func TestSimpleMessage_InvalidReceivingQueue_ShouldFail(t *testing.T) {

	var err error
	sSender := GetSender("localhost1")
	msg := messaging.NewMessage("queue_test", ([]byte)("Hello World"), "")
	err = sSender.Send(msg)
	if err != nil {
		t.Logf("Success: sender address is unreachable. %s", err)
	} else {
		t.Errorf("Failed: sender address is reachable. %s", err)
	}

	sReceiver := GetReceiver("localhost1", "queue_test")
	_, err = sReceiver.Consume()

	if err != nil {
		t.Logf("Success: receiver address is unreachable. %s", err)
	} else {
		t.Errorf("Failed: receiver address is reachable. %s", err)
	}
}

func GetReceiver(host string, notification string) messaging.Receiver {
	return (receiver.NewSimpleReceiver(host, notification))
}

func GetSender(host string) messaging.Sender {
	return (sender.NewSimpleSender(host))
}
