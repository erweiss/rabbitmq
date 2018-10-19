package rabbitmq

import (
	"testing"
	"time"

	"github.com/erweiss/rabbitmq/messaging"
	"github.com/erweiss/rabbitmq/subscriptions"
)

func TestSubscriptionMessage(t *testing.T) {

	expected := "Hello World (Subscription Fanout Test)"

	sReceiver := GetConsumer("subscription_test", "localhost", "test_exchange", false)
	ch, err := sReceiver.Consume()
	if err != nil {
		t.Error(err)
	}

	go func() {
		sSender := GetPublisher("localhost", "test_exchange", false)
		msg := messaging.NewMessage("subscription_test", ([]byte)(expected), "")
		err = sSender.Send(msg)
		if err != nil {
			t.Error(err)
		}
	}()

	time.Sleep(time.Millisecond * 50)
	t.Log("Waiting on subscription_test response...")
	forwardedMsg := <-ch

	if string(forwardedMsg.Payload) == expected {
		t.Logf("Success: Forwarded Message: %s", string(forwardedMsg.Payload))
	} else {
		t.Errorf("Failed: Forwarded message != expected. Received: %s; Expected: %s",
			string(forwardedMsg.Payload), expected)
	}
}

func TestDirectSubscriptionMessage(t *testing.T) {

	expected := "Hello World (Direct Subscription Test)"

	sReceiver := GetConsumer("subscription_test_direct", "localhost", "test_exchange_direct", true)
	ch, err := sReceiver.Consume()
	if err != nil {
		t.Error(err)
	}

	go func() {
		sSender := GetPublisher("localhost", "test_exchange_direct", true)
		msg := messaging.NewMessage("subscription_test_direct", ([]byte)(expected), "")
		err = sSender.Send(msg)
		if err != nil {
			t.Error(err)
		}
	}()

	time.Sleep(time.Millisecond * 50)
	t.Log("Waiting on subscription_test response...")
	forwardedMsg := <-ch

	if string(forwardedMsg.Payload) == expected {
		t.Logf("Success: Forwarded Message: %s", string(forwardedMsg.Payload))
	} else {
		t.Errorf("Failed: Forwarded message != expected. Received: %s; Expected: %s",
			string(forwardedMsg.Payload), expected)
	}
}

func GetConsumer(notification string, host string, exchange string, direct bool) messaging.Receiver {
	return (subscriptions.NewRabbitMQConsumer(notification, host, exchange, direct))
}

func GetPublisher(host string, exchange string, direct bool) messaging.Sender {
	return (subscriptions.NewRabbitMQPublisher(host, exchange, direct))
}
