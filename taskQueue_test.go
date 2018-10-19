package rabbitmq

import (
	"testing"

	"github.com/erweiss/rabbitmq/messaging"
	"github.com/erweiss/rabbitmq/task"
	"github.com/erweiss/rabbitmq/worker"
)

func TestTaskMessage(t *testing.T) {

	sSender := GetTask("localhost")
	msg := messaging.NewMessage("task_test", ([]byte)("Hello World (Task Test)"), "")
	sSender.Send(msg)

	sSender2 := GetTask("localhost")
	msg2 := messaging.NewMessage("task_test", ([]byte)("Hello World (Task Test2)"), "")
	sSender2.Send(msg2)

	sReceiver := GetWorker("localhost", "task_test")
	ch, err := sReceiver.Consume()
	if err != nil {
		t.Error(err)
	}

	sReceiver2 := GetWorker("localhost", "task_test")
	ch2, err := sReceiver2.Consume()
	if err != nil {
		t.Error(err)
	}

	forwardedMsg := <-ch
	t.Logf("Success: Forwarded Message 1: %s", string(forwardedMsg.Payload))

	forwardedMsg2 := <-ch2
	t.Logf("Success: Forwarded Message 2: %s", string(forwardedMsg2.Payload))
}

func GetWorker(host string, notification string) messaging.Receiver {
	return (worker.NewWorker(host, notification))
}

func GetTask(host string) messaging.Sender {
	return (task.NewTask(host))
}
