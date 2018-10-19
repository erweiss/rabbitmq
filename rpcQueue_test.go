package rabbitmq

import (
	"testing"

	"fmt"

	"github.com/erweiss/rabbitmq/messaging"
	"github.com/erweiss/rabbitmq/rpc"
)

func TestRpcMessage(t *testing.T) {

	go func() {
		server := GetServer("localhost", "rpc_queue")
		server.StartListening(func(msg *messaging.Message) (*messaging.Message, error) {
			response := messaging.NewMessage(msg.Notification, ([]byte)(fmt.Sprintf("%s: Message Received",
				string(msg.Payload))), msg.Tag)
			return response, nil
		})
	}()

	client := GetClient("localhost")
	msg := messaging.NewMessage("rpc_queue", ([]byte)("Hello World (RPC Test)"), "")
	responseMsg, err := client.SendCommand(msg)

	if err != nil {
		t.Error(err)
	}

	t.Logf("Success: Forwarded Message: %s", string(responseMsg.Payload))
}

func TestRpcMessage_InvalidReceivingQueue_ShouldFail(t *testing.T) {

	server := GetServer("localhost1", "rpc_queue_test")
	err := server.StartListening(func(msg *messaging.Message) (*messaging.Message, error) {
		response := messaging.NewMessage(msg.Notification, ([]byte)(fmt.Sprintf("%s: Message Received",
			string(msg.Payload))), msg.Tag)
		return response, nil
	})

	if err != nil {
		t.Logf("Success: client address is unreachable. %s", err)
	} else {
		t.Errorf("Failed: client address is reachable. %s", err)
	}

	client := GetClient("localhost1")
	msg := messaging.NewMessage("rpc_queue_test", ([]byte)("Hello World"), "")
	_, err = client.SendCommand(msg)
	if err != nil {
		t.Logf("Success: server address is unreachable. %s", err)
	} else {
		t.Errorf("Failed: server address is reachable. %s", err)
	}

}

func GetServer(host string, notification string) messaging.RpcServer {
	return (rpc.NewServer(host, notification))
}

func GetClient(host string) messaging.RpcClient {
	return (rpc.NewClient(host))
}
