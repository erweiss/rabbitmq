package messaging

type (
	RpcClient interface {
		SendCommand(message *Message) (*Message, error)
	}

	RpcServer interface {
		StartListening(callback func(*Message) (*Message, error)) error
	}

	RpcMessage struct {
		Base *Message
		ID   string
		ReplyTo  string
	}
)

func NewRpcMessage(id string, replyTo string, message *Message) *RpcMessage {
	return &RpcMessage{
		Base: message,
		ID:   id,
		ReplyTo:  replyTo,
	}
}
