package messaging

type (
	Sender interface {
		Send(message *Message) error
	}
)
